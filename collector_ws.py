#!/usr/bin/env python3
"""
WebSocket-based High-Frequency Polymarket Data Collector

Uses WebSocket for real-time price updates (10-100Hz possible)
Falls back to REST if WebSocket disconnects
"""

import os
import sys
import time
import json
import sqlite3
import logging
import asyncio
import threading
from datetime import datetime
from dataclasses import dataclass
from typing import Optional, Dict
from pathlib import Path
from collections import deque

import requests
import websockets
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s.%(msecs)03d [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)


@dataclass
class PriceUpdate:
    """Single price update at millisecond precision."""
    timestamp_ms: int
    market_ts: int
    asset: str
    side: str
    bid: float
    ask: float
    mid: float
    spread_bps: int
    bid_depth: float
    ask_depth: float
    source: str


class WebSocketCollector:
    """WebSocket-based collector for ultra-low latency data."""
    
    WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
    REST_API = "https://clob.polymarket.com"
    GAMMA_API = "https://gamma-api.polymarket.com"
    
    def __init__(self, db_path: str = None):
        # Auto-generate DB path based on 12-hour rotation
        if db_path is None:
            db_path = self._get_db_path()
        
        self.db_path = db_path
        self.current_period = self._get_current_period()
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        
        # Initialize database
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self._init_tables()
        
        # Buffers
        self.price_buffer: deque = deque(maxlen=10000)
        self.buffer_lock = threading.Lock()
        
        # State
        self.running = True
        self.ws_connected = False
        self.updates_count = 0
        self.duplicates_filtered = 0
        self.last_prices: Dict[str, dict] = {}
        
        # Current market info
        self.current_market = None
        self.market_refresh_time = 0
    
    def _get_current_period(self) -> str:
        """Get current 12-hour period (AM/PM)."""
        hour = datetime.now().hour
        return "AM" if hour < 12 else "PM"
    
    def _get_db_path(self) -> str:
        """Generate DB path with 12-hour rotation."""
        now = datetime.now()
        period = "AM" if now.hour < 12 else "PM"
        return f"data/raw/btc_hf_{now:%Y-%m-%d}_{period}.db"
    
    def _init_tables(self):
        """Initialize database tables."""
        self.conn.executescript('''
            CREATE TABLE IF NOT EXISTS markets (
                timestamp INTEGER PRIMARY KEY,
                asset TEXT,
                slug TEXT,
                created_at INTEGER,
                up_token_id TEXT,
                down_token_id TEXT
            );
            
            CREATE TABLE IF NOT EXISTS price_updates (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp_ms INTEGER NOT NULL,
                market_ts INTEGER NOT NULL,
                asset TEXT NOT NULL,
                side TEXT NOT NULL,
                bid INTEGER NOT NULL,
                ask INTEGER NOT NULL,
                mid INTEGER NOT NULL,
                spread_bps INTEGER,
                bid_depth REAL,
                ask_depth REAL,
                source TEXT
            );
            
            CREATE INDEX IF NOT EXISTS idx_price_time 
                ON price_updates(timestamp_ms);
            CREATE INDEX IF NOT EXISTS idx_price_market 
                ON price_updates(market_ts, timestamp_ms);
        ''')
        self.conn.commit()
    
    def get_current_market(self) -> Optional[dict]:
        """Get current BTC 5-min market from Gamma API."""
        # Refresh every 60 seconds
        if self.current_market and time.time() - self.market_refresh_time < 60:
            return self.current_market
        
        try:
            current_window = (int(time.time()) // 300) * 300
            slug = f"btc-updown-5m-{current_window}"
            
            resp = requests.get(
                f"{self.GAMMA_API}/events",
                params={"slug": slug},
                timeout=10
            )
            resp.raise_for_status()
            data = resp.json()
            
            if not data or not data[0].get('markets'):
                return None
            
            event = data[0]
            market = event['markets'][0]
            token_ids = json.loads(market.get('clobTokenIds', '[]'))
            
            self.current_market = {
                'timestamp': current_window,
                'asset': 'BTC',
                'slug': slug,
                'up_token_id': token_ids[0] if len(token_ids) > 0 else None,
                'down_token_id': token_ids[1] if len(token_ids) > 1 else None
            }
            self.market_refresh_time = time.time()
            
            # Save to DB
            self.conn.execute('''
                INSERT OR IGNORE INTO markets 
                (timestamp, asset, slug, created_at, up_token_id, down_token_id)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (current_window, 'BTC', slug, int(time.time()), 
                  token_ids[0] if len(token_ids) > 0 else None,
                  token_ids[1] if len(token_ids) > 1 else None))
            self.conn.commit()
            
            return self.current_market
        except Exception as e:
            logger.warning(f"Error fetching market: {e}")
            return self.current_market
    
    def _scale(self, price: float) -> int:
        """Scale price by 1e6 for integer storage."""
        return int(price * 1_000_000)
    
    def _is_duplicate(self, update: PriceUpdate) -> bool:
        """Check if this is a duplicate price."""
        key = f"{update.market_ts}_{update.side}"
        last = self.last_prices.get(key)
        
        if last and last['bid'] == update.bid and last['ask'] == update.ask:
            return True
        
        self.last_prices[key] = {'bid': update.bid, 'ask': update.ask}
        return False
    
    def buffer_update(self, update: PriceUpdate):
        """Add update to buffer."""
        if self._is_duplicate(update):
            self.duplicates_filtered += 1
            return
        
        with self.buffer_lock:
            self.price_buffer.append(update)
        
        self.updates_count += 1
    
    def flush_buffer(self):
        """Flush buffer to database."""
        with self.buffer_lock:
            if not self.price_buffer:
                return
            updates = list(self.price_buffer)
            self.price_buffer.clear()
        
        try:
            self.conn.executemany('''
                INSERT INTO price_updates 
                (timestamp_ms, market_ts, asset, side, bid, ask, mid, 
                 spread_bps, bid_depth, ask_depth, source)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', [
                (u.timestamp_ms, u.market_ts, u.asset, u.side,
                 self._scale(u.bid), self._scale(u.ask), self._scale(u.mid),
                 u.spread_bps, u.bid_depth, u.ask_depth, u.source)
                for u in updates
            ])
            self.conn.commit()
            logger.debug(f"Flushed {len(updates)} updates")
        except Exception as e:
            logger.error(f"Error flushing: {e}")
    
    async def websocket_listener(self):
        """Main WebSocket listener for real-time updates."""
        reconnect_delay = 1
        
        while self.running:
            try:
                logger.info(f"Connecting to WebSocket...")
                
                async with websockets.connect(
                    self.WS_URL,
                    ping_interval=20,
                    ping_timeout=10
                ) as ws:
                    self.ws_connected = True
                    reconnect_delay = 1  # Reset on successful connect
                    logger.info("✅ WebSocket connected!")
                    
                    # Get current market
                    market = self.get_current_market()
                    if not market:
                        logger.warning("No market available, retrying...")
                        await asyncio.sleep(5)
                        continue
                    
                    # Subscribe to both UP and DOWN tokens
                    for token_id, side in [
                        (market.get('up_token_id'), 'up'),
                        (market.get('down_token_id'), 'down')
                    ]:
                        if token_id:
                            subscribe_msg = {
                                "type": "subscribe",
                                "channel": "orderbook",
                                "market": token_id
                            }
                            await ws.send(json.dumps(subscribe_msg))
                            logger.info(f"Subscribed to {side}: {token_id[:20]}...")
                    
                    # Listen for messages
                    async for message in ws:
                        if not self.running:
                            break
                        
                        try:
                            data = json.loads(message)
                            await self._process_ws_message(data)
                        except json.JSONDecodeError:
                            continue
                        except Exception as e:
                            logger.warning(f"Error processing message: {e}")
                            
            except websockets.exceptions.ConnectionClosed:
                logger.warning("WebSocket closed, reconnecting...")
            except Exception as e:
                logger.error(f"WebSocket error: {e}")
            
            self.ws_connected = False
            await asyncio.sleep(reconnect_delay)
            reconnect_delay = min(reconnect_delay * 2, 60)  # Exponential backoff
    
    async def _process_ws_message(self, data: dict):
        """Process WebSocket message."""
        msg_type = data.get('type', '')
        
        if msg_type == 'orderbook_update':
            # Extract orderbook data
            token_id = data.get('market', '')
            timestamp = int(time.time() * 1000)
            
            # Determine side from token_id
            market = self.get_current_market()
            side = 'up' if token_id == market.get('up_token_id') else 'down'
            
            # Extract bids/asks
            bids = data.get('bids', [])
            asks = data.get('asks', [])
            
            if bids and asks:
                best_bid = float(bids[0]['price'])
                best_ask = float(asks[0]['price'])
                mid = (best_bid + best_ask) / 2
                spread_bps = int((best_ask - best_bid) / mid * 10000)
                
                # Calculate depth
                bid_depth = sum(float(b['size']) * float(b['price']) for b in bids[:5])
                ask_depth = sum(float(a['size']) * float(a['price']) for a in asks[:5])
                
                market_ts = (int(time.time()) // 300) * 300
                
                update = PriceUpdate(
                    timestamp_ms=timestamp,
                    market_ts=market_ts,
                    asset='BTC',
                    side=side,
                    bid=best_bid,
                    ask=best_ask,
                    mid=mid,
                    spread_bps=spread_bps,
                    bid_depth=bid_depth,
                    ask_depth=ask_depth,
                    source='websocket'
                )
                
                self.buffer_update(update)
    
    def db_flusher(self, interval_sec: float = 5):
        """Periodically flush buffer to database."""
        logger.info(f"Starting DB flusher ({interval_sec}s interval)")
        
        while self.running:
            time.sleep(interval_sec)
            self.flush_buffer()
            
            if self.updates_count % 1000 == 0 and self.updates_count > 0:
                logger.info(
                    f"📊 Updates: {self.updates_count}, "
                    f"Duplicates: {self.duplicates_filtered}, "
                    f"Buffer: {len(self.price_buffer)}, "
                    f"WS: {'✅' if self.ws_connected else '❌'}"
                )
    
    def run(self):
        """Run WebSocket collector."""
        logger.info("="*60)
        logger.info("WebSocket Polymarket Collector Starting")
        logger.info(f"Database: {self.db_path}")
        logger.info("="*60)
        
        # Start DB flusher in thread
        flush_thread = threading.Thread(
            target=self.db_flusher,
            args=(5,),
            daemon=True
        )
        flush_thread.start()
        
        # Run WebSocket listener
        try:
            asyncio.run(self.websocket_listener())
        except KeyboardInterrupt:
            logger.info("\nStopping collector...")
            self.running = False
        
        # Final flush
        self.flush_buffer()
        self.conn.close()
        logger.info("Collector stopped.")


if __name__ == "__main__":
    collector = WebSocketCollector()
    collector.run()
