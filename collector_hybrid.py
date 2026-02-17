#!/usr/bin/env python3
"""
Hybrid Polymarket Data Collector

Uses WebSocket for real-time updates (primary)
Falls back to REST polling if WebSocket disconnects
Best of both worlds: speed + reliability
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


class HybridCollector:
    """Hybrid WebSocket + REST collector."""
    
    WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
    REST_API = "https://clob.polymarket.com"
    GAMMA_API = "https://gamma-api.polymarket.com"
    
    def __init__(self, db_path: str = None):
        if db_path is None:
            db_path = self._get_db_path()
        
        self.db_path = db_path
        self.current_period = self._get_current_period()
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self._init_tables()
        
        # HTTP session for REST
        self.session = requests.Session()
        
        # Buffers
        self.price_buffer: deque = deque(maxlen=10000)
        self.buffer_lock = threading.Lock()
        
        # State
        self.running = True
        self.ws_connected = False
        self.last_ws_message = 0
        self.updates_count = 0
        self.duplicates_filtered = 0
        self.last_prices: Dict[str, dict] = {}
        
        # Market info
        self.current_market = None
        self.market_refresh_time = 0
    
    def _get_current_period(self) -> str:
        hour = datetime.now().hour
        return "AM" if hour < 12 else "PM"
    
    def _get_db_path(self) -> str:
        now = datetime.now()
        period = "AM" if now.hour < 12 else "PM"
        return f"data/raw/btc_hf_{now:%Y-%m-%d}_{period}.db"
    
    def _init_tables(self):
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
            
            CREATE INDEX IF NOT EXISTS idx_price_time ON price_updates(timestamp_ms);
            CREATE INDEX IF NOT EXISTS idx_price_market ON price_updates(market_ts, timestamp_ms);
        ''')
        self.conn.commit()
    
    def get_current_market(self) -> Optional[dict]:
        """Fetch the ACTIVE BTC 5-min market - rotates to next window when current closes."""
        # Check cache first (short TTL for active rotation)
        if self.current_market and time.time() - self.market_refresh_time < 10:
            return self.current_market
        
        try:
            now = int(time.time())
            
            # Try current and next few windows to find active market
            for window_offset in range(0, 3):
                window = ((now // 300) * 300) + (window_offset * 300)
                slug = f"btc-updown-5m-{window}"
                
                resp = self.session.get(
                    f"{self.GAMMA_API}/events",
                    params={"slug": slug},
                    timeout=10
                )
                resp.raise_for_status()
                data = resp.json()
                
                if not data or not data[0].get('markets'):
                    continue
                
                event = data[0]
                market = event['markets'][0]
                
                # Check if market is active (not closed, accepting orders)
                is_closed = market.get('closed', False)
                accepting = market.get('acceptingOrders', False)
                end_date = market.get('endDate', '')
                
                # Skip if market is closed or expired
                if is_closed:
                    continue
                
                # Check if expired by end_date
                if end_date:
                    from datetime import datetime
                    try:
                        expiry = datetime.fromisoformat(end_date.replace('Z', '+00:00'))
                        if expiry.timestamp() < now:
                            continue  # Market expired
                    except:
                        pass
                
                # Found active market
                token_ids = json.loads(market.get('clobTokenIds', '[]'))
                
                self.current_market = {
                    'timestamp': window,
                    'asset': 'BTC',
                    'slug': slug,
                    'up_token_id': token_ids[0] if len(token_ids) > 0 else None,
                    'down_token_id': token_ids[1] if len(token_ids) > 1 else None
                }
                self.market_refresh_time = time.time()
                
                # Log market rotation
                if window != getattr(self, '_last_logged_window', None):
                    logger.info(f"📊 Active market: {slug} (accepting={accepting}, closed={is_closed})")
                    self._last_logged_window = window
                
                self.conn.execute('''
                    INSERT OR IGNORE INTO markets 
                    (timestamp, asset, slug, created_at, up_token_id, down_token_id)
                    VALUES (?, ?, ?, ?, ?, ?)
                ''', (window, 'BTC', slug, int(time.time()),
                      token_ids[0] if len(token_ids) > 0 else None,
                      token_ids[1] if len(token_ids) > 1 else None))
                self.conn.commit()
                
                return self.current_market
            
            # No active market found
            logger.warning("No active BTC 5-min market found")
            return None
            
        except Exception as e:
            logger.warning(f"Error fetching market: {e}")
            return self.current_market
    
    def _scale(self, price: float) -> int:
        return int(price * 1_000_000)
    
    def _is_duplicate(self, update: PriceUpdate) -> bool:
        """Check if update is duplicate - allow updates every 100ms even if price same."""
        key = f"{update.market_ts}_{update.side}"
        last = self.last_prices.get(key)
        
        if last:
            # Always allow update if price changed
            if last['bid'] != update.bid or last['ask'] != update.ask:
                return False
            # Allow update if 100ms passed since last (for high-frequency capture)
            if update.timestamp_ms - last['timestamp_ms'] > 100:
                return False
            return True
        
        self.last_prices[key] = {
            'bid': update.bid, 
            'ask': update.ask,
            'timestamp_ms': update.timestamp_ms
        }
        return False
    
    def buffer_update(self, update: PriceUpdate):
        if self._is_duplicate(update):
            self.duplicates_filtered += 1
            return
        
        with self.buffer_lock:
            self.price_buffer.append(update)
        
        self.updates_count += 1
    
    def flush_buffer(self):
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
    
    def fetch_rest_snapshot(self, token_id: str, side: str) -> Optional[PriceUpdate]:
        """Fetch via REST as fallback."""
        try:
            resp = self.session.get(
                f"{self.REST_API}/book",
                params={"token_id": token_id},
                timeout=5
            )
            resp.raise_for_status()
            book = resp.json()
            
            bids = book.get('bids', [])
            asks = book.get('asks', [])
            
            if not bids or not asks:
                return None
            
            best_bid = float(bids[0]['price'])
            best_ask = float(asks[0]['price'])
            mid = (best_bid + best_ask) / 2
            spread_bps = int((best_ask - best_bid) / mid * 10000)
            
            bid_depth = sum(float(b['size']) * float(b['price']) for b in bids[:5])
            ask_depth = sum(float(a['size']) * float(a['price']) for a in asks[:5])
            
            market_ts = (int(time.time()) // 300) * 300
            
            return PriceUpdate(
                timestamp_ms=int(time.time() * 1000),
                market_ts=market_ts,
                asset='BTC',
                side=side,
                bid=best_bid,
                ask=best_ask,
                mid=mid,
                spread_bps=spread_bps,
                bid_depth=bid_depth,
                ask_depth=ask_depth,
                source='rest_fallback'
            )
        except Exception as e:
            logger.warning(f"REST fetch error: {e}")
            return None
    
    async def websocket_listener(self):
        """WebSocket listener with auto-reconnect."""
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
                    self.last_ws_message = time.time()
                    reconnect_delay = 1
                    logger.info("✅ WebSocket connected!")
                    
                    market = self.get_current_market()
                    if not market:
                        await asyncio.sleep(5)
                        continue
                    
                    # Subscribe to market channel (not orderbook)
                    for token_id, side in [
                        (market.get('up_token_id'), 'up'),
                        (market.get('down_token_id'), 'down')
                    ]:
                        if token_id:
                            await ws.send(json.dumps({
                                "type": "subscribe",
                                "channel": "market",
                                "market": token_id
                            }))
                            logger.info(f"Subscribed to {side}: {token_id[:20]}...")
                    
                    async for message in ws:
                        if not self.running:
                            break
                        
                        self.last_ws_message = time.time()
                        
                        try:
                            data = json.loads(message)
                            await self._process_ws_message(data)
                        except json.JSONDecodeError:
                            continue
                        except Exception as e:
                            logger.warning(f"Error processing: {e}")
                            
            except websockets.exceptions.ConnectionClosed:
                logger.warning("WebSocket closed, will reconnect...")
            except Exception as e:
                logger.error(f"WebSocket error: {e}")
            
            self.ws_connected = False
            await asyncio.sleep(reconnect_delay)
            reconnect_delay = min(reconnect_delay * 2, 60)
    
    async def _process_ws_message(self, data: dict):
        """Process WebSocket message - handles multiple event types."""
        event_type = data.get('event_type', '')
        
        if event_type == 'book':
            # Full orderbook update
            await self._process_book_message(data)
        elif event_type == 'price_change':
            # Price change update
            await self._process_price_change(data)
        elif event_type == 'last_trade_price':
            # Trade executed
            logger.debug(f"Trade: {data.get('side')} {data.get('size')} @ {data.get('price')}")
        elif event_type == 'best_bid_ask':
            # Best bid/ask update
            await self._process_best_bid_ask(data)
        else:
            # Unknown message type
            logger.debug(f"Unknown event type: {event_type}")
    
    async def _process_book_message(self, data: dict):
        """Process book message (full orderbook)."""
        asset_id = data.get('asset_id', '')
        timestamp = int(data.get('timestamp', 0))
        
        market = self.get_current_market()
        if asset_id == market.get('up_token_id'):
            side = 'up'
        elif asset_id == market.get('down_token_id'):
            side = 'down'
        else:
            return
        
        bids = data.get('bids', [])
        asks = data.get('asks', [])
        
        if not bids or not asks:
            return
        
        best_bid = float(bids[0]['price'])
        best_ask = float(asks[0]['price'])
        mid = (best_bid + best_ask) / 2
        spread_bps = int((best_ask - best_bid) / mid * 10000)
        
        bid_depth = sum(float(b['size']) * float(b['price']) for b in bids[:5])
        ask_depth = sum(float(a['size']) * float(a['price']) for a in asks[:5])
        
        market_ts = (timestamp // 1000 // 300) * 300
        
        self.buffer_update(PriceUpdate(
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
            source='websocket_book'
        ))
    
    async def _process_price_change(self, data: dict):
        """Process price change message."""
        for change in data.get('price_changes', []):
            asset_id = change.get('asset_id', '')
            
            market = self.get_current_market()
            if asset_id == market.get('up_token_id'):
                side = 'up'
            elif asset_id == market.get('down_token_id'):
                side = 'down'
            else:
                continue
            
            best_bid = float(change.get('best_bid', 0))
            best_ask = float(change.get('best_ask', 0))
            
            if best_bid == 0 or best_ask == 0:
                continue
            
            mid = (best_bid + best_ask) / 2
            spread_bps = int((best_ask - best_bid) / mid * 10000)
            
            timestamp = int(data.get('timestamp', time.time() * 1000))
            market_ts = (timestamp // 1000 // 300) * 300
            
            self.buffer_update(PriceUpdate(
                timestamp_ms=timestamp,
                market_ts=market_ts,
                asset='BTC',
                side=side,
                bid=best_bid,
                ask=best_ask,
                mid=mid,
                spread_bps=spread_bps,
                bid_depth=0,  # Not provided in price_change
                ask_depth=0,
                source='websocket_price'
            ))
    
    async def _process_best_bid_ask(self, data: dict):
        """Process best bid/ask message."""
        asset_id = data.get('asset_id', '')
        
        market = self.get_current_market()
        if asset_id == market.get('up_token_id'):
            side = 'up'
        elif asset_id == market.get('down_token_id'):
            side = 'down'
        else:
            return
        
        best_bid = float(data.get('best_bid', 0))
        best_ask = float(data.get('best_ask', 0))
        
        if best_bid == 0 or best_ask == 0:
            return
        
        mid = (best_bid + best_ask) / 2
        spread_bps = int((best_ask - best_bid) / mid * 10000)
        
        timestamp = int(data.get('timestamp', time.time() * 1000))
        market_ts = (timestamp // 1000 // 300) * 300
        
        self.buffer_update(PriceUpdate(
            timestamp_ms=timestamp,
            market_ts=market_ts,
            asset='BTC',
            side=side,
            bid=best_bid,
            ask=best_ask,
            mid=mid,
            spread_bps=spread_bps,
            bid_depth=0,
            ask_depth=0,
            source='websocket_bba'
        ))
    
    def rest_fallback_poller(self, interval_sec: float = 5):
        """REST poller that activates when WebSocket is stale."""
        logger.info(f"REST fallback started ({interval_sec}s interval)")
        
        while self.running:
            time.sleep(interval_sec)
            
            # Check if WebSocket is stale (>5s without message)
            ws_stale = time.time() - self.last_ws_message > 5
            
            if self.ws_connected and not ws_stale:
                continue  # WebSocket is healthy
            
            if ws_stale and self.ws_connected:
                logger.warning("WebSocket stale, using REST fallback...")
            
            # Fetch via REST
            market = self.get_current_market()
            if market:
                for token_id, side in [
                    (market.get('up_token_id'), 'up'),
                    (market.get('down_token_id'), 'down')
                ]:
                    if token_id:
                        update = self.fetch_rest_snapshot(token_id, side)
                        if update:
                            self.buffer_update(update)
    
    def db_flusher(self, interval_sec: float = 5):
        """Periodically flush to database."""
        logger.info(f"DB flusher started ({interval_sec}s)")
        
        last_count = 0
        while self.running:
            time.sleep(interval_sec)
            self.flush_buffer()
            
            # Log stats every 1000 updates or 30 seconds
            if self.updates_count - last_count >= 100 or time.time() % 30 < interval_sec:
                logger.info(
                    f"📊 Updates: {self.updates_count} | "
                    f"Dupes: {self.duplicates_filtered} | "
                    f"Buffer: {len(self.price_buffer)} | "
                    f"WS: {'✅' if self.ws_connected else '❌'} | "
                    f"Source: {'WS' if time.time() - self.last_ws_message < 5 else 'REST'}"
                )
                last_count = self.updates_count
    
    def run(self):
        """Run hybrid collector."""
        logger.info("="*60)
        logger.info("Hybrid Polymarket Collector Starting")
        logger.info("WebSocket primary + REST fallback")
        logger.info(f"Database: {self.db_path}")
        logger.info("="*60)
        
        # Start threads
        threads = [
            threading.Thread(target=self.db_flusher, args=(1,), daemon=True),  # Flush every 1s
            threading.Thread(target=self.rest_fallback_poller, args=(0.5,), daemon=True),  # REST every 500ms
        ]
        
        for t in threads:
            t.start()
        
        # Run WebSocket in main thread
        try:
            asyncio.run(self.websocket_listener())
        except KeyboardInterrupt:
            logger.info("\nStopping...")
            self.running = False
        
        self.flush_buffer()
        self.conn.close()
        logger.info("Collector stopped.")


if __name__ == "__main__":
    collector = HybridCollector()
    collector.run()
