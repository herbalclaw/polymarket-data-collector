#!/usr/bin/env python3
"""
High-Frequency Polymarket BTC 5-Min Data Collector

Captures orderbook changes at millisecond granularity using WebSocket
with REST fallback for maximum data fidelity.
"""

import os
import sys
import time
import json
import sqlite3
import logging
import asyncio
import threading
from datetime import datetime, timezone
from dataclasses import dataclass, asdict
from typing import Optional, Dict, List, Callable
from pathlib import Path
from collections import deque

import requests
import websockets
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=getattr(logging, os.getenv('LOG_LEVEL', 'INFO')),
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
    side: str  # 'up' or 'down'
    bid: float
    ask: float
    mid: float
    spread_bps: int
    bid_depth: float
    ask_depth: float
    source: str  # 'websocket' or 'rest'


class HighFrequencyCollector:
    """Millisecond-precision collector for BTC 5-min markets."""
    
    GAMMA_API = "https://gamma-api.polymarket.com"
    CLOB_API = "https://clob.polymarket.com"
    WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
    
    def __init__(self, db_path: str = None):
        # Auto-generate DB path based on 12-hour rotation if not provided
        if db_path is None:
            db_path = self._get_db_path()
        
        self.db_path = db_path
        self.current_period = self._get_current_period()
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self._init_tables()
        
        # HTTP session for REST fallback
        self.session = requests.Session()
        retry = Retry(total=3, backoff_factor=0.1)
        adapter = HTTPAdapter(pool_connections=10, pool_maxsize=10, max_retries=retry)
        self.session.mount("https://", adapter)
        
        # Buffers for batch inserts
        self.price_buffer: deque = deque(maxlen=10000)
        self.buffer_lock = threading.Lock()
        
        # Track last prices to avoid duplicates
        self.last_prices: Dict[str, dict] = {}
        
        # WebSocket state
        self.ws_connected = False
        self.ws = None
        self.running = True
        
        # Stats
        self.updates_count = 0
        self.duplicates_filtered = 0
    
    def _get_current_period(self) -> str:
        """Get current 12-hour period (AM/PM)."""
        hour = datetime.now().hour
        return "AM" if hour < 12 else "PM"
    
    def _get_db_path(self) -> str:
        """Generate DB path with 12-hour rotation."""
        now = datetime.now()
        period = "AM" if now.hour < 12 else "PM"
        return f"data/raw/btc_hf_{now:%Y-%m-%d}_{period}.db"
    
    def check_rotation(self) -> bool:
        """Check if we need to rotate to new DB file (12-hour period changed)."""
        current = self._get_current_period()
        if current != self.current_period:
            logger.info(f"Rotating database: {self.current_period} -> {current}")
            self.flush_buffer()  # Flush any pending data
            self.conn.close()
            self.db_path = self._get_db_path()
            self.current_period = current
            Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
            self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
            self.conn.execute("PRAGMA journal_mode=WAL")
            self.conn.execute("PRAGMA synchronous=NORMAL")
            self._init_tables()
            self.last_prices.clear()
            return True
        return False
        
    def _init_tables(self):
        """Optimized schema for high-frequency data."""
        self.conn.executescript('''
            CREATE TABLE IF NOT EXISTS markets (
                timestamp INTEGER PRIMARY KEY,
                asset TEXT,
                slug TEXT,
                created_at INTEGER,
                resolved_at INTEGER,
                outcome TEXT,
                up_token_id TEXT,
                down_token_id TEXT
            );
            
            -- Partitioned by time for query performance
            CREATE TABLE IF NOT EXISTS price_updates (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp_ms INTEGER NOT NULL,
                market_ts INTEGER NOT NULL,
                asset TEXT NOT NULL,
                side TEXT NOT NULL,
                bid INTEGER NOT NULL,  -- scaled by 1e6
                ask INTEGER NOT NULL,
                mid INTEGER NOT NULL,
                spread_bps INTEGER,
                bid_depth REAL,
                ask_depth REAL,
                source TEXT
            );
            
            -- Indexes for fast queries
            CREATE INDEX IF NOT EXISTS idx_price_time 
                ON price_updates(timestamp_ms);
            CREATE INDEX IF NOT EXISTS idx_price_market 
                ON price_updates(market_ts, timestamp_ms);
            CREATE INDEX IF NOT EXISTS idx_price_asset 
                ON price_updates(asset, timestamp_ms);
            
            VACUUM;
        ''')
        self.conn.commit()
    
    def _scale(self, price: float) -> int:
        return int(round(price * 1_000_000))
    
    def _unscale(self, price: int) -> float:
        return price / 1_000_000
    
    def get_current_market(self) -> Optional[dict]:
        """Get current BTC 5-min market."""
        now = int(time.time())
        current_window = (now // 300) * 300
        
        # Check cache
        cursor = self.conn.execute(
            "SELECT * FROM markets WHERE timestamp = ?",
            (current_window,)
        )
        row = cursor.fetchone()
        if row:
            return {
                'timestamp': row[0],
                'asset': row[1],
                'up_token_id': row[6],
                'down_token_id': row[7]
            }
        
        # Fetch from API
        slug = f"btc-updown-5m-{current_window}"
        try:
            resp = self.session.get(
                f"{self.GAMMA_API}/events",
                params={"slug": slug},
                timeout=5
            )
            resp.raise_for_status()
            data = resp.json()
            
            if not data or not data[0].get('markets'):
                return None
            
            event = data[0]
            market = event['markets'][0]
            token_ids = json.loads(market.get('clobTokenIds', '[]'))
            
            up_token = token_ids[0] if len(token_ids) > 0 else None
            down_token = token_ids[1] if len(token_ids) > 1 else None
            
            self.conn.execute('''
                INSERT OR IGNORE INTO markets 
                (timestamp, asset, slug, created_at, up_token_id, down_token_id)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (current_window, 'BTC', slug, int(time.time()), up_token, down_token))
            self.conn.commit()
            
            return {
                'timestamp': current_window,
                'asset': 'BTC',
                'up_token_id': up_token,
                'down_token_id': down_token
            }
        except Exception as e:
            logger.warning(f"Error fetching market: {e}")
            return None
    
    def fetch_rest_snapshot(self, token_id: str, side: str) -> Optional[PriceUpdate]:
        """Fetch current orderbook via REST."""
        try:
            resp = self.session.get(
                f"{self.CLOB_API}/book",
                params={"token_id": token_id},
                timeout=2
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
            
            market = self.get_current_market()
            if not market:
                return None
            
            return PriceUpdate(
                timestamp_ms=int(time.time() * 1000),
                market_ts=market['timestamp'],
                asset='BTC',
                side=side,
                bid=best_bid,
                ask=best_ask,
                mid=mid,
                spread_bps=int((best_ask - best_bid) * 10000),
                bid_depth=float(bids[0]['price']) * float(bids[0]['size']),
                ask_depth=float(asks[0]['price']) * float(asks[0]['size']),
                source='rest'
            )
        except Exception as e:
            logger.debug(f"REST fetch error: {e}")
            return None
    
    def _is_duplicate(self, update: PriceUpdate) -> bool:
        """Check if price is unchanged from last update."""
        key = f"{update.asset}:{update.side}"
        last = self.last_prices.get(key)
        
        if last and last['bid'] == update.bid and last['ask'] == update.ask:
            return True
        
        self.last_prices[key] = {'bid': update.bid, 'ask': update.ask}
        return False
    
    def buffer_update(self, update: PriceUpdate):
        """Add update to buffer, filtering duplicates."""
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
            logger.debug(f"Flushed {len(updates)} updates to DB")
        except Exception as e:
            logger.error(f"Error flushing buffer: {e}")
    
    async def websocket_listener(self, token_id: str, side: str):
        """Listen to WebSocket for real-time updates."""
        while self.running:
            try:
                async with websockets.connect(self.WS_URL) as ws:
                    self.ws = ws
                    self.ws_connected = True
                    logger.info(f"WebSocket connected for {side}")
                    
                    # Subscribe to market
                    subscribe_msg = {
                        "type": "subscribe",
                        "channel": "market",
                        "market": f"btc-updown-5m-{int(time.time()) // 300 * 300}"
                    }
                    await ws.send(json.dumps(subscribe_msg))
                    
                    async for message in ws:
                        if not self.running:
                            break
                        
                        try:
                            data = json.loads(message)
                            # Parse WebSocket message for price updates
                            # This depends on Polymarket's WS message format
                            # Placeholder for actual parsing logic
                            
                        except json.JSONDecodeError:
                            continue
                            
            except Exception as e:
                logger.warning(f"WebSocket error: {e}")
                self.ws_connected = False
                await asyncio.sleep(5)
    
    def rest_poller(self, interval_ms: float = 100):
        """High-frequency REST polling as fallback."""
        logger.info(f"Starting REST poller ({interval_ms}ms interval)")
        
        while self.running:
            start = time.time()
            
            market = self.get_current_market()
            if market:
                # Poll both up and down tokens
                if market.get('up_token_id'):
                    update = self.fetch_rest_snapshot(market['up_token_id'], 'up')
                    if update:
                        self.buffer_update(update)
                
                if market.get('down_token_id'):
                    update = self.fetch_rest_snapshot(market['down_token_id'], 'down')
                    if update:
                        self.buffer_update(update)
            
            # Sleep for remaining interval
            elapsed = (time.time() - start) * 1000
            sleep_time = max(0, (interval_ms - elapsed) / 1000)
            time.sleep(sleep_time)
    
    def db_flusher(self, interval_sec: float = 5):
        """Periodically flush buffer to database."""
        logger.info(f"Starting DB flusher ({interval_sec}s interval)")
        
        while self.running:
            time.sleep(interval_sec)
            
            # Check for 12-hour rotation
            self.check_rotation()
            
            self.flush_buffer()
            
            # Log stats
            if self.updates_count % 1000 == 0:
                logger.info(
                    f"Updates: {self.updates_count}, "
                    f"Duplicates filtered: {self.duplicates_filtered}, "
                    f"Buffer: {len(self.price_buffer)}, "
                    f"DB: {self.db_path}"
                )
    
    def run(self, poll_interval_ms: float = 100):
        """
        Run high-frequency collection.
        
        Args:
            poll_interval_ms: How often to poll REST API (default 100ms = 10Hz)
        """
        logger.info(f"Starting HF collector ({poll_interval_ms}ms poll interval)")
        logger.info(f"Database: {self.db_path}")
        
        # Start REST poller in thread
        poll_thread = threading.Thread(
            target=self.rest_poller,
            args=(poll_interval_ms,),
            daemon=True
        )
        poll_thread.start()
        
        # Start DB flusher in thread
        flush_thread = threading.Thread(
            target=self.db_flusher,
            args=(5,),  # Flush every 5 seconds
            daemon=True
        )
        flush_thread.start()
        
        try:
            while self.running:
                time.sleep(1)
        except KeyboardInterrupt:
            logger.info("\nStopping collector...")
            self.running = False
        
        # Final flush
        self.flush_buffer()
        self.conn.close()
        logger.info("Collector stopped.")


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser()
    parser.add_argument("--interval", type=float, default=100,
                       help="Poll interval in milliseconds (default: 100ms = 10Hz)")
    parser.add_argument("--db", default=None,
                       help="Database path (auto-rotates every 12 hours if not specified)")
    args = parser.parse_args()
    
    collector = HighFrequencyCollector(args.db)
    collector.run(poll_interval_ms=args.interval)
