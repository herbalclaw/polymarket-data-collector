#!/usr/bin/env python3
"""
Polymarket Multi-Asset Data Collector

Continuously collects orderbook data for BTC, ETH, SOL 5-minute markets.
"""

import os
import sys
import time
import json
import sqlite3
import logging
import argparse
from datetime import datetime, timezone
from dataclasses import dataclass
from typing import Optional, Dict, List
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Setup logging
logging.basicConfig(
    level=getattr(logging, os.getenv('LOG_LEVEL', 'INFO')),
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)


@dataclass
class AssetConfig:
    """Configuration for a tracked asset."""
    symbol: str
    slug_prefix: str
    token_ids: Dict[str, str] = None  # Cache for token IDs
    
    def get_slug(self, timestamp: int) -> str:
        """Generate market slug for this asset."""
        return f"{self.slug_prefix}-updown-5m-{timestamp}"


class PolymarketCollector:
    """Multi-asset data collector for Polymarket."""
    
    GAMMA_API = "https://gamma-api.polymarket.com"
    CLOB_API = "https://clob.polymarket.com"
    
    # Asset configurations (currently only BTC has 5-min markets)
    ASSETS = {
        'BTC': AssetConfig('BTC', 'btc'),
        # 'ETH': AssetConfig('ETH', 'eth'),  # Uncomment when available
        # 'SOL': AssetConfig('SOL', 'sol'),  # Uncomment when available
    }
    
    def __init__(self, db_path: str = "data/raw/polymarket_data.db"):
        self.db_path = db_path
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        
        # Initialize database
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self._init_tables()
        
        # HTTP session
        self.session = requests.Session()
        retry = Retry(total=3, backoff_factor=0.1)
        adapter = HTTPAdapter(pool_connections=10, pool_maxsize=10, max_retries=retry)
        self.session.mount("https://", adapter)
        
        # Track last snapshots to avoid duplicates
        self._last_snapshots: Dict[str, dict] = {}
        
        # Load which assets to track
        assets_env = os.getenv('ASSETS', 'BTC,ETH,SOL')
        self.tracked_assets = [a.strip().upper() for a in assets_env.split(',')]
        logger.info(f"Tracking assets: {', '.join(self.tracked_assets)}")
    
    def _init_tables(self):
        """Create optimized schema."""
        self.conn.executescript('''
            CREATE TABLE IF NOT EXISTS markets (
                timestamp INTEGER NOT NULL,
                asset TEXT NOT NULL,
                slug TEXT,
                created_at INTEGER DEFAULT (strftime('%s', 'now')),
                resolved_at INTEGER,
                outcome TEXT CHECK(outcome IN ('up', 'down', NULL)),
                up_token_id TEXT,
                down_token_id TEXT,
                PRIMARY KEY (timestamp, asset)
            );
            
            CREATE TABLE IF NOT EXISTS orderbook_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                market_ts INTEGER NOT NULL,
                asset TEXT NOT NULL,
                recorded_at INTEGER NOT NULL,
                best_bid INTEGER NOT NULL,
                best_ask INTEGER NOT NULL,
                mid_price INTEGER NOT NULL,
                bid_depth REAL DEFAULT 0,
                ask_depth REAL DEFAULT 0,
                spread_bps INTEGER DEFAULT 0,
                FOREIGN KEY (market_ts, asset) REFERENCES markets(timestamp, asset)
            );
            
            CREATE INDEX IF NOT EXISTS idx_snapshots_market 
                ON orderbook_snapshots(market_ts, asset, recorded_at);
            CREATE INDEX IF NOT EXISTS idx_snapshots_time 
                ON orderbook_snapshots(recorded_at);
            CREATE INDEX IF NOT EXISTS idx_markets_resolved 
                ON markets(resolved_at) WHERE resolved_at IS NOT NULL;
            
            VACUUM;
        ''')
        self.conn.commit()
    
    def _scale_price(self, price: float) -> int:
        """Convert float to scaled integer (6 decimal precision)."""
        return int(round(price * 1_000_000))
    
    def get_market(self, asset: str, timestamp: int) -> Optional[dict]:
        """Get or fetch market info."""
        # Check cache first
        cursor = self.conn.execute(
            "SELECT * FROM markets WHERE asset = ? AND timestamp = ?",
            (asset, timestamp)
        )
        row = cursor.fetchone()
        
        if row:
            return {
                'timestamp': row[0],
                'asset': row[1],
                'outcome': row[5],
                'up_token_id': row[6],
                'down_token_id': row[7]
            }
        
        # Fetch from API
        config = self.ASSETS.get(asset)
        if not config:
            return None
            
        slug = config.get_slug(timestamp)
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
            
            # Check if already resolved
            outcome = None
            if market.get('closed') and event.get('umaResolutionStatus') == 'resolved':
                prices = json.loads(market.get('outcomePrices', '[0.5, 0.5]'))
                if float(prices[0]) > 0.99:
                    outcome = 'up'
                elif float(prices[1]) > 0.99:
                    outcome = 'down'
            
            self.conn.execute('''
                INSERT OR IGNORE INTO markets 
                (timestamp, asset, slug, outcome, up_token_id, down_token_id)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (timestamp, asset, slug, outcome, up_token, down_token))
            self.conn.commit()
            
            return {
                'timestamp': timestamp,
                'asset': asset,
                'outcome': outcome,
                'up_token_id': up_token,
                'down_token_id': down_token
            }
            
        except Exception as e:
            logger.warning(f"Error fetching {asset} market {slug}: {e}")
            return None
    
    def record_orderbook(self, asset: str, market_ts: int, token_id: str) -> bool:
        """Record orderbook snapshot."""
        try:
            resp = self.session.get(
                f"{self.CLOB_API}/book",
                params={"token_id": token_id},
                timeout=3
            )
            resp.raise_for_status()
            book = resp.json()
            
            bids = book.get('bids', [])
            asks = book.get('asks', [])
            
            if not bids or not asks:
                return False
            
            best_bid = float(bids[0]['price'])
            best_ask = float(asks[0]['price'])
            mid = (best_bid + best_ask) / 2
            spread_bps = int((best_ask - best_bid) * 10_000)
            
            # Depth at best level only (saves space)
            bid_depth = float(bids[0]['price']) * float(bids[0]['size'])
            ask_depth = float(asks[0]['price']) * float(asks[0]['size'])
            
            # Check for duplicates
            cache_key = f"{asset}:{market_ts}"
            last = self._last_snapshots.get(cache_key)
            if last and last['bid'] == best_bid and last['ask'] == best_ask:
                return True  # Skip duplicate
            
            self.conn.execute('''
                INSERT INTO orderbook_snapshots 
                (market_ts, asset, recorded_at, best_bid, best_ask, mid_price, 
                 bid_depth, ask_depth, spread_bps)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (market_ts, asset, int(time.time() * 1000),
                  self._scale_price(best_bid), self._scale_price(best_ask),
                  self._scale_price(mid), bid_depth, ask_depth, spread_bps))
            self.conn.commit()
            
            self._last_snapshots[cache_key] = {'bid': best_bid, 'ask': best_ask}
            return True
            
        except Exception as e:
            logger.debug(f"Error recording {asset} orderbook: {e}")
            return False
    
    def check_resolutions(self, current_ts: int):
        """Check for newly resolved markets."""
        # Check markets from last few windows
        for offset in range(1, 5):
            ts = current_ts - (offset * 300)
            
            for asset in self.tracked_assets:
                cursor = self.conn.execute(
                    "SELECT outcome FROM markets WHERE timestamp = ? AND asset = ?",
                    (ts, asset)
                )
                row = cursor.fetchone()
                
                if row and row[0] is None:
                    # Check if resolved now
                    self._update_resolution(asset, ts)
    
    def _update_resolution(self, asset: str, timestamp: int):
        """Update market resolution."""
        try:
            config = self.ASSETS.get(asset)
            if not config:
                return
                
            slug = config.get_slug(timestamp)
            resp = self.session.get(
                f"{self.GAMMA_API}/events",
                params={"slug": slug},
                timeout=5
            )
            resp.raise_for_status()
            data = resp.json()
            
            if not data:
                return
            
            event = data[0]
            market = event.get('markets', [{}])[0]
            
            if market.get('closed') and event.get('umaResolutionStatus') == 'resolved':
                prices = json.loads(market.get('outcomePrices', '[0.5, 0.5]'))
                if float(prices[0]) > 0.99:
                    outcome = 'up'
                elif float(prices[1]) > 0.99:
                    outcome = 'down'
                else:
                    return
                
                self.conn.execute('''
                    UPDATE markets 
                    SET resolved_at = ?, outcome = ?
                    WHERE timestamp = ? AND asset = ?
                ''', (int(time.time()), outcome, timestamp, asset))
                self.conn.commit()
                
                logger.info(f"[RESOLVED] {asset} @ {datetime.fromtimestamp(timestamp)}: {outcome.upper()}")
                
        except Exception as e:
            logger.debug(f"Error checking resolution: {e}")
    
    def get_stats(self) -> dict:
        """Get collection statistics."""
        stats = {}
        
        cursor = self.conn.execute("SELECT COUNT(*) FROM markets")
        stats['total_markets'] = cursor.fetchone()[0]
        
        cursor = self.conn.execute(
            "SELECT COUNT(*) FROM markets WHERE outcome IS NOT NULL"
        )
        stats['resolved_markets'] = cursor.fetchone()[0]
        
        cursor = self.conn.execute("SELECT COUNT(*) FROM orderbook_snapshots")
        stats['total_snapshots'] = cursor.fetchone()[0]
        
        # By asset
        cursor = self.conn.execute('''
            SELECT asset, COUNT(*) FROM markets GROUP BY asset
        ''')
        stats['by_asset'] = {row[0]: row[1] for row in cursor.fetchall()}
        
        # File size
        stats['db_size_mb'] = round(Path(self.db_path).stat().st_size / (1024*1024), 2)
        
        return stats
    
    def run(self, interval: float = 5.0):
        """Main collection loop."""
        logger.info(f"Starting collector (interval: {interval}s)")
        logger.info(f"Database: {self.db_path}")
        logger.info("Press Ctrl+C to stop\n")
        
        last_stats_time = 0
        last_resolution_check = 0
        
        try:
            while True:
                now = int(time.time())
                current_window = (now // 300) * 300
                
                # Collect for each asset
                for asset in self.tracked_assets:
                    market = self.get_market(asset, current_window)
                    if not market:
                        continue
                    
                    # Record both sides (up and down tokens)
                    if market.get('up_token_id'):
                        self.record_orderbook(asset, current_window, market['up_token_id'])
                    if market.get('down_token_id'):
                        self.record_orderbook(asset, current_window, market['down_token_id'])
                
                # Check resolutions every 30 seconds
                if now - last_resolution_check > 30:
                    self.check_resolutions(current_window)
                    last_resolution_check = now
                
                # Print stats every 60 seconds
                if now - last_stats_time > 60:
                    stats = self.get_stats()
                    logger.info(
                        f"Markets: {stats['resolved_markets']}/{stats['total_markets']} resolved, "
                        f"Snapshots: {stats['total_snapshots']:,}, "
                        f"DB: {stats['db_size_mb']}MB"
                    )
                    last_stats_time = now
                
                time.sleep(interval)
                
        except KeyboardInterrupt:
            logger.info("\nStopping collector...")
        finally:
            self.conn.close()
            logger.info("Database saved.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--interval", type=float, default=5.0)
    parser.add_argument("--db", default="data/raw/polymarket_data.db")
    args = parser.parse_args()
    
    collector = PolymarketCollector(args.db)
    collector.run(interval=args.interval)
