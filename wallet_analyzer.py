#!/usr/bin/env python3
"""
Polymarket Wallet Strategy Analyzer

Deep analysis of profitable trader strategies by correlating:
- On-chain trade data
- BTC spot price movements
- Market timing patterns
- Position sizing behavior
"""

import requests
import json
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
import sqlite3

# Binance API for BTC price data
BINANCE_API = "https://api.binance.com/api/v3"

# Polymarket APIs
POLY_GAMMA = "https://gamma-api.polymarket.com"
POLY_CLOB = "https://clob.polymarket.com"

@dataclass
class Trade:
    """Represents a single trade."""
    timestamp: int
    market_slug: str
    side: str  # 'up' or 'down'
    size: float
    price: float
    tx_hash: str
    block_number: int

@dataclass
class BTCPrice:
    """BTC price at a specific time."""
    timestamp: int
    price: float
    change_1m: float
    change_5m: float
    volatility: float

class WalletAnalyzer:
    """Analyze wallet trading strategies."""
    
    def __init__(self, wallet_address: str):
        self.wallet = wallet_address
        self.trades: List[Trade] = []
        self.btc_prices: Dict[int, BTCPrice] = {}
        
    def fetch_trades(self, days: int = 7) -> List[Trade]:
        """Fetch all trades for this wallet from Polymarket."""
        # This would use Polymarket's API or subgraph
        # For now, placeholder structure
        trades = []
        
        # Example: Query Polymarket subgraph
        query = """
        {
          trades(where: {
            trader: "%s",
            timestamp_gt: %d
          }, orderBy: timestamp, orderDirection: desc) {
            timestamp
            market { slug }
            side
            size
            price
            transactionHash
            blockNumber
          }
        }
        """ % (self.wallet.lower(), int((datetime.now() - timedelta(days=days)).timestamp()))
        
        # Would call subgraph here
        # response = requests.post(POLY_SUBGRAPH, json={'query': query})
        
        return trades
    
    def fetch_btc_price_history(self, start_ts: int, end_ts: int) -> Dict[int, BTCPrice]:
        """Fetch BTC price history from Binance."""
        prices = {}
        
        # Get 1-minute klines
        url = f"{BINANCE_API}/klines"
        params = {
            'symbol': 'BTCUSDT',
            'interval': '1m',
            'startTime': start_ts * 1000,
            'endTime': end_ts * 1000,
            'limit': 1000
        }
        
        try:
            response = requests.get(url, params=params, timeout=10)
            data = response.json()
            
            for i, candle in enumerate(data):
                ts = candle[0] // 1000  # Convert to seconds
                price = float(candle[4])  # Close price
                
                # Calculate changes
                change_1m = 0
                change_5m = 0
                volatility = 0
                
                if i > 0:
                    prev_price = float(data[i-1][4])
                    change_1m = (price - prev_price) / prev_price * 100
                
                if i >= 5:
                    price_5m_ago = float(data[i-5][4])
                    change_5m = (price - price_5m_ago) / price_5m_ago * 100
                    
                    # Simple volatility (high-low range)
                    highs = [float(data[j][2]) for j in range(i-5, i+1)]
                    lows = [float(data[j][3]) for j in range(i-5, i+1)]
                    volatility = (max(highs) - min(lows)) / price * 100
                
                prices[ts] = BTCPrice(
                    timestamp=ts,
                    price=price,
                    change_1m=change_1m,
                    change_5m=change_5m,
                    volatility=volatility
                )
        except Exception as e:
            print(f"Error fetching BTC prices: {e}")
        
        return prices
    
    def analyze_strategy(self) -> Dict:
        """Analyze the trading strategy."""
        if not self.trades:
            return {"error": "No trades found"}
        
        analysis = {
            "wallet": self.wallet,
            "total_trades": len(self.trades),
            "strategy_type": None,
            "confidence": 0,
            "patterns": [],
            "indicators": []
        }
        
        # Analyze timing patterns
        trade_times = [t.timestamp for t in self.trades]
        time_diffs = [trade_times[i] - trade_times[i+1] for i in range(len(trade_times)-1)]
        
        avg_time_between = sum(time_diffs) / len(time_diffs) if time_diffs else 0
        
        # Strategy detection patterns
        
        # 1. Check for latency arbitrage (trades within seconds of market open)
        early_trades = sum(1 for t in self.trades if t.timestamp % 300 < 10)  # First 10s of 5-min window
        early_pct = early_trades / len(self.trades) if self.trades else 0
        
        if early_pct > 0.7:
            analysis["patterns"].append("High frequency early-entry (latency arbitrage)")
            analysis["strategy_type"] = "LATENCY_ARBITRAGE"
            analysis["confidence"] = 0.9
        
        # 2. Check for trend following (trades aligned with BTC momentum)
        trend_aligned = 0
        for trade in self.trades:
            btc_price = self._get_nearest_btc_price(trade.timestamp)
            if btc_price:
                if trade.side == 'up' and btc_price.change_5m > 0.1:
                    trend_aligned += 1
                elif trade.side == 'down' and btc_price.change_5m < -0.1:
                    trend_aligned += 1
        
        trend_pct = trend_aligned / len(self.trades) if self.trades else 0
        
        if trend_pct > 0.65 and analysis["strategy_type"] != "LATENCY_ARBITRAGE":
            analysis["patterns"].append(f"Trend following ({trend_pct:.1%} aligned with BTC momentum)")
            analysis["strategy_type"] = "TREND_FOLLOWING"
            analysis["confidence"] = trend_pct
        
        # 3. Check for contrarian/streak reversal (trades against recent direction)
        # Would need more historical data to detect streaks
        
        # 4. Check for volatility-based (trades during high volatility)
        high_vol_trades = 0
        for trade in self.trades:
            btc_price = self._get_nearest_btc_price(trade.timestamp)
            if btc_price and btc_price.volatility > 0.5:  # >0.5% volatility
                high_vol_trades += 1
        
        vol_pct = high_vol_trades / len(self.trades) if self.trades else 0
        
        if vol_pct > 0.6 and not analysis["strategy_type"]:
            analysis["patterns"].append(f"Volatility-based trading ({vol_pct:.1%} in high vol)")
            analysis["strategy_type"] = "VOLATILITY_BASED"
            analysis["confidence"] = vol_pct
        
        # 5. Check for support/resistance (trades at price levels)
        # Would need order book data
        
        analysis["indicators"] = {
            "avg_time_between_trades_sec": avg_time_between,
            "early_entry_pct": early_pct,
            "trend_aligned_pct": trend_pct,
            "high_vol_pct": vol_pct
        }
        
        return analysis
    
    def _get_nearest_btc_price(self, timestamp: int) -> Optional[BTCPrice]:
        """Get BTC price closest to timestamp."""
        # Round to nearest minute
        minute_ts = (timestamp // 60) * 60
        return self.btc_prices.get(minute_ts)
    
    def generate_report(self) -> str:
        """Generate a detailed strategy report."""
        analysis = self.analyze_strategy()
        
        report = f"""
{'='*60}
WALLET STRATEGY ANALYSIS
{'='*60}

Wallet: {self.wallet}
Analysis Date: {datetime.now()}

STRATEGY CLASSIFICATION:
Type: {analysis.get('strategy_type', 'UNKNOWN')}
Confidence: {analysis.get('confidence', 0):.1%}

DETECTED PATTERNS:
"""
        for pattern in analysis.get('patterns', []):
            report += f"  • {pattern}\n"
        
        report += f"\nTECHNICAL INDICATORS:\n"
        for key, value in analysis.get('indicators', {}).items():
            if isinstance(value, float):
                report += f"  • {key}: {value:.3f}\n"
            else:
                report += f"  • {key}: {value}\n"
        
        report += f"\n{'='*60}\n"
        
        return report


def analyze_top_wallets():
    """Analyze the top profitable wallets we identified."""
    
    # Top wallets from research
    wallets = [
        "0x9d84ce0306f8551e02efef1680475fc0f1dc1344",  # $2.6M profit, 63% WR
        "0xd218e474776403a330142299f7796e8ba32eb5c9",  # $958K profit, 67% WR
    ]
    
    print("Starting deep strategy analysis of top Polymarket wallets...")
    print("This will take several minutes to collect and correlate data.\n")
    
    for wallet in wallets:
        print(f"\nAnalyzing wallet: {wallet[:20]}...")
        analyzer = WalletAnalyzer(wallet)
        
        # Fetch trades
        print("  Fetching trade history...")
        trades = analyzer.fetch_trades(days=7)
        
        if not trades:
            print("  ⚠️  No trades found or API unavailable")
            continue
        
        print(f"  Found {len(trades)} trades")
        
        # Fetch BTC price data
        if trades:
            start_ts = min(t.timestamp for t in trades)
            end_ts = max(t.timestamp for t in trades)
            print(f"  Fetching BTC price data ({datetime.fromtimestamp(start_ts)} to {datetime.fromtimestamp(end_ts)})...")
            analyzer.btc_prices = analyzer.fetch_btc_price_history(start_ts, end_ts)
            print(f"  Got {len(analyzer.btc_prices)} price points")
        
        # Generate report
        report = analyzer.generate_report()
        print(report)
        
        # Save to file
        filename = f"wallet_analysis_{wallet[:20]}.txt"
        with open(filename, 'w') as f:
            f.write(report)
        print(f"  Report saved to: {filename}")


if __name__ == "__main__":
    analyze_top_wallets()
