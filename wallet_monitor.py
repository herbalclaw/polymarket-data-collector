#!/usr/bin/env python3
"""
Real-time Polymarket Wallet Monitor

Monitors specific wallets and analyzes their trades in real-time
to decipher their strategy by correlating with:
- BTC spot price movements
- Market timing
- Price action patterns
"""

import requests
import time
import json
from datetime import datetime, timedelta
from collections import deque
import threading

# APIs
BINANCE_API = "https://api.binance.com/api/v3"
POLY_GAMMA = "https://gamma-api.polymarket.com"

class RealTimeWalletMonitor:
    """Monitor wallets in real-time to understand their strategy."""
    
    def __init__(self, wallet_address: str):
        self.wallet = wallet_address
        self.btc_prices = deque(maxlen=100)  # Last 100 BTC prices
        self.trades_seen = set()  # Track seen trades
        self.strategy_log = []
        
        # Strategy hypothesis tracking
        self.hypotheses = {
            'trend_following': {'score': 0, 'tests': 0},
            'mean_reversion': {'score': 0, 'tests': 0},
            'breakout': {'score': 0, 'tests': 0},
            'support_resistance': {'score': 0, 'tests': 0},
            'latency_arbitrage': {'score': 0, 'tests': 0},
        }
    
    def fetch_btc_price(self) -> dict:
        """Get current BTC price from Binance."""
        try:
            response = requests.get(
                f"{BINANCE_API}/ticker/24hr",
                params={'symbol': 'BTCUSDT'},
                timeout=5
            )
            data = response.json()
            return {
                'price': float(data['lastPrice']),
                'change_1h': float(data['priceChangePercent']),
                'high': float(data['highPrice']),
                'low': float(data['lowPrice']),
                'volume': float(data['volume']),
                'timestamp': int(time.time())
            }
        except Exception as e:
            print(f"Error fetching BTC price: {e}")
            return None
    
    def fetch_recent_trades(self) -> list:
        """Fetch recent trades for this wallet."""
        # This would query Polymarket API or subgraph
        # Placeholder for structure
        trades = []
        
        # In reality, would query:
        # - Polymarket subgraph
        # - Or API endpoint
        
        return trades
    
    def analyze_trade_context(self, trade: dict, btc_data: dict) -> dict:
        """Analyze the context of a trade."""
        analysis = {
            'timestamp': trade.get('timestamp'),
            'side': trade.get('side'),
            'size': trade.get('size'),
            'btc_price_at_trade': btc_data['price'] if btc_data else None,
            'btc_trend_1h': btc_data['change_1h'] if btc_data else None,
            'market_position': None,  # Early, mid, late in 5-min window
            'pattern_detected': None,
        }
        
        # Determine market position (early = latency arb, mid/late = strategy)
        if trade.get('timestamp'):
            window_start = (trade['timestamp'] // 300) * 300
            seconds_into_window = trade['timestamp'] - window_start
            analysis['market_position'] = seconds_into_window
            
            if seconds_into_window < 15:
                analysis['pattern_detected'] = 'VERY_EARLY_ENTRY'
                self.hypotheses['latency_arbitrage']['score'] += 1
            elif seconds_into_window < 60:
                analysis['pattern_detected'] = 'EARLY_ENTRY'
            else:
                analysis['pattern_detected'] = 'STRATEGIC_TIMING'
        
        # Check trend alignment
        if btc_data and trade.get('side'):
            if trade['side'] == 'up' and btc_data['change_1h'] > 0.5:
                analysis['trend_alignment'] = 'WITH_TREND'
                self.hypotheses['trend_following']['score'] += 1
            elif trade['side'] == 'down' and btc_data['change_1h'] < -0.5:
                analysis['trend_alignment'] = 'WITH_TREND'
                self.hypotheses['trend_following']['score'] += 1
            elif trade['side'] == 'up' and btc_data['change_1h'] < -0.5:
                analysis['trend_alignment'] = 'AGAINST_TREND'
                self.hypotheses['mean_reversion']['score'] += 1
            elif trade['side'] == 'down' and btc_data['change_1h'] > 0.5:
                analysis['trend_alignment'] = 'AGAINST_TREND'
                self.hypotheses['mean_reversion']['score'] += 1
            else:
                analysis['trend_alignment'] = 'NEUTRAL'
        
        self.hypotheses['trend_following']['tests'] += 1
        self.hypotheses['mean_reversion']['tests'] += 1
        
        return analysis
    
    def print_strategy_hypothesis(self):
        """Print current strategy hypothesis."""
        print("\n" + "="*60)
        print("STRATEGY HYPOTHESIS (based on observed trades)")
        print("="*60)
        
        for strategy, data in self.hypotheses.items():
            if data['tests'] > 0:
                confidence = data['score'] / data['tests']
                bar = '█' * int(confidence * 20) + '░' * (20 - int(confidence * 20))
                print(f"{strategy:20s}: {bar} {confidence:.1%} ({data['score']}/{data['tests']})")
        
        # Determine most likely strategy
        best = max(self.hypotheses.items(), key=lambda x: x[1]['score'] / x[1]['tests'] if x[1]['tests'] > 0 else 0)
        if best[1]['tests'] >= 5:
            print(f"\n🏆 MOST LIKELY STRATEGY: {best[0].upper()}")
            print(f"   Confidence: {best[1]['score'] / best[1]['tests']:.1%}")
        
        print("="*60 + "\n")
    
    def run(self, duration_minutes: int = 60):
        """Run real-time monitoring."""
        print(f"\n🔍 Starting real-time monitoring of wallet: {self.wallet[:20]}...")
        print(f"   Duration: {duration_minutes} minutes")
        print(f"   Will analyze each trade and correlate with BTC price\n")
        
        start_time = time.time()
        check_interval = 30  # Check every 30 seconds
        
        try:
            while time.time() - start_time < duration_minutes * 60:
                # Fetch BTC price
                btc_data = self.fetch_btc_price()
                if btc_data:
                    self.btc_prices.append(btc_data)
                    print(f"[{datetime.now().strftime('%H:%M:%S')}] BTC: ${btc_data['price']:,.2f} ({btc_data['change_1h']:+.2f}% 1h)")
                
                # Fetch recent trades
                trades = self.fetch_recent_trades()
                new_trades = [t for t in trades if t.get('id') not in self.trades_seen]
                
                for trade in new_trades:
                    self.trades_seen.add(trade.get('id'))
                    print(f"\n🎯 NEW TRADE DETECTED!")
                    print(f"   Time: {datetime.fromtimestamp(trade.get('timestamp', 0))}")
                    print(f"   Side: {trade.get('side', 'unknown').upper()}")
                    print(f"   Size: ${trade.get('size', 0):,.2f}")
                    
                    # Analyze context
                    analysis = self.analyze_trade_context(trade, btc_data)
                    print(f"   Pattern: {analysis.get('pattern_detected')}")
                    print(f"   Trend Alignment: {analysis.get('trend_alignment')}")
                    
                    self.strategy_log.append(analysis)
                
                # Print strategy hypothesis every 5 minutes
                elapsed = time.time() - start_time
                if int(elapsed) % 300 < check_interval:
                    self.print_strategy_hypothesis()
                
                time.sleep(check_interval)
                
        except KeyboardInterrupt:
            print("\n\n⏹️  Monitoring stopped by user")
        
        # Final report
        print("\n" + "="*60)
        print("FINAL ANALYSIS REPORT")
        print("="*60)
        self.print_strategy_hypothesis()
        
        print(f"\nTotal trades observed: {len(self.trades_seen)}")
        print(f"Monitoring duration: {int((time.time() - start_time) / 60)} minutes")


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python wallet_monitor.py <wallet_address> [duration_minutes]")
        print("\nExample:")
        print("  python wallet_monitor.py 0x9d84ce0306f8551e02efef1680475fc0f1dc1344 120")
        sys.exit(1)
    
    wallet = sys.argv[1]
    duration = int(sys.argv[2]) if len(sys.argv) > 2 else 60
    
    monitor = RealTimeWalletMonitor(wallet)
    monitor.run(duration_minutes=duration)
