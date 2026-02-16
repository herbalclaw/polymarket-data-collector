#!/usr/bin/env python3
"""
Strategy Discovery for Polymarket BTC 5-Minute Markets

This script analyzes:
1. Historical market data patterns
2. Price action around entry points
3. Optimal timing strategies
4. Edge detection through data analysis
"""

import sqlite3
import json
from datetime import datetime, timedelta
from collections import defaultdict
import statistics

class StrategyDiscovery:
    """Discover trading edges through data analysis."""
    
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
        
    def analyze_streak_patterns(self) -> dict:
        """Analyze streak reversal patterns from collected data."""
        print("="*70)
        print("STREAK REVERSAL ANALYSIS")
        print("="*70)
        
        # Get all price updates ordered by time
        cursor = self.conn.execute('''
            SELECT timestamp_ms, side, mid, market_ts 
            FROM price_updates 
            ORDER BY timestamp_ms ASC
        ''')
        
        rows = cursor.fetchall()
        if len(rows) < 10:
            print(f"⚠️  Insufficient data: only {len(rows)} price updates")
            return {}
        
        print(f"\nAnalyzing {len(rows)} price updates...\n")
        
        # Group by market window
        windows = defaultdict(list)
        for row in rows:
            ts, side, mid, market_ts = row
            windows[market_ts].append({
                'timestamp': ts,
                'side': side,
                'mid': mid / 1_000_000  # Convert from scaled
            })
        
        print(f"Found {len(windows)} market windows\n")
        
        # Analyze outcomes (which side won)
        outcomes = []
        for market_ts, updates in sorted(windows.items()):
            if len(updates) < 2:
                continue
                
            # Determine outcome based on final price
            first_price = updates[0]['mid']
            last_price = updates[-1]['mid']
            
            outcome = 'up' if last_price > first_price else 'down'
            outcomes.append({
                'market_ts': market_ts,
                'outcome': outcome,
                'first_price': first_price,
                'last_price': last_price,
                'change_pct': (last_price - first_price) / first_price * 100
            })
        
        if len(outcomes) < 5:
            print(f"⚠️  Insufficient completed windows: {len(outcomes)}")
            return {}
        
        # Detect streaks
        streaks = []
        current_streak = 1
        current_side = outcomes[0]['outcome']
        
        for i in range(1, len(outcomes)):
            if outcomes[i]['outcome'] == current_side:
                current_streak += 1
            else:
                streaks.append({
                    'side': current_side,
                    'length': current_streak,
                    'end_index': i - 1
                })
                current_streak = 1
                current_side = outcomes[i]['outcome']
        
        # Add final streak
        streaks.append({
            'side': current_side,
            'length': current_streak,
            'end_index': len(outcomes) - 1
        })
        
        print(f"Detected {len(streaks)} streaks\n")
        
        # Analyze reversal after streaks
        reversal_stats = defaultdict(lambda: {'total': 0, 'reversed': 0})
        
        for i, streak in enumerate(streaks[:-1]):  # Skip last (no next outcome)
            streak_length = streak['length']
            next_outcome = outcomes[streak['end_index'] + 1]['outcome']
            reversed = next_outcome != streak['side']
            
            reversal_stats[streak_length]['total'] += 1
            if reversed:
                reversal_stats[streak_length]['reversed'] += 1
        
        # Print results
        print("STREAK REVERSAL STATISTICS:")
        print("-" * 50)
        print(f"{'Streak Length':<15} {'Reversal Rate':<15} {'Sample Size':<15}")
        print("-" * 50)
        
        for length in sorted(reversal_stats.keys()):
            stats = reversal_stats[length]
            if stats['total'] >= 3:  # Minimum sample size
                rate = stats['reversed'] / stats['total']
                print(f"{length:<15} {rate:>13.1%} {stats['total']:>13}")
        
        print("-" * 50)
        
        # Find optimal trigger
        best_edge = 0
        best_trigger = None
        
        for length, stats in reversal_stats.items():
            if stats['total'] >= 5:  # Need decent sample
                rate = stats['reversed'] / stats['total']
                edge = rate - 0.5  # Edge over random
                if edge > best_edge:
                    best_edge = edge
                    best_trigger = length
        
        if best_trigger:
            print(f"\n✅ OPTIMAL STRATEGY: Bet on reversal after {best_trigger}-streak")
            print(f"   Expected edge: {best_edge:.1%} over random")
            print(f"   Win rate: {reversal_stats[best_trigger]['reversed'] / reversal_stats[best_trigger]['total']:.1%}")
        
        return reversal_stats
    
    def analyze_entry_timing(self) -> dict:
        """Analyze optimal entry timing within 5-min window."""
        print("\n" + "="*70)
        print("ENTRY TIMING ANALYSIS")
        print("="*70)
        
        cursor = self.conn.execute('''
            SELECT timestamp_ms, market_ts, side, mid
            FROM price_updates
            ORDER BY timestamp_ms ASC
        ''')
        
        rows = cursor.fetchall()
        
        # Group by window and analyze price movement
        windows = defaultdict(list)
        for row in rows:
            ts, market_ts, side, mid = row
            seconds_into_window = (ts - market_ts * 1000) / 1000
            windows[market_ts].append({
                'seconds_in': seconds_into_window,
                'side': side,
                'mid': mid / 1_000_000
            })
        
        # Analyze volatility by time segment
        segments = defaultdict(list)
        
        for market_ts, updates in windows.items():
            if len(updates) < 5:
                continue
            
            for update in updates:
                sec = update['seconds_in']
                # Divide window into segments
                if sec < 30:
                    segment = '0-30s (Early)'
                elif sec < 60:
                    segment = '30-60s (Early-Mid)'
                elif sec < 120:
                    segment = '60-120s (Mid)'
                elif sec < 180:
                    segment = '120-180s (Mid-Late)'
                else:
                    segment = '180s+ (Late)'
                
                segments[segment].append(update['mid'])
        
        print("\nPRICE VOLATILITY BY ENTRY TIME:")
        print("-" * 60)
        print(f"{'Time Segment':<25} {'Avg Price':<15} {'Std Dev':<15}")
        print("-" * 60)
        
        for segment in ['0-30s (Early)', '30-60s (Early-Mid)', '60-120s (Mid)', 
                        '120-180s (Mid-Late)', '180s+ (Late)']:
            if segment in segments and len(segments[segment]) > 5:
                prices = segments[segment]
                avg = statistics.mean(prices)
                std = statistics.stdev(prices) if len(prices) > 1 else 0
                print(f"{segment:<25} {avg:>13.4f} {std:>13.4f}")
        
        print("-" * 60)
        print("\n💡 INSIGHT: Lower std dev = more predictable prices")
        
        return segments
    
    def analyze_price_levels(self) -> dict:
        """Analyze which price levels are most predictive."""
        print("\n" + "="*70)
        print("PRICE LEVEL ANALYSIS")
        print("="*70)
        
        cursor = self.conn.execute('''
            SELECT market_ts, side, mid
            FROM price_updates
            WHERE side = 'up'
            GROUP BY market_ts
            HAVING timestamp_ms = MIN(timestamp_ms)
        ''')
        
        up_prices = [row[2] / 1_000_000 for row in cursor.fetchall()]
        
        cursor = self.conn.execute('''
            SELECT market_ts, side, mid
            FROM price_updates
            WHERE side = 'down'
            GROUP BY market_ts
            HAVING timestamp_ms = MIN(timestamp_ms)
        ''')
        
        down_prices = [row[2] / 1_000_000 for row in cursor.fetchall()]
        
        if up_prices and down_prices:
            print(f"\nInitial UP token prices: {min(up_prices):.4f} - {max(up_prices):.4f}")
            print(f"Initial DOWN token prices: {min(down_prices):.4f} - {max(down_prices):.4f}")
            print(f"\nAvg UP price: {statistics.mean(up_prices):.4f}")
            print(f"Avg DOWN price: {statistics.mean(down_prices):.4f}")
            
            # Check for mispricing opportunities
            combined = [p + d for p, d in zip(up_prices, down_prices)]
            if combined:
                print(f"\nAvg combined price (should be ~1.0): {statistics.mean(combined):.4f}")
                
                mispriced = [c for c in combined if c < 0.98 or c > 1.02]
                if mispriced:
                    print(f"⚠️  Found {len(mispriced)} mispriced windows!")
        
        return {}
    
    def generate_strategy_report(self):
        """Generate complete strategy discovery report."""
        print("\n" + "="*70)
        print("POLYMARKET BTC 5-MIN STRATEGY DISCOVERY REPORT")
        print(f"Generated: {datetime.now()}")
        print("="*70)
        
        # Run all analyses
        streak_data = self.analyze_streak_patterns()
        timing_data = self.analyze_entry_timing()
        price_data = self.analyze_price_levels()
        
        # Summary
        print("\n" + "="*70)
        print("STRATEGY RECOMMENDATIONS")
        print("="*70)
        
        print("""
Based on data analysis, here are the recommended strategies:

1. STREAK REVERSAL (Primary Strategy)
   - Wait for N consecutive same-direction outcomes
   - Bet on reversal
   - Optimal trigger: TBD from data
   - Expected edge: TBD from data

2. ENTRY TIMING
   - Avoid first 30 seconds (high volatility)
   - Enter between 60-180 seconds for stability
   - Monitor price levels for mispricing

3. RISK MANAGEMENT
   - Max 5% bankroll per trade
   - Stop after 3 consecutive losses
   - Daily loss limit: 20% of bankroll

4. DATA COLLECTION
   - Continue collecting price data
   - Re-analyze weekly for pattern shifts
   - Track your own trades for performance
""")


if __name__ == "__main__":
    import sys
    
    db_path = sys.argv[1] if len(sys.argv) > 1 else "data/raw/btc_hf_2026-02-16_PM.db"
    
    print(f"Loading data from: {db_path}")
    
    discovery = StrategyDiscovery(db_path)
    discovery.generate_strategy_report()
