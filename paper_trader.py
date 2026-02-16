#!/usr/bin/env python3
"""
Paper Trading Strategy Tester for Polymarket BTC 5-Min

Tests multiple strategies using real market data without risking capital.
Strategies:
1. Multi-Exchange Momentum (follow aggregated price direction)
2. Arbitrage Detection (exploit price discrepancies)
3. VWAP Deviation (mean reversion to volume-weighted price)
4. Cross-Exchange Lead/Lag (front-run based on which exchange moves first)
"""

import json
import time
import asyncio
from datetime import datetime
from dataclasses import dataclass, field
from typing import List, Dict, Optional
from collections import deque

from price_aggregator import MultiExchangeAggregator, PriceData


@dataclass
class Trade:
    timestamp: float
    strategy: str
    side: str  # 'up' or 'down'
    entry_price: float
    confidence: float
    reason: str
    exit_price: Optional[float] = None
    exit_time: Optional[float] = None
    pnl: float = 0.0
    status: str = "open"  # open, closed


@dataclass
class StrategyPerformance:
    strategy_name: str
    total_trades: int = 0
    winning_trades: int = 0
    losing_trades: int = 0
    total_pnl: float = 0.0
    max_drawdown: float = 0.0
    trades: List[Trade] = field(default_factory=list)
    
    @property
    def win_rate(self) -> float:
        if self.total_trades == 0:
            return 0.0
        return self.winning_trades / self.total_trades
    
    @property
    def avg_pnl(self) -> float:
        if self.total_trades == 0:
            return 0.0
        return self.total_pnl / self.total_trades


class PaperTrader:
    """Paper trading system for testing strategies."""
    
    def __init__(self, initial_bankroll: float = 1000.0):
        self.bankroll = initial_bankroll
        self.initial_bankroll = initial_bankroll
        self.trades: List[Trade] = []
        self.active_trade: Optional[Trade] = None
        
        # Price history for analysis
        self.price_history: deque = deque(maxlen=100)
        self.aggregator = MultiExchangeAggregator()
        
        # Strategy performance tracking
        self.strategy_performance: Dict[str, StrategyPerformance] = {
            "momentum": StrategyPerformance("Multi-Exchange Momentum"),
            "arbitrage": StrategyPerformance("Arbitrage Detection"),
            "vwap": StrategyPerformance("VWAP Deviation"),
            "leadlag": StrategyPerformance("Cross-Exchange Lead/Lag"),
        }
        
        # Market state
        self.current_window_start = 0
        self.window_prices: List[float] = []
        
    def calculate_momentum_signal(self, prices: List[PriceData]) -> Optional[Dict]:
        """
        Strategy 1: Multi-Exchange Momentum
        If aggregated price is trending up/down across exchanges, follow it.
        """
        if len(self.price_history) < 10:
            return None
        
        # Calculate VWAP trend
        recent_vwaps = [p['metrics']['price_stats']['vwap'] 
                       for p in list(self.price_history)[-10:]]
        
        if len(recent_vwaps) < 5:
            return None
        
        # Simple moving average
        sma_short = sum(recent_vwaps[-3:]) / 3
        sma_long = sum(recent_vwaps) / len(recent_vwaps)
        
        price_change = (recent_vwaps[-1] - recent_vwaps[0]) / recent_vwaps[0] * 100
        
        if sma_short > sma_long * 1.001 and price_change > 0.05:
            return {
                "signal": "up",
                "confidence": min(abs(price_change) * 10, 0.9),
                "reason": f"Upward momentum: {price_change:.3f}% over {len(recent_vwaps)} samples"
            }
        elif sma_short < sma_long * 0.999 and price_change < -0.05:
            return {
                "signal": "down",
                "confidence": min(abs(price_change) * 10, 0.9),
                "reason": f"Downward momentum: {price_change:.3f}% over {len(recent_vwaps)} samples"
            }
        
        return None
    
    def calculate_arbitrage_signal(self, metrics: Dict) -> Optional[Dict]:
        """
        Strategy 2: Arbitrage Detection
        If large price discrepancy, bet on convergence.
        """
        arb_pct = metrics['arbitrage']['max_price_diff_pct']
        
        if arb_pct > 0.2:  # >0.2% difference
            # Find which exchange is highest/lowest
            prices = self.aggregator.prices
            
            max_exchange = max(prices.items(), key=lambda x: x[1].price)
            min_exchange = min(prices.items(), key=lambda x: x[1].price)
            
            # Bet that prices will converge toward mean
            mean_price = metrics['price_stats']['mean']
            
            if max_exchange[1].price > mean_price * 1.001:
                return {
                    "signal": "down",
                    "confidence": min(arb_pct * 2, 0.8),
                    "reason": f"Arbitrage: {max_exchange[0]} overpriced by {arb_pct:.3f}%"
                }
            elif min_exchange[1].price < mean_price * 0.999:
                return {
                    "signal": "up",
                    "confidence": min(arb_pct * 2, 0.8),
                    "reason": f"Arbitrage: {min_exchange[0]} underpriced by {arb_pct:.3f}%"
                }
        
        return None
    
    def calculate_vwap_signal(self, metrics: Dict) -> Optional[Dict]:
        """
        Strategy 3: VWAP Deviation
        If price deviates from VWAP, bet on mean reversion.
        """
        current_price = metrics['price_stats']['mean']
        vwap = metrics['price_stats']['vwap']
        
        deviation = (current_price - vwap) / vwap * 100
        
        if abs(deviation) > 0.1:  # >0.1% deviation
            if deviation > 0:
                return {
                    "signal": "down",
                    "confidence": min(abs(deviation) * 5, 0.85),
                    "reason": f"Price {deviation:.3f}% above VWAP, expecting reversion"
                }
            else:
                return {
                    "signal": "up",
                    "confidence": min(abs(deviation) * 5, 0.85),
                    "reason": f"Price {abs(deviation):.3f}% below VWAP, expecting reversion"
                }
        
        return None
    
    def calculate_leadlag_signal(self, prices: List[PriceData]) -> Optional[Dict]:
        """
        Strategy 4: Cross-Exchange Lead/Lag
        Detect which exchange leads and follow it.
        """
        if len(self.price_history) < 5:
            return None
        
        # Compare current prices to previous
        prev_data = list(self.price_history)[-5]
        prev_prices = {k: v for k, v in prev_data.items() if k.endswith('_price')}
        
        if not prev_prices:
            return None
        
        # Find exchange with biggest move
        max_change = 0
        leading_exchange = None
        leading_direction = None
        
        for exchange, price_data in self.aggregator.prices.items():
            prev_key = f"{exchange.lower()}_price"
            if prev_key in prev_prices:
                prev_price = prev_prices[prev_key]
                change = (price_data.price - prev_price) / prev_price * 100
                
                if abs(change) > abs(max_change):
                    max_change = change
                    leading_exchange = exchange
                    leading_direction = "up" if change > 0 else "down"
        
        if abs(max_change) > 0.05:  # >0.05% move
            return {
                "signal": leading_direction,
                "confidence": min(abs(max_change) * 10, 0.75),
                "reason": f"{leading_exchange} leading with {max_change:.3f}% move"
            }
        
        return None
    
    def evaluate_strategies(self) -> List[Dict]:
        """Run all strategies and return signals."""
        signals = []
        
        if len(self.aggregator.prices) < 2:
            return signals
        
        metrics = self.aggregator.calculate_aggregated_metrics()
        prices = list(self.aggregator.prices.values())
        
        # Strategy 1: Momentum
        mom_signal = self.calculate_momentum_signal(prices)
        if mom_signal:
            signals.append({"strategy": "momentum", **mom_signal})
        
        # Strategy 2: Arbitrage
        arb_signal = self.calculate_arbitrage_signal(metrics)
        if arb_signal:
            signals.append({"strategy": "arbitrage", **arb_signal})
        
        # Strategy 3: VWAP
        vwap_signal = self.calculate_vwap_signal(metrics)
        if vwap_signal:
            signals.append({"strategy": "vwap", **vwap_signal})
        
        # Strategy 4: Lead/Lag
        leadlag_signal = self.calculate_leadlag_signal(prices)
        if leadlag_signal:
            signals.append({"strategy": "leadlag", **leadlag_signal})
        
        return signals
    
    def execute_paper_trade(self, signal: Dict, current_price: float):
        """Execute a paper trade based on signal."""
        # Close existing trade if opposite signal
        if self.active_trade:
            if self.active_trade.side != signal['signal']:
                self.close_trade(current_price, "Signal reversal")
            else:
                return  # Same direction, hold
        
        # Open new trade
        trade = Trade(
            timestamp=time.time(),
            strategy=signal['strategy'],
            side=signal['signal'],
            entry_price=current_price,
            confidence=signal['confidence'],
            reason=signal['reason']
        )
        
        self.active_trade = trade
        self.trades.append(trade)
        
        print(f"\n📈 PAPER TRADE OPENED")
        print(f"   Strategy: {signal['strategy']}")
        print(f"   Side: {signal['signal'].upper()}")
        print(f"   Entry: ${current_price:,.2f}")
        print(f"   Confidence: {signal['confidence']:.1%}")
        print(f"   Reason: {signal['reason']}")
    
    def close_trade(self, exit_price: float, reason: str):
        """Close the active paper trade."""
        if not self.active_trade:
            return
        
        trade = self.active_trade
        trade.exit_price = exit_price
        trade.exit_time = time.time()
        trade.status = "closed"
        
        # Calculate P&L (simplified - assumes $1 per trade)
        if trade.side == "up":
            trade.pnl = (exit_price - trade.entry_price) / trade.entry_price * 100
        else:
            trade.pnl = (trade.entry_price - exit_price) / trade.entry_price * 100
        
        # Update strategy performance
        perf = self.strategy_performance[trade.strategy]
        perf.total_trades += 1
        perf.trades.append(trade)
        
        if trade.pnl > 0:
            perf.winning_trades += 1
        else:
            perf.losing_trades += 1
        
        perf.total_pnl += trade.pnl
        
        print(f"\n📉 PAPER TRADE CLOSED")
        print(f"   Strategy: {trade.strategy}")
        print(f"   P&L: {trade.pnl:+.3f}%")
        print(f"   Reason: {reason}")
        
        self.active_trade = None
    
    def print_performance(self):
        """Print strategy performance summary."""
        print("\n" + "="*70)
        print("PAPER TRADING PERFORMANCE")
        print("="*70)
        
        for name, perf in self.strategy_performance.items():
            if perf.total_trades > 0:
                print(f"\n{name}:")
                print(f"  Trades: {perf.total_trades} (W: {perf.winning_trades}, L: {perf.losing_trades})")
                print(f"  Win Rate: {perf.win_rate:.1%}")
                print(f"  Total P&L: {perf.total_pnl:+.3f}%")
                print(f"  Avg P&L per Trade: {perf.avg_pnl:+.3f}%")
        
        total_trades = sum(p.total_trades for p in self.strategy_performance.values())
        total_pnl = sum(p.total_pnl for p in self.strategy_performance.values())
        
        print(f"\n{'='*70}")
        print(f"TOTAL: {total_trades} trades, {total_pnl:+.3f}% P&L")
        print(f"{'='*70}")
    
    async def run(self):
        """Main paper trading loop."""
        print("="*70)
        print("PAPER TRADING BOT - Multi-Strategy")
        print("Testing strategies without real money")
        print("="*70)
        
        cycle = 0
        
        while True:
            cycle += 1
            print(f"\n{'='*70}")
            print(f"Cycle {cycle} - {datetime.now().strftime('%H:%M:%S')}")
            print(f"{'='*70}")
            
            # Fetch prices
            await self.aggregator.fetch_all_prices()
            
            if len(self.aggregator.prices) >= 2:
                metrics = self.aggregator.calculate_aggregated_metrics()
                
                # Store price history
                self.price_history.append({
                    'timestamp': time.time(),
                    'vwap': metrics['price_stats']['vwap'],
                    **{f"{k.lower()}_price": v.price for k, v in self.aggregator.prices.items()}
                })
                
                current_price = metrics['price_stats']['vwap']
                
                print(f"\nAggregated BTC Price: ${current_price:,.2f}")
                print(f"Spread: {metrics['spread_stats']['effective_spread']:.2f} bps")
                
                # Evaluate strategies
                signals = self.evaluate_strategies()
                
                if signals:
                    print(f"\n🎯 STRATEGY SIGNALS ({len(signals)}):")
                    for sig in signals:
                        print(f"   [{sig['strategy']}] {sig['signal'].upper()} @ {sig['confidence']:.1%}")
                    
                    # Take the highest confidence signal
                    best_signal = max(signals, key=lambda x: x['confidence'])
                    
                    if best_signal['confidence'] > 0.6:  # Minimum 60% confidence
                        self.execute_paper_trade(best_signal, current_price)
                
                # Check if we should close active trade (5-min window)
                if self.active_trade:
                    window_start = (int(time.time()) // 300) * 300
                    if window_start != self.current_window_start:
                        self.close_trade(current_price, "Window close")
                        self.current_window_start = window_start
                
                # Print performance every 10 cycles
                if cycle % 10 == 0:
                    self.print_performance()
            
            await asyncio.sleep(5)  # 5-second intervals


if __name__ == "__main__":
    trader = PaperTrader(initial_bankroll=1000.0)
    try:
        asyncio.run(trader.run())
    except KeyboardInterrupt:
        print("\n\nStopping paper trader...")
        trader.print_performance()
