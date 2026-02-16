#!/usr/bin/env python3
"""
Multi-Source BTC Price Aggregator & Edge Detector

Pulls prices from multiple CEXs and DEXs to find:
1. Price discrepancies (arbitrage opportunities)
2. Leading indicators (which exchange moves first)
3. Optimal entry points based on aggregated data
"""

import asyncio
import aiohttp
import json
import time
from datetime import datetime
from dataclasses import dataclass
from typing import Dict, List, Optional
import statistics

@dataclass
class PriceData:
    exchange: str
    price: float
    bid: float
    ask: float
    spread: float
    volume_24h: float
    timestamp: float
    latency_ms: float

class MultiExchangeAggregator:
    """Aggregate BTC prices from multiple exchanges."""
    
    def __init__(self):
        self.prices: Dict[str, PriceData] = {}
        self.price_history: List[Dict] = []
        
    async def fetch_binance(self, session: aiohttp.ClientSession) -> Optional[PriceData]:
        """Fetch from Binance (spot)."""
        start = time.time()
        try:
            async with session.get(
                'https://api.binance.com/api/v3/ticker/bookTicker',
                params={'symbol': 'BTCUSDT'},
                timeout=aiohttp.ClientTimeout(total=2)
            ) as resp:
                data = await resp.json()
                latency = (time.time() - start) * 1000
                
                bid = float(data['bidPrice'])
                ask = float(data['askPrice'])
                
                return PriceData(
                    exchange='Binance',
                    price=(bid + ask) / 2,
                    bid=bid,
                    ask=ask,
                    spread=(ask - bid) / ((bid + ask) / 2) * 10000,  # bps
                    volume_24h=0,  # Would need separate call
                    timestamp=time.time(),
                    latency_ms=latency
                )
        except Exception as e:
            print(f"Binance error: {e}")
            return None
    
    async def fetch_coinbase(self, session: aiohttp.ClientSession) -> Optional[PriceData]:
        """Fetch from Coinbase."""
        start = time.time()
        try:
            async with session.get(
                'https://api.exchange.coinbase.com/products/BTC-USD/ticker',
                timeout=aiohttp.ClientTimeout(total=2)
            ) as resp:
                data = await resp.json()
                latency = (time.time() - start) * 1000
                
                bid = float(data['bid'])
                ask = float(data['ask'])
                price = float(data['price'])
                
                return PriceData(
                    exchange='Coinbase',
                    price=price,
                    bid=bid,
                    ask=ask,
                    spread=(ask - bid) / price * 10000,
                    volume_24h=float(data['volume']),
                    timestamp=time.time(),
                    latency_ms=latency
                )
        except Exception as e:
            print(f"Coinbase error: {e}")
            return None
    
    async def fetch_kraken(self, session: aiohttp.ClientSession) -> Optional[PriceData]:
        """Fetch from Kraken."""
        start = time.time()
        try:
            async with session.get(
                'https://api.kraken.com/0/public/Ticker',
                params={'pair': 'XBTUSD'},
                timeout=aiohttp.ClientTimeout(total=2)
            ) as resp:
                data = await resp.json()
                latency = (time.time() - start) * 1000
                
                ticker = data['result']['XXBTZUSD']
                bid = float(ticker['b'][0])
                ask = float(ticker['a'][0])
                
                return PriceData(
                    exchange='Kraken',
                    price=(bid + ask) / 2,
                    bid=bid,
                    ask=ask,
                    spread=(ask - bid) / ((bid + ask) / 2) * 10000,
                    volume_24h=float(ticker['v'][1]),
                    timestamp=time.time(),
                    latency_ms=latency
                )
        except Exception as e:
            print(f"Kraken error: {e}")
            return None
    
    async def fetch_bybit(self, session: aiohttp.ClientSession) -> Optional[PriceData]:
        """Fetch from Bybit."""
        start = time.time()
        try:
            async with session.get(
                'https://api.bybit.com/v5/market/tickers',
                params={'category': 'spot', 'symbol': 'BTCUSDT'},
                timeout=aiohttp.ClientTimeout(total=2)
            ) as resp:
                data = await resp.json()
                latency = (time.time() - start) * 1000
                
                ticker = data['result']['list'][0]
                bid = float(ticker['bid1Price'])
                ask = float(ticker['ask1Price'])
                
                return PriceData(
                    exchange='Bybit',
                    price=(bid + ask) / 2,
                    bid=bid,
                    ask=ask,
                    spread=(ask - bid) / ((bid + ask) / 2) * 10000,
                    volume_24h=float(ticker['turnover24h']),
                    timestamp=time.time(),
                    latency_ms=latency
                )
        except Exception as e:
            print(f"Bybit error: {e}")
            return None
    
    async def fetch_all_prices(self) -> Dict[str, PriceData]:
        """Fetch prices from all exchanges concurrently."""
        async with aiohttp.ClientSession() as session:
            tasks = [
                self.fetch_binance(session),
                self.fetch_coinbase(session),
                self.fetch_kraken(session),
                self.fetch_bybit(session),
            ]
            
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            self.prices = {}
            for result in results:
                if isinstance(result, PriceData):
                    self.prices[result.exchange] = result
            
            return self.prices
    
    def calculate_aggregated_metrics(self) -> Dict:
        """Calculate aggregated metrics across exchanges."""
        if len(self.prices) < 2:
            return {"error": "Need at least 2 exchanges"}
        
        prices = [p.price for p in self.prices.values()]
        bids = [p.bid for p in self.prices.values()]
        asks = [p.ask for p in self.prices.values()]
        latencies = [p.latency_ms for p in self.prices.values()]
        
        # Weighted average by volume
        total_volume = sum(p.volume_24h for p in self.prices.values())
        if total_volume > 0:
            vwap = sum(p.price * p.volume_24h for p in self.prices.values()) / total_volume
        else:
            vwap = statistics.mean(prices)
        
        metrics = {
            "timestamp": datetime.now().isoformat(),
            "exchanges": len(self.prices),
            "price_stats": {
                "min": min(prices),
                "max": max(prices),
                "mean": statistics.mean(prices),
                "median": statistics.median(prices),
                "vwap": vwap,
                "std_dev": statistics.stdev(prices) if len(prices) > 1 else 0,
            },
            "spread_stats": {
                "best_bid": max(bids),
                "best_ask": min(asks),
                "effective_spread": (min(asks) - max(bids)) / vwap * 10000,
            },
            "latency_stats": {
                "min_ms": min(latencies),
                "max_ms": max(latencies),
                "mean_ms": statistics.mean(latencies),
            },
            "arbitrage": {
                "max_price_diff": max(prices) - min(prices),
                "max_price_diff_pct": (max(prices) - min(prices)) / statistics.mean(prices) * 100,
            }
        }
        
        return metrics
    
    def detect_edge_signals(self) -> List[Dict]:
        """Detect potential trading signals."""
        signals = []
        
        if len(self.prices) < 2:
            return signals
        
        metrics = self.calculate_aggregated_metrics()
        
        # Signal 1: Large price discrepancy
        arb_pct = metrics['arbitrage']['max_price_diff_pct']
        if arb_pct > 0.1:  # >0.1% difference
            signals.append({
                "type": "ARBITRAGE",
                "strength": "HIGH" if arb_pct > 0.5 else "MEDIUM",
                "description": f"Price discrepancy of {arb_pct:.3f}% across exchanges",
                "action": "Check for arb opportunity"
            })
        
        # Signal 2: Low effective spread
        eff_spread = metrics['spread_stats']['effective_spread']
        if eff_spread < 5:  # <5 bps
            signals.append({
                "type": "TIGHT_SPREAD",
                "strength": "HIGH",
                "description": f"Tight spread: {eff_spread:.2f} bps",
                "action": "Good for entry"
            })
        
        # Signal 3: High volatility (std dev)
        std_dev = metrics['price_stats']['std_dev']
        mean_price = metrics['price_stats']['mean']
        cv = (std_dev / mean_price) * 100  # Coefficient of variation
        
        if cv > 0.05:  # >0.05% variation
            signals.append({
                "type": "HIGH_VOLATILITY",
                "strength": "MEDIUM" if cv < 0.1 else "HIGH",
                "description": f"High cross-exchange volatility: {cv:.3f}%",
                "action": "Wait for consolidation"
            })
        
        return signals
    
    async def run_continuous(self, interval_sec: float = 5.0):
        """Run continuous price aggregation."""
        print("="*70)
        print("Multi-Exchange BTC Price Aggregator")
        print("="*70)
        
        while True:
            print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Fetching prices...")
            
            await self.fetch_all_prices()
            
            if len(self.prices) >= 2:
                metrics = self.calculate_aggregated_metrics()
                signals = self.detect_edge_signals()
                
                print(f"\n📊 Aggregated Price: ${metrics['price_stats']['vwap']:,.2f}")
                print(f"   Range: ${metrics['price_stats']['min']:,.2f} - ${metrics['price_stats']['max']:,.2f}")
                print(f"   Std Dev: ${metrics['price_stats']['std_dev']:,.2f}")
                print(f"   Effective Spread: {metrics['spread_stats']['effective_spread']:.2f} bps")
                
                if signals:
                    print(f"\n🚨 SIGNALS DETECTED ({len(signals)}):")
                    for sig in signals:
                        print(f"   [{sig['type']}] {sig['strength']}: {sig['description']}")
                
                # Store for analysis
                self.price_history.append({
                    'timestamp': metrics['timestamp'],
                    'metrics': metrics,
                    'signals': signals
                })
            else:
                print("⚠️  Insufficient data from exchanges")
            
            await asyncio.sleep(interval_sec)


if __name__ == "__main__":
    aggregator = MultiExchangeAggregator()
    try:
        asyncio.run(aggregator.run_continuous(interval_sec=5.0))
    except KeyboardInterrupt:
        print("\n\nStopping aggregator...")
