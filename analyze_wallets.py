#!/usr/bin/env python3
"""
Analyze top Polymarket wallet strategies using official APIs
"""

from polymarket_apis import PolymarketGraphQLClient
from datetime import datetime, timedelta
import json

# Top wallets to analyze
WALLETS = [
    "0x9d84ce0306f8551e02efef1680475fc0f1dc1344",  # $2.6M profit
    "0xd218e474776403a330142299f7796e8ba32eb5c9",  # $958K profit
]

def analyze_wallet(wallet_address: str):
    """Analyze a specific wallet's trading strategy."""
    print(f"\n{'='*70}")
    print(f"Analyzing Wallet: {wallet_address}")
    print(f"{'='*70}\n")
    
    try:
        # Initialize client with orderbook subgraph (has trade data)
        client = PolymarketGraphQLClient(endpoint_name="orderbook_subgraph")
        
        # Query recent trades using orderFilledEvents
        query = f"""
        query GetTrades {{
          orderFilledEvents(
            where: {{maker: "{wallet_address.lower()}"}}
            orderBy: timestamp
            orderDirection: desc
            first: 50
          ) {{
            id
            timestamp
            maker
            taker
            makerAmountFilled
            takerAmountFilled
            makerAssetId
            takerAssetId
            fee
            transactionHash
          }}
        }}
        """
        
        result = client.query(query)
        trades = result.get('data', {}).get('orderFilledEvents', [])
        
        if not trades:
            print("No trades found for this wallet")
            return
        
        print(f"Found {len(trades)} recent trades\n")
        
        # Analyze patterns
        analysis = {
            'total_trades': len(trades),
            'btc_trades': 0,
            'avg_trade_size': 0,
            'early_entries': 0,  # Within first 30s
            'avg_entry_timing': [],
            'strategy_hypothesis': None
        }
        
        total_size = 0
        
        for trade in trades:
            ts = int(trade.get('timestamp', 0))
            maker_amount = float(trade.get('makerAmountFilled', 0))
            taker_amount = float(trade.get('takerAmountFilled', 0))
            maker_asset = trade.get('makerAssetId', '')
            
            total_size += maker_amount
            
            # Note: We need to correlate asset IDs with markets
            # For now, just show raw data
            print(f"  Trade: {datetime.fromtimestamp(ts)}")
            print(f"    Maker Amount: {maker_amount}")
            print(f"    Taker Amount: {taker_amount}")
            print(f"    Asset ID: {maker_asset[:20]}...")
            print()
        
        # Calculate averages
        if analysis['avg_entry_timing']:
            avg_timing = sum(analysis['avg_entry_timing']) / len(analysis['avg_entry_timing'])
            analysis['avg_entry_timing'] = avg_timing
        
        if total_size > 0:
            analysis['avg_trade_size'] = total_size / len(trades)
        
        # Strategy classification
        print(f"\n{'='*70}")
        print("STRATEGY ANALYSIS")
        print(f"{'='*70}\n")
        
        print(f"Total Trades Analyzed: {analysis['total_trades']}")
        print(f"BTC-Related Trades: {analysis['btc_trades']}")
        print(f"Average Trade Size: ${analysis['avg_trade_size']:,.2f}")
        
        if analysis['avg_entry_timing']:
            print(f"Average Entry Timing: {analysis['avg_entry_timing']:.1f}s into window")
            print(f"Early Entries (<30s): {analysis['early_entries']} ({analysis['early_entries']/analysis['btc_trades']*100:.1f}%)")
        
        # Classify strategy
        if analysis['early_entries'] / max(analysis['btc_trades'], 1) > 0.7:
            analysis['strategy_hypothesis'] = "LATENCY_ARBITRAGE"
            print(f"\n⚠️  STRATEGY: High-frequency/Latency Arbitrage")
            print("   Enters very early in window - likely using fast data feeds")
        elif analysis['avg_entry_timing'] > 60:
            analysis['strategy_hypothesis'] = "STRATEGIC_TIMING"
            print(f"\n✅ STRATEGY: Strategic Timing (Legitimate)")
            print("   Enters mid/late window - likely using price action analysis")
        else:
            analysis['strategy_hypothesis'] = "MIXED"
            print(f"\n⚠️  STRATEGY: Mixed/Unclear")
        
        print(f"\n{'='*70}\n")
        
    except Exception as e:
        print(f"Error analyzing wallet: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    print("Polymarket Wallet Strategy Analyzer")
    print("Using official Polymarket GraphQL API\n")
    
    for wallet in WALLETS:
        analyze_wallet(wallet)
