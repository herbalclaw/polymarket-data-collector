#!/bin/bash
# Start the paper trading bot for strategy testing

cd /root/.openclaw/workspace/polymarket-data-collector
source venv/bin/activate

echo "Starting Paper Trading Bot..."
echo "Strategies: Momentum, Arbitrage, VWAP, Lead/Lag"
echo ""

python paper_trader.py
