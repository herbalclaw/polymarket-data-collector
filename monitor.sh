#!/bin/bash
# Simple trade reporter for Lucas

LOG="/root/.openclaw/workspace/polymarket-data-collector/paper_trading.log"

while true; do
    clear
    echo "=== Paper Trading Bot Monitor ==="
    echo "Time: $(date)"
    echo ""
    
    if [ -s "$LOG" ]; then
        # Show last 30 lines
        tail -30 "$LOG"
        
        # Check for trades
        if grep -q "PAPER TRADE" "$LOG"; then
            echo ""
            echo "=== TRADES DETECTED ==="
            grep "PAPER TRADE" "$LOG"
            grep "P&L:" "$LOG" | tail -5
        fi
    else
        echo "Log is empty - bot may be starting..."
        echo "PID: $(pgrep -f paper_trader | head -1)"
    fi
    
    sleep 10
done
