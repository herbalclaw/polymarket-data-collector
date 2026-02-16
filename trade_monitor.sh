#!/bin/bash
# Trade monitor - alerts on new trades

LOG_FILE="/root/.openclaw/workspace/polymarket-data-collector/paper_trading.log"
LAST_TRADE=""

while true; do
    # Check for new trade opened
    NEW_TRADE=$(grep "PAPER TRADE OPENED" "$LOG_FILE" | tail -1)
    if [ "$NEW_TRADE" != "$LAST_TRADE" ] && [ -n "$NEW_TRADE" ]; then
        LAST_TRADE="$NEW_TRADE"
        
        # Extract trade details
        CYCLE=$(grep -B 5 "PAPER TRADE OPENED" "$LOG_FILE" | grep "Cycle" | tail -1)
        STRATEGY=$(grep -A 1 "PAPER TRADE OPENED" "$LOG_FILE" | grep "Strategy:" | tail -1)
        SIDE=$(grep -A 2 "PAPER TRADE OPENED" "$LOG_FILE" | grep "Side:" | tail -1)
        ENTRY=$(grep -A 3 "PAPER TRADE OPENED" "$LOG_FILE" | grep "Entry:" | tail -1)
        CONFIDENCE=$(grep -A 4 "PAPER TRADE OPENED" "$LOG_FILE" | grep "Confidence:" | tail -1)
        
        echo "🚨 NEW PAPER TRADE OPENED!"
        echo "$CYCLE"
        echo "$STRATEGY"
        echo "$SIDE"
        echo "$ENTRY"
        echo "$CONFIDENCE"
        echo ""
    fi
    
    # Check for closed trades
    CLOSED_TRADE=$(grep "PAPER TRADE CLOSED" "$LOG_FILE" | tail -1)
    if [ "$CLOSED_TRADE" != "$LAST_CLOSED" ] && [ -n "$CLOSED_TRADE" ]; then
        LAST_CLOSED="$CLOSED_TRADE"
        
        PNL=$(grep -A 2 "PAPER TRADE CLOSED" "$LOG_FILE" | grep "P&L:" | tail -1)
        REASON=$(grep -A 3 "PAPER TRADE CLOSED" "$LOG_FILE" | grep "Reason:" | tail -1)
        
        echo "📉 PAPER TRADE CLOSED!"
        echo "$PNL"
        echo "$REASON"
        echo ""
    fi
    
    sleep 2
done
