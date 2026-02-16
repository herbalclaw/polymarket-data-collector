#!/bin/bash
# Health monitor for Polymarket data collector
# Run this via cron every 5 minutes

cd /root/.openclaw/workspace/polymarket-data-collector

LOG_FILE="health_check.log"
DB_FILE=$(ls -t data/raw/btc_hf_*.db 2>/dev/null | head -1)
TIMESTAMP=$(date '+%Y-%m-%d %H:%M:%S')

# Check if collector is running
COLLECTOR_PID=$(pgrep -f "collector_hybrid.py" | head -1)

if [ -z "$COLLECTOR_PID" ]; then
    echo "[$TIMESTAMP] ❌ CRITICAL: Collector not running! Restarting..." >> $LOG_FILE
    
    # Restart collector
    source venv/bin/activate
    nohup python collector_hybrid.py > collector.log 2>&1 &
    echo "[$TIMESTAMP] ✅ Collector restarted (PID: $!)" >> $LOG_FILE
else
    # Check if collector is responsive (last update within 60 seconds)
    if [ -n "$DB_FILE" ]; then
        LAST_UPDATE=$(source venv/bin/activate && python -c "
import sqlite3
import time
try:
    conn = sqlite3.connect('$DB_FILE')
    cursor = conn.execute('SELECT MAX(timestamp_ms) FROM price_updates')
    result = cursor.fetchone()[0]
    if result:
        print(int(result / 1000))
    else:
        print(0)
except:
    print(0)
" 2>/dev/null)
        
        CURRENT_TIME=$(date +%s)
        TIME_DIFF=$((CURRENT_TIME - LAST_UPDATE))
        
        if [ "$TIME_DIFF" -gt 120 ]; then
            echo "[$TIMESTAMP] ⚠️  WARNING: No updates for ${TIME_DIFF}s" >> $LOG_FILE
            
            # Check if it's API issues
            if grep -q "ReadTimeoutError" collector.log | tail -5; then
                echo "[$TIMESTAMP] ⚠️  API timeouts detected" >> $LOG_FILE
            fi
        else
            echo "[$TIMESTAMP] ✅ Healthy: Last update ${TIME_DIFF}s ago, PID: $COLLECTOR_PID" >> $LOG_FILE
        fi
    fi
fi

# Check auto-push
PUSH_PID=$(pgrep -f "git.*push" | head -1)
if [ -z "$PUSH_PID" ]; then
    echo "[$TIMESTAMP] ❌ Auto-push not running! Restarting..." >> $LOG_FILE
    
    nohup bash -c '
while true; do
    sleep 300
    cd /root/.openclaw/workspace/polymarket-data-collector
    DB_FILE=$(ls -t data/raw/btc_hf_*.db 2>/dev/null | head -1)
    if [ -n "$DB_FILE" ]; then
        git add "$DB_FILE" .gitignore 2>/dev/null
        git diff --cached --quiet || {
            TIMESTAMP=$(date -u +"%Y-%m-%d-%H:%M:%S")
            git commit -m "Data: $TIMESTAMP UTC | Auto-push" 2>/dev/null
            git push origin master 2>/dev/null
        }
    fi
done
' > git_push.log 2>&1 &
    
    echo "[$TIMESTAMP] ✅ Auto-push restarted" >> $LOG_FILE
else
    echo "[$TIMESTAMP] ✅ Auto-push running (PID: $PUSH_PID)" >> $LOG_FILE
fi

# Log current stats
if [ -n "$DB_FILE" ]; then
    UPDATE_COUNT=$(source venv/bin/activate && python -c "
import sqlite3
try:
    conn = sqlite3.connect('$DB_FILE')
    cursor = conn.execute('SELECT COUNT(*) FROM price_updates')
    print(cursor.fetchone()[0])
except:
    print(0
" 2>/dev/null)
    echo "[$TIMESTAMP] 📊 Stats: $UPDATE_COUNT updates in $DB_FILE" >> $LOG_FILE
fi
