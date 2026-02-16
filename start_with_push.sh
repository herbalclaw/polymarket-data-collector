#!/bin/bash
# Start high-frequency collector with auto-push to GitHub

cd /root/.openclaw/workspace/polymarket-data-collector

# Start collector in background
source venv/bin/activate
python collector_hf.py --interval 100 &
COLLECTOR_PID=$!

echo "Collector started (PID: $COLLECTOR_PID)"
echo "Data saving to: data/raw/btc_hf_data.db"
echo "Auto-pushing to GitHub every 5 minutes..."

# Auto-push loop
while true; do
    sleep 300  # 5 minutes
    
    # Check if collector is still running
    if ! kill -0 $COLLECTOR_PID 2>/dev/null; then
        echo "Collector stopped. Exiting."
        exit 1
    fi
    
    # Commit and push data
    git add data/raw/*.db
    git diff --cached --quiet || {
        TIMESTAMP=$(date -u +"%Y-%m-%d-%H:%M:%S")
        git commit -m "Data update: $TIMESTAMP UTC"
        git push origin master
        echo "Pushed at $TIMESTAMP"
    }
done
