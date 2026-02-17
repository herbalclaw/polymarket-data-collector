#!/bin/bash
# Auto-push script for Polymarket data collector
# Pushes data to GitHub every 5 minutes

while true; do
    sleep 300  # 5 minutes
    
    cd /root/.openclaw/workspace/polymarket-data-collector
    
    # Find the most recent database file
    DB_FILE=$(ls -t data/raw/btc_hf_*.db 2>/dev/null | head -1)
    
    if [ -n "$DB_FILE" ]; then
        # Force SQLite to checkpoint (flush WAL to main database)
        python3 -c "
import sqlite3
try:
    conn = sqlite3.connect('$DB_FILE')
    conn.execute('PRAGMA wal_checkpoint;')
    conn.commit()
    conn.close()
except Exception as e:
    print(f'Checkpoint error: {e}')
"
        
        # Add the database file
        git add -f "$DB_FILE"
        
        # Check if there are changes to commit
        if ! git diff --cached --quiet; then
            TIMESTAMP=$(date -u +"%Y-%m-%d-%H:%M:%S")
            ROW_COUNT=$(python3 -c "
import sqlite3
try:
    conn = sqlite3.connect('$DB_FILE')
    cursor = conn.cursor()
    cursor.execute('SELECT COUNT(*) FROM price_updates')
    count = cursor.fetchone()[0]
    conn.close()
    print(count)
except:
    print('0')
")
            git commit -m "Data: $TIMESTAMP UTC | $ROW_COUNT updates | $DB_FILE" || true
            git push origin master || echo "Push failed, will retry"
        fi
    fi
done
