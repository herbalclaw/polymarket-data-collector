#!/bin/bash
# Auto-push script for Polymarket data collector
# Pushes code changes to GitHub every 5 minutes (data files are too large for git)

while true; do
    sleep 300  # 5 minutes
    
    cd /root/.openclaw/workspace/polymarket-data-collector
    
    # Add any code/config changes (not data files - they're in .gitignore)
    git add -A
    
    # Check if there are changes to commit
    if ! git diff --cached --quiet; then
        TIMESTAMP=$(date -u +"%Y-%m-%d-%H:%M:%S")
        git commit -m "Auto: $TIMESTAMP UTC | code/config updates" || true
        git push origin master || echo "Push failed, will retry"
    fi
done
