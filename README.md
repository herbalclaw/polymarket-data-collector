# Polymarket BTC 5-Min Data Collector

Continuous high-frequency data collection for Polymarket **BTC 5-minute up/down markets**.

> **Note:** Currently only BTC has 5-minute prediction markets on Polymarket. ETH and SOL markets are not available.

## Features

- **Millisecond precision**: Captures every price change at 10-100Hz
- **Dual collection**: WebSocket + REST polling for maximum coverage
- **Efficient storage**: Optimized SQLite schema (~80 bytes per update)
- **Duplicate filtering**: Only stores price changes, not redundant data
- **Auto-push to GitHub**: Commits data every 5 minutes
- **Backtest-ready**: Easy export to pandas/NumPy

## Supported Markets

| Asset | Market Type | Status |
|-------|-------------|--------|
| BTC   | 5-min Up/Down | ✅ Active |
| ETH   | 5-min Up/Down | ❌ Not available on Polymarket |
| SOL   | 5-min Up/Down | ❌ Not available on Polymarket |

## Collection Modes

### High-Frequency Mode (Recommended)
```bash
python collector_hf.py --interval 100  # 100ms = 10Hz polling
```

Captures every meaningful price change in the orderbook.

### Standard Mode
```bash
python collector.py --interval 5  # 5 second snapshots
```

Lower frequency for basic analysis.

## Quick Start

### 1. Install Dependencies

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 2. Start Collecting (with auto-push)

```bash
# Start collector + auto-push to GitHub every 5 mins
./start_with_push.sh
```

Or manually:
```bash
# Start collector
python collector_hf.py --interval 100

# In another terminal: auto-push loop
while true; do
    sleep 300
    git add data/raw/btc_hf_data.db
    git commit -m "Data update: $(date -u +%Y-%m-%d-%H:%M:%S)"
    git push origin master
done
```

## Data Storage

**Location:** `data/raw/btc_hf_data.db`

**Tables:**
- `markets` - Market metadata (timestamp, token IDs, resolution)
- `price_updates` - Millisecond price data

**Schema (price_updates):**
| Column | Type | Description |
|--------|------|-------------|
| `timestamp_ms` | INTEGER | Millisecond-precision timestamp |
| `market_ts` | INTEGER | Market start timestamp (5-min window) |
| `asset` | TEXT | 'BTC' |
| `side` | TEXT | 'up' or 'down' |
| `bid` | INTEGER | Bid price (scaled by 1e6) |
| `ask` | INTEGER | Ask price (scaled by 1e6) |
| `mid` | INTEGER | Mid price (scaled by 1e6) |
| `spread_bps` | INTEGER | Spread in basis points |
| `bid_depth` | REAL | USD depth at best bid |
| `ask_depth` | REAL | USD depth at best ask |
| `source` | TEXT | 'rest' or 'websocket' |

## Storage Size Estimate

At **100Hz polling** (100ms intervals):

| Metric | Value |
|--------|-------|
| Updates/minute | ~1,200 (both up/down sides) |
| Updates/hour | ~72,000 |
| SQLite size | ~5 MB/hour |
| Daily size | ~120 MB/day |
| Monthly size | ~3.6 GB/month |

## Querying Data

```python
import sqlite3
import pandas as pd

conn = sqlite3.connect('data/raw/btc_hf_data.db')

# Get all price updates for current market
df = pd.read_sql('''
    SELECT 
        datetime(timestamp_ms/1000, 'unixepoch') as time,
        side,
        bid / 1000000.0 as bid,
        ask / 1000000.0 as ask,
        mid / 1000000.0 as mid
    FROM price_updates
    ORDER BY timestamp_ms DESC
    LIMIT 1000
''', conn)

# Get market outcomes
outcomes = pd.read_sql('''
    SELECT 
        datetime(timestamp, 'unixepoch') as market_time,
        outcome,
        datetime(resolved_at, 'unixepoch') as resolved_time
    FROM markets
    WHERE outcome IS NOT NULL
    ORDER BY timestamp DESC
''', conn)
```

## Backtesting

```python
from backtest import load_data, streak_strategy

# Load BTC data
df = load_data('BTC', start='2026-02-01', end='2026-02-15')

# Run streak reversal backtest
result = streak_strategy(df, trigger=4, bet_amount=5.0)
print(f"Win rate: {result.win_rate:.1%}, PnL: ${result.total_pnl:+.2f}")
```

## Export to Parquet

```bash
# Export monthly data for analysis
python export.py --month 2026-02

# Output: data/parquet/BTC_5min_2026-02.parquet
```

## Repository Structure

```
.
├── collector_hf.py       # High-frequency collector (100ms)
├── collector.py          # Standard collector (5s)
├── export.py             # Export to Parquet
├── backtest.py           # Backtesting utilities
├── storage.py            # Binary storage format
├── start_with_push.sh    # Start collector + auto-push
├── requirements.txt      # Python dependencies
├── .env.example          # Configuration template
└── data/
    └── raw/
        └── btc_hf_data.db    # SQLite database (gitignored)
```

## GitHub Auto-Push

The repository includes a script (`start_with_push.sh`) that:
1. Starts the high-frequency collector
2. Commits the database to GitHub every 5 minutes
3. Logs all activity

This ensures your data is backed up and versioned.

## License

MIT
