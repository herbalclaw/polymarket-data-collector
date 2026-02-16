# Polymarket Data Collector

Continuous data collection for Polymarket crypto prediction markets (BTC, ETH, SOL 5-minute).

## Features

- **Multi-asset support**: BTC, ETH, SOL 5-minute up/down markets
- **High-frequency snapshots**: Configurable interval (default: 5 seconds)
- **Efficient storage**: SQLite for collection, Parquet for analysis
- **Automatic resolution tracking**: Detects when markets resolve
- **Backtest-ready**: Easy export to pandas/duckdb

## Repository Structure

```
.
├── collector.py          # Main data collection daemon
├── export.py             # Export to Parquet
├── backtest.py           # Backtesting utilities
├── requirements.txt      # Python dependencies
├── .env.example          # Configuration template
├── data/
│   └── raw/              # SQLite databases (gitignored)
└── data/parquet/         # Exported Parquet files
```

## Quick Start

### 1. Install Dependencies

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 2. Configure

```bash
cp .env.example .env
# Edit .env with your settings (optional)
```

### 3. Start Collecting

```bash
python collector.py
```

Data will be saved to `data/raw/polymarket_data.db`

### 4. Export to Parquet (for analysis)

```bash
python export.py --month 2026-02
```

## Data Schema

### Orderbook Snapshots
- `market_ts`: Market start timestamp
- `asset`: BTC, ETH, or SOL
- `bid/ask/mid`: Prices (6 decimal precision)
- `bid_depth/ask_depth`: Liquidity at best level
- `spread_bps`: Spread in basis points

### Market Resolutions
- `timestamp`: Market start
- `asset`: BTC, ETH, or SOL
- `outcome`: up/down resolution
- `resolved_at`: When it resolved

## Backtesting

```python
from backtest import load_data

# Load all BTC data
df = load_data('BTC', start='2026-02-01', end='2026-02-15')

# Run your strategy
results = backtest_streak_strategy(df, trigger=4)
```

## Storage Size

| Asset | Snapshots/Day | Daily Size | Monthly Size |
|-------|---------------|------------|--------------|
| BTC   | ~2,880        | ~5 MB      | ~150 MB      |
| ETH   | ~2,880        | ~5 MB      | ~150 MB      |
| SOL   | ~2,880        | ~5 MB      | ~150 MB      |
| **Total** | **~8,640** | **~15 MB** | **~450 MB** |

Parquet export: ~90 MB/month (compressed)

## License

MIT
