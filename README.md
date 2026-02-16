# Polymarket Data Collector

Continuous high-frequency data collection for Polymarket BTC 5-minute markets.

## Features

- **Millisecond precision**: Captures every price change at 10-100Hz
- **Dual collection**: WebSocket + REST polling for maximum coverage
- **Efficient storage**: Custom binary format (~24 bytes per update)
- **Duplicate filtering**: Only stores price changes, not redundant data
- **Backtest-ready**: Easy export to pandas/NumPy

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

## Storage Formats

| Format | Size per Update | Use Case |
|--------|-----------------|----------|
| Binary (custom) | 24 bytes | Long-term storage |
| HDF5 | ~40 bytes | Analysis with pandas |
| SQLite | ~80 bytes | Querying with SQL |

## Quick Start

```bash
# Install dependencies
pip install -r requirements.txt

# Run high-frequency collector
python collector_hf.py --interval 100

# In another terminal: monitor stats
python export.py --stats
```

## Data Schema

Each price update contains:
- `timestamp_ms`: Millisecond-precision timestamp
- `bid/ask/mid`: Orderbook prices
- `spread_bps`: Spread in basis points
- `bid_depth/ask_depth`: Liquidity at best level
- `source`: 'websocket' or 'rest'

## Storage Size Estimate

At 100Hz (100ms polling):
- ~600 updates per minute per side (up/down)
- ~1,200 updates per minute total
- ~72,000 updates per hour
- ~1.7 MB per hour (binary format)
- ~40 MB per day
- ~1.2 GB per month

## GitHub Actions

The `.github/workflows/collect.yml` runs continuous collection every 5 minutes.

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
