#!/usr/bin/env python3
"""
Export SQLite data to Parquet for analysis and Git storage.
"""

import argparse
import sqlite3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime
from pathlib import Path


def export_monthly(db_path: str, output_dir: str, year: int = None, month: int = None):
    """Export data to monthly Parquet files."""
    
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    
    # Get date range if not specified
    if year is None or month is None:
        cursor = conn.execute("""
            SELECT 
                MIN(datetime(market_ts, 'unixepoch')),
                MAX(datetime(market_ts, 'unixepoch'))
            FROM orderbook_snapshots
        """)
        row = cursor.fetchone()
        if not row or not row[0]:
            print("No data to export")
            return
        
        min_date = datetime.fromisoformat(row[0].replace(' ', 'T'))
        max_date = datetime.fromisoformat(row[1].replace(' ', 'T'))
    else:
        min_date = datetime(year, month, 1)
        max_date = datetime(year + (month // 12), ((month % 12) + 1), 1)
    
    print(f"Exporting data from {min_date.date()} to {max_date.date()}")
    
    # Export by month
    current = datetime(min_date.year, min_date.month, 1)
    while current <= max_date:
        y, m = current.year, current.month
        
        start_ts = int(current.timestamp())
        if m == 12:
            end_ts = int(datetime(y + 1, 1, 1).timestamp())
        else:
            end_ts = int(datetime(y, m + 1, 1).timestamp())
        
        # Query snapshots
        df = pd.read_sql("""
            SELECT 
                s.market_ts,
                datetime(s.market_ts, 'unixepoch') as market_time,
                s.asset,
                datetime(s.recorded_at/1000, 'unixepoch') as recorded_time,
                s.best_bid / 1000000.0 as best_bid,
                s.best_ask / 1000000.0 as best_ask,
                s.mid_price / 1000000.0 as mid_price,
                s.bid_depth,
                s.ask_depth,
                s.spread_bps,
                m.outcome
            FROM orderbook_snapshots s
            LEFT JOIN markets m ON s.market_ts = m.timestamp AND s.asset = m.asset
            WHERE s.market_ts >= ? AND s.market_ts < ?
            ORDER BY s.market_ts, s.recorded_at
        """, conn, params=(start_ts, end_ts))
        
        if len(df) == 0:
            current = datetime(y + (m // 12), ((m % 12) + 1), 1)
            continue
        
        # Convert types
        df['market_time'] = pd.to_datetime(df['market_time'])
        df['recorded_time'] = pd.to_datetime(df['recorded_time'])
        
        # Partition by asset for smaller files
        for asset in df['asset'].unique():
            df_asset = df[df['asset'] == asset].copy()
            
            output_file = Path(output_dir) / f"{asset}_5min_{y}-{m:02d}.parquet"
            
            table = pa.Table.from_pandas(df_asset.drop(columns=['asset']))
            pq.write_table(table, output_file, compression='zstd')
            
            file_size = output_file.stat().st_size / (1024 * 1024)
            print(f"  {asset} {y}-{m:02d}: {len(df_asset):,} rows -> {file_size:.2f} MB")
        
        current = datetime(y + (m // 12), ((m % 12) + 1), 1)
    
    conn.close()
    print("\nExport complete!")


def export_resolutions(db_path: str, output_dir: str):
    """Export market resolutions."""
    
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    
    df = pd.read_sql("""
        SELECT 
            timestamp as market_ts,
            asset,
            datetime(timestamp, 'unixepoch') as market_time,
            outcome,
            datetime(resolved_at, 'unixepoch') as resolved_time
        FROM markets
        WHERE outcome IS NOT NULL
        ORDER BY timestamp
    """, conn)
    
    if len(df) > 0:
        df['market_time'] = pd.to_datetime(df['market_time'])
        df['resolved_time'] = pd.to_datetime(df['resolved_time'])
        
        output_file = Path(output_dir) / "market_resolutions.parquet"
        df.to_parquet(output_file, compression='zstd', index=False)
        print(f"Exported {len(df):,} resolutions to {output_file}")
    
    conn.close()


def show_stats(db_path: str):
    """Show database statistics."""
    conn = sqlite3.connect(db_path)
    
    print("=== Database Statistics ===\n")
    
    # Overall counts
    cursor = conn.execute("SELECT COUNT(*) FROM markets")
    total_markets = cursor.fetchone()[0]
    
    cursor = conn.execute("SELECT COUNT(*) FROM markets WHERE outcome IS NOT NULL")
    resolved = cursor.fetchone()[0]
    
    cursor = conn.execute("SELECT COUNT(*) FROM orderbook_snapshots")
    snapshots = cursor.fetchone()[0]
    
    print(f"Total markets: {total_markets:,}")
    print(f"Resolved: {resolved:,} ({resolved/max(1,total_markets)*100:.1f}%)")
    print(f"Snapshots: {snapshots:,}")
    
    # By asset
    print("\nBy Asset:")
    cursor = conn.execute('''
        SELECT asset, 
               COUNT(*) as total,
               SUM(CASE WHEN outcome IS NOT NULL THEN 1 ELSE 0 END) as resolved
        FROM markets 
        GROUP BY asset
    ''')
    for row in cursor.fetchall():
        print(f"  {row[0]}: {row[2]:,}/{row[1]:,} resolved")
    
    # File size
    db_size = Path(db_path).stat().st_size / (1024 * 1024)
    print(f"\nDatabase size: {db_size:.2f} MB")
    
    # Sample recent data
    print("\n=== Recent Snapshots ===")
    df = pd.read_sql("""
        SELECT 
            datetime(market_ts, 'unixepoch') as time,
            asset,
            best_bid / 1000000.0 as bid,
            best_ask / 1000000.0 as ask,
            spread_bps
        FROM orderbook_snapshots 
        ORDER BY recorded_at DESC 
        LIMIT 10
    """, conn)
    print(df.to_string(index=False))
    
    conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", default="data/raw/polymarket_data.db")
    parser.add_argument("--output", default="data/parquet")
    parser.add_argument("--month", help="Export specific month (YYYY-MM)")
    parser.add_argument("--resolutions", action="store_true", help="Export resolutions only")
    parser.add_argument("--stats", action="store_true", help="Show statistics")
    args = parser.parse_args()
    
    if args.stats:
        show_stats(args.db)
    elif args.resolutions:
        export_resolutions(args.db, args.output)
    elif args.month:
        year, month = map(int, args.month.split('-'))
        export_monthly(args.db, args.output, year, month)
    else:
        # Export all
        export_monthly(args.db, args.output)
        export_resolutions(args.db, args.output)
