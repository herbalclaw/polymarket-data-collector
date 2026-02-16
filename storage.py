#!/usr/bin/env python3
"""
Time-series optimized storage for high-frequency price data.

Uses a custom binary format for minimal storage + fast reads.
"""

import struct
import numpy as np
from pathlib import Path
from datetime import datetime
from typing import List, Tuple, Optional
import gzip


class TimeSeriesStore:
    """
    Binary storage format optimized for millisecond price data.
    
    Format:
    - Header: 16 bytes (magic, version, start_time, record_count)
    - Records: 24 bytes each (timestamp_ms, bid, ask, mid, spread_bps)
      All prices stored as uint32 (scaled by 1e6)
    """
    
    HEADER_FMT = '<4sIQQ'  # magic(4), version(4), start_time(8), count(8)
    HEADER_SIZE = struct.calcsize(HEADER_FMT)
    
    RECORD_FMT = '<QIIII'  # timestamp_ms(8), bid(4), ask(4), mid(4), spread(4)
    RECORD_SIZE = struct.calcsize(RECORD_FMT)
    
    MAGIC = b'PMHF'  # Polymarket High Frequency
    VERSION = 1
    
    def __init__(self, base_dir: str = "data/timeseries"):
        self.base_dir = Path(base_dir)
        self.base_dir.mkdir(parents=True, exist_ok=True)
    
    def _get_file_path(self, asset: str, market_ts: int) -> Path:
        """Generate file path for a market."""
        dt = datetime.utcfromtimestamp(market_ts)
        return self.base_dir / asset / f"{dt:%Y/%m/%d}/{market_ts}.bin.gz"
    
    def write_market_data(self, asset: str, market_ts: int, 
                          timestamps: List[int], bids: List[float], 
                          asks: List[float], mids: List[float],
                          spreads: List[int]):
        """
        Write price data for a single 5-min market.
        
        Args:
            asset: Asset symbol (e.g., 'BTC')
            market_ts: Market start timestamp
            timestamps: List of millisecond timestamps
            bids: List of bid prices
            asks: List of ask prices  
            mids: List of mid prices
            spreads: List of spreads in basis points
        """
        file_path = self._get_file_path(asset, market_ts)
        file_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Scale prices to uint32
        bids_scaled = [int(b * 1_000_000) for b in bids]
        asks_scaled = [int(a * 1_000_000) for a in asks]
        mids_scaled = [int(m * 1_000_000) for m in mids]
        
        # Build header
        header = struct.pack(
            self.HEADER_FMT,
            self.MAGIC,
            self.VERSION,
            market_ts,
            len(timestamps)
        )
        
        # Build records
        records = b''
        for ts, bid, ask, mid, spread in zip(timestamps, bids_scaled, 
                                              asks_scaled, mids_scaled, spreads):
            records += struct.pack(self.RECORD_FMT, ts, bid, ask, mid, spread)
        
        # Compress and write
        with gzip.open(file_path, 'wb') as f:
            f.write(header)
            f.write(records)
    
    def read_market_data(self, asset: str, market_ts: int) -> Optional[Tuple]:
        """
        Read price data for a market.
        
        Returns:
            Tuple of (timestamps, bids, asks, mids, spreads) or None
        """
        file_path = self._get_file_path(asset, market_ts)
        
        if not file_path.exists():
            return None
        
        with gzip.open(file_path, 'rb') as f:
            # Read header
            header = f.read(self.HEADER_SIZE)
            magic, version, start_time, count = struct.unpack(self.HEADER_FMT, header)
            
            if magic != self.MAGIC:
                raise ValueError("Invalid file format")
            
            # Read records
            timestamps = []
            bids = []
            asks = []
            mids = []
            spreads = []
            
            for _ in range(count):
                record = f.read(self.RECORD_SIZE)
                ts, bid, ask, mid, spread = struct.unpack(self.RECORD_FMT, record)
                
                timestamps.append(ts)
                bids.append(bid / 1_000_000)
                asks.append(ask / 1_000_000)
                mids.append(mid / 1_000_000)
                spreads.append(spread)
        
        return timestamps, bids, asks, mids, spreads
    
    def get_storage_stats(self) -> dict:
        """Get storage statistics."""
        total_files = 0
        total_bytes = 0
        total_records = 0
        
        for file_path in self.base_dir.rglob("*.bin.gz"):
            total_files += 1
            total_bytes += file_path.stat().st_size
            
            # Count records (approximate from file size)
            # Actual count would require reading headers
            approx_records = (file_path.stat().st_size * 2) // self.RECORD_SIZE
            total_records += approx_records
        
        return {
            'files': total_files,
            'bytes': total_bytes,
            'mb': round(total_bytes / (1024 * 1024), 2),
            'approx_records': total_records,
            'bytes_per_record': total_bytes / max(1, total_records)
        }


class HDF5Store:
    """
    Alternative: HDF5 storage using PyTables (better for analysis).
    
    Requires: pip install tables
    """
    
    def __init__(self, file_path: str = "data/btc_prices.h5"):
        self.file_path = file_path
        Path(file_path).parent.mkdir(parents=True, exist_ok=True)
    
    def append_prices(self, timestamps: List[int], bids: List[float],
                      asks: List[float], mids: List[float]):
        """Append price data to HDF5 file."""
        import pandas as pd
        
        df = pd.DataFrame({
            'timestamp_ms': timestamps,
            'bid': bids,
            'ask': asks,
            'mid': mids
        })
        
        # Append to HDF5 with compression
        df.to_hdf(
            self.file_path,
            key='prices',
            mode='a',
            format='table',
            complevel=5,
            complib='zlib',
            append=True
        )
    
    def read_range(self, start_ms: int, end_ms: int) -> 'pd.DataFrame':
        """Read price data for a time range."""
        import pandas as pd
        
        store = pd.HDFStore(self.file_path, mode='r')
        df = store.select('prices', 
                         where=f'timestamp_ms >= {start_ms} and timestamp_ms <= {end_ms}')
        store.close()
        return df


# Size comparison
if __name__ == "__main__":
    # Generate sample data
    n = 10000  # 10k updates (about 1 minute at 100Hz)
    timestamps = list(range(1700000000000, 1700000000000 + n * 100, 100))
    bids = [0.50 + i * 0.0001 for i in range(n)]
    asks = [b + 0.01 for b in bids]
    mids = [(b + a) / 2 for b, a in zip(bids, asks)]
    spreads = [100] * n
    
    # Test binary store
    store = TimeSeriesStore()
    store.write_market_data('BTC', 1700000000, timestamps, bids, asks, mids, spreads)
    
    stats = store.get_storage_stats()
    print(f"Binary store: {stats['mb']} MB for {n} records")
    print(f"Bytes per record: {stats['bytes_per_record']:.2f}")
    
    # Read back
    data = store.read_market_data('BTC', 1700000000)
    if data:
        print(f"Read back {len(data[0])} records successfully")
