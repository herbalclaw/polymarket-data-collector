#!/usr/bin/env python3
"""
Backtesting utilities for Polymarket data.
"""

import pandas as pd
import duckdb
from pathlib import Path
from typing import Optional, Callable
from dataclasses import dataclass


@dataclass
class BacktestResult:
    """Results from a backtest run."""
    total_trades: int
    wins: int
    losses: int
    win_rate: float
    total_pnl: float
    avg_pnl: float
    max_drawdown: float
    trades: pd.DataFrame


def load_data(
    asset: str,
    parquet_dir: str = "data/parquet",
    start: Optional[str] = None,
    end: Optional[str] = None
) -> pd.DataFrame:
    """Load data for an asset from Parquet files."""
    
    con = duckdb.connect()
    
    # Find parquet files for this asset
    files = list(Path(parquet_dir).glob(f"{asset}_5min_*.parquet"))
    if not files:
        raise FileNotFoundError(f"No data files found for {asset} in {parquet_dir}")
    
    # Load and filter
    files_str = ", ".join([f"'{f}'" for f in files])
    
    query = f"""
        SELECT * FROM read_parquet([{files_str}])
        WHERE 1=1
    """
    
    if start:
        query += f" AND market_time >= '{start}'"
    if end:
        query += f" AND market_time <= '{end}'"
    
    query += " ORDER BY market_time, recorded_time"
    
    df = con.execute(query).fetchdf()
    con.close()
    
    return df


def get_market_outcomes(
    asset: str,
    parquet_dir: str = "data/parquet"
) -> pd.DataFrame:
    """Get market resolutions for an asset."""
    
    resolutions_file = Path(parquet_dir) / "market_resolutions.parquet"
    if not resolutions_file.exists():
        raise FileNotFoundError(f"Resolutions file not found: {resolutions_file}")
    
    df = pd.read_parquet(resolutions_file)
    return df[df['asset'] == asset].sort_values('market_time')


def streak_strategy(
    outcomes: pd.DataFrame,
    trigger: int = 4,
    bet_amount: float = 5.0,
    fee_pct: float = 0.025
) -> BacktestResult:
    """
    Backtest streak reversal strategy.
    
    Args:
        outcomes: DataFrame with 'market_time' and 'outcome' columns
        trigger: Streak length to trigger bet
        bet_amount: Bet size in USD
        fee_pct: Fee percentage (e.g., 0.025 = 2.5%)
    """
    
    outcomes = outcomes.copy()
    outcomes['outcome_num'] = (outcomes['outcome'] == 'up').astype(int)
    
    trades = []
    bankroll = 1000.0
    peak = bankroll
    max_drawdown = 0.0
    
    for i in range(trigger, len(outcomes)):
        # Check for streak
        window = outcomes.iloc[i-trigger:i]
        if len(window['outcome_num'].unique()) != 1:
            continue
        
        streak_dir = 'up' if window.iloc[0]['outcome_num'] == 1 else 'down'
        bet_dir = 'down' if streak_dir == 'up' else 'up'
        actual = outcomes.iloc[i]['outcome']
        
        won = bet_dir == actual
        
        if won:
            pnl = bet_amount * (1 - fee_pct)
        else:
            pnl = -bet_amount
        
        bankroll += pnl
        peak = max(peak, bankroll)
        drawdown = peak - bankroll
        max_drawdown = max(max_drawdown, drawdown)
        
        trades.append({
            'time': outcomes.iloc[i]['market_time'],
            'streak': f"{trigger}x{streak_dir}",
            'bet': bet_dir,
            'actual': actual,
            'won': won,
            'pnl': pnl,
            'bankroll': bankroll
        })
    
    if not trades:
        return BacktestResult(0, 0, 0, 0.0, 0.0, 0.0, 0.0, pd.DataFrame())
    
    df_trades = pd.DataFrame(trades)
    wins = df_trades['won'].sum()
    losses = len(df_trades) - wins
    
    return BacktestResult(
        total_trades=len(df_trades),
        wins=wins,
        losses=losses,
        win_rate=wins / len(df_trades),
        total_pnl=df_trades['pnl'].sum(),
        avg_pnl=df_trades['pnl'].mean(),
        max_drawdown=max_drawdown,
        trades=df_trades
    )


def print_backtest_result(result: BacktestResult):
    """Pretty print backtest results."""
    
    print("\n" + "="*50)
    print("BACKTEST RESULTS")
    print("="*50)
    print(f"Total Trades: {result.total_trades}")
    print(f"Wins: {result.wins} ({result.win_rate:.1%})")
    print(f"Losses: {result.losses} ({1-result.win_rate:.1%})")
    print(f"Total PnL: ${result.total_pnl:+.2f}")
    print(f"Avg PnL/Trade: ${result.avg_pnl:+.2f}")
    print(f"Max Drawdown: ${result.max_drawdown:.2f}")
    print("="*50)
    
    if result.total_trades > 0:
        print("\nLast 10 Trades:")
        print(result.trades.tail(10).to_string(index=False))


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser()
    parser.add_argument("--asset", default="BTC", choices=["BTC", "ETH", "SOL"])
    parser.add_argument("--trigger", type=int, default=4)
    parser.add_argument("--start", help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", help="End date (YYYY-MM-DD)")
    parser.add_argument("--data-dir", default="data/parquet")
    args = parser.parse_args()
    
    print(f"Loading {args.asset} data...")
    
    # Load outcomes
    outcomes = get_market_outcomes(args.asset, args.data_dir)
    
    if args.start:
        outcomes = outcomes[outcomes['market_time'] >= args.start]
    if args.end:
        outcomes = outcomes[outcomes['market_time'] <= args.end]
    
    print(f"Found {len(outcomes)} resolved markets")
    
    # Run backtest
    result = streak_strategy(outcomes, trigger=args.trigger)
    print_backtest_result(result)
