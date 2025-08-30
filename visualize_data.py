import argparse
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import sqlite3

from config import DB_PATH, INIT_ACCOUNT_VALUE, TICKERS

def build_series(df: pd.DataFrame) -> pd.Series:
    if df.empty or "timestamp" not in df.columns or "account_value" not in df.columns:
        return pd.Series(dtype=float)

    dates = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
    values = pd.to_numeric(df["account_value"], errors="coerce")
    
    s = pd.Series(dict(zip(dates,values)), index=dates).sort_index()

    s = s[~s.index.duplicated(keep="last")]

    s = s.replace([np.inf, -np.inf], np.nan).dropna()

    return s

def plot_account_value(db_path: str = DB_PATH, *, multiplier: int = 1, timestep: str = "day"):
    parser = argparse.ArgumentParser(description="Plot per-ticker account value vs time with % on secondary axis.")
    parser.add_argument("--file", type=str, default=db_path, help="Path to JSON data file (default from config).")
    parser.add_argument("--tickers", type=str, nargs="*", default=None, help="Subset of tickers to plot (default: all in file or config.TICKERS).")
    parser.add_argument("--mode", type=str, choices=["all", "common", "from-date"], default="all",
                        help="Start mode: 'all' (each from own start), 'common' (earliest common), 'from-date' (use --date).")
    parser.add_argument("--date", type=str, default=None, help="YYYY-MM-DD (UTC) when --mode=from-date.")
    parser.add_argument("--title", type=str, default="Account Equity by Ticker", help="Plot title.")
    parser.add_argument("--out", type=str, default=None, help="Optional output PNG path to save the figure.")
    parser.add_argument("--show-percent-lines", action="store_true",
                        help="Also draw percent-change lines on right axis (one per ticker).")

    args = parser.parse_args()

    connection = sqlite3.connect(DB_PATH)
    cursor = connection.cursor()
    timeframe = f"{multiplier}{timestep[0].upper()}"

    fig, ax = plt.subplots(layout='constrained')

    if args.tickers:
        tickers = args.tickers
    else:
        tickers = TICKERS

    for ticker in tickers:
        data = cursor.execute("SELECT timestamp, account_value FROM signals WHERE ticker = ? AND timeframe = ? ORDER BY timestamp ASC", (ticker, timeframe)).fetchall()
        df = pd.DataFrame(data, columns=['timestamp', 'account_value'])

        try:
            plot_df = build_series(df)
        except Exception:
            #logger.error(f"{ticker}: error in build_series", exc_info=True)
            continue
        
        ax.plot(plot_df.index, plot_df.values, label=ticker)

    ax.set_title(args.title)
    ax.set_xlabel("Date")
    ax.set_ylabel("Account Value")
    
    def dollar2percent(x):
        return (x / INIT_ACCOUNT_VALUE - 1) * 100
    
    def percent2dollar(x): 
        return (x / 100 + 1)* INIT_ACCOUNT_VALUE

    secax = ax.secondary_yaxis("right", functions=(dollar2percent, percent2dollar))
    secax.set_ylabel("Percent Gain/Loss")

    ax.legend()
    fig.savefig("account_value.png", dpi=300, bbox_inches="tight")
    
    connection.commit()
    connection.close()
    
if __name__ == "__main__":
    plot_account_value()
