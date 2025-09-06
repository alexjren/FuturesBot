import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import sqlite3

from config import DB_PATH, INIT_ACCOUNT_VALUE, TICKERS, TIMEFRAMES

def build_series(df: pd.DataFrame) -> pd.Series:
    if df.empty or "timestamp" not in df.columns or "account_value" not in df.columns:
        return pd.Series(dtype=float)

    dates = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
    values = pd.to_numeric(df["account_value"], errors="coerce")
    
    s = pd.Series(dict(zip(dates,values)), index=dates).sort_index()

    s = s[~s.index.duplicated(keep="last")]

    s = s.replace([np.inf, -np.inf], np.nan).dropna()

    return s

def plot_account_value(db_path: str = DB_PATH):
    connection = sqlite3.connect(db_path)
    cursor = connection.cursor()

    for timeframe in TIMEFRAMES:
        fig, ax = plt.subplots(layout='constrained')

        for ticker in TICKERS:
            data = cursor.execute("SELECT timestamp, account_value FROM signals WHERE ticker = ? AND timeframe = ? ORDER BY timestamp ASC", (ticker, timeframe)).fetchall()
            df = pd.DataFrame(data, columns=['timestamp', 'account_value'])

            try:
                plot_df = build_series(df)
            except Exception:
                #logger.error(f"{ticker}: error in build_series", exc_info=True)
                continue
            
            ax.plot(plot_df.index, plot_df.values, label=ticker)

        ax.set_title("Account Equity by Ticker")
        ax.set_xlabel("Date")
        ax.set_ylabel("Account Value")
        ax.set_yscale("log")
        
        def dollar2percent(x):
            return (x / INIT_ACCOUNT_VALUE - 1) * 100
        
        def percent2dollar(x): 
            return (x / 100 + 1)* INIT_ACCOUNT_VALUE

        secax = ax.secondary_yaxis("right", functions=(dollar2percent, percent2dollar))
        secax.set_ylabel("Percent Gain/Loss")
        y_min, y_max = ax.get_ylim()
        secax.set_ylim(dollar2percent(y_min), dollar2percent(y_max))
        secax.set_yscale("linear")

        ax.legend()
        fig.savefig(f"account_value_{timeframe}.png", dpi=300, bbox_inches="tight")
        plt.clf()
    
    connection.commit()
    connection.close()
    
if __name__ == "__main__":
    plot_account_value()
