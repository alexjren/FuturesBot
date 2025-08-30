import sqlite3
import logging

import pandas as pd
import time
from dataclasses import asdict
from datetime import datetime, timedelta
from strategy import calculate_signals
from polygon import RESTClient
from config import API_KEY, TICKERS, DB_PATH, ENTRY_PERIOD, EXIT_PERIOD

BARS_TABLE = """CREATE TABLE IF NOT EXISTS
bars(
    ticker          TEXT NOT NULL,
    timeframe       TEXT NOT NULL,
    timestamp       INTEGER NOT NULL,
    open            REAL,
    high            REAL,
    low             REAL,
    close           REAL,
    volume          INTEGER,
    vwap            REAL,
    transactions    INTEGER,
    PRIMARY KEY (ticker, timeframe, timestamp)
)
"""

PROCESS_TABLE = """CREATE TABLE IF NOT EXISTS
process(
    ticker          TEXT NOT NULL,
    timeframe       TEXT NOT NULL,
    timestamp        INTEGER NOT NULL,
    high_entry      REAL,
    low_entry       REAL,
    high_exit       REAL,
    low_exit        REAL,
    prev_high       REAL,
    prev_low        REAL,
    bars_since_high INTEGER,
    bars_since_low  INTEGER,
    PRIMARY KEY (ticker, timeframe, timestamp)
    FOREIGN KEY (ticker, timeframe, timestamp) REFERENCES bars(ticker, timeframe, timestamp)
)"""

SIGNALS_TABLE = """CREATE TABLE IF NOT EXISTS
signals(
    ticker          TEXT NOT NULL,
    timeframe       TEXT NOT NULL,
    timestamp        INTEGER NOT NULL,
    signal          TEXT,
    position        TEXT,
    entry_price     REAL,
    stop_price      REAL,
    target_price    REAL,
    position_basis  REAL,
    unit_size       INTEGER,
    account_value   REAL,
    PRIMARY KEY (ticker, timeframe, timestamp)
    FOREIGN KEY (ticker, timeframe, timestamp) REFERENCES bars(ticker, timeframe, timestamp)
)"""

def update_database(*, multiplier: int = 1, timespan: str = 'day'):
    client = RESTClient(API_KEY)

    connection = sqlite3.connect(DB_PATH)
    cursor = connection.cursor()

    cursor.execute(BARS_TABLE)

    timeframe = f"{multiplier}{timespan[0].upper()}"

    end_ts = int(datetime.now().timestamp()*1000)
    logger.info(f"Fetching timeframe: {timeframe}")

    for ticker in TICKERS:
        try:
            last_ts = cursor.execute(f"SELECT MAX(timestamp) FROM bars WHERE ticker = '{ticker}' AND timeframe = '{timeframe}'").fetchone()[0]
            start_ts = last_ts
        except:
            start_ts = int((datetime.now()-timedelta(days=730)).timestamp()*1000)

        logger.info(f"{ticker}: range ({datetime.utcfromtimestamp(start_ts/1000)}, {datetime.utcfromtimestamp(end_ts/1000)})")
        
        count = 0
        try:
            for a in client.list_aggs(ticker, multiplier, timespan, start_ts, end_ts, sort = "asc", limit = 50000):
                ad = asdict(a)
                timestamp = ad['timestamp']
                open = ad['open']
                high = ad['high']
                low = ad['low']
                close = ad['close']
                volume = ad['volume']
                vwap = ad['vwap']
                transactions = ad['transactions']

                cursor.execute("REPLACE INTO bars VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", (ticker, timeframe, timestamp, open, high, low, close, volume, vwap, transactions))
                count+=1
        except Exception as e:
            logger.warning(f"{ticker}: API error '{e}'. Retrying in 1 minute.")
            time.sleep(60)
        logger.info(f"{ticker}: {count} bars updated.")
    
    connection.commit()
    connection.close()
    logger.info("Database updated.")

def process_data(*, multiplier: int = 1, timespan: str = 'day'):
    connection =  sqlite3.connect(DB_PATH)
    cursor = connection.cursor()

    cursor.execute(PROCESS_TABLE)

    timeframe = f"{multiplier}{timespan[0].upper()}"
    logger.info(f"Processing timeframe: {timeframe}")
    
    for ticker in TICKERS:
        count = 0
        cursor.execute("SELECT * FROM bars WHERE ticker = ? AND timeframe = ? ORDER BY timestamp ASC", (ticker, timeframe))
        bars = cursor.fetchall()
        df = pd.DataFrame(bars, columns=['ticker', 'timeframe', 'timestamp', 'open', 'high', 'low', 'close', 'volume', 'vwap', 'transactions'])
        
        df["high_entry"] = df["high"].rolling(ENTRY_PERIOD, min_periods=ENTRY_PERIOD).max()
        df["low_entry"]  = df["low"].rolling(ENTRY_PERIOD,  min_periods=ENTRY_PERIOD).min()

        df["high_exit"] = df["high"].rolling(EXIT_PERIOD, min_periods=EXIT_PERIOD).max()
        df["low_exit"]  = df["low"].rolling(EXIT_PERIOD,  min_periods=EXIT_PERIOD).min()

        df["prev_high"] = df["high"].rolling(ENTRY_PERIOD, min_periods=ENTRY_PERIOD).max().shift(1)
        df["prev_low"]  = df["low"].rolling(ENTRY_PERIOD,  min_periods=ENTRY_PERIOD).min().shift(1)

        df["new_high"] = df["high"] > df["prev_high"]
        df["new_low"]  = df["low"]  < df["prev_low"]

        nh = df["new_high"].fillna(False).astype(bool).to_numpy()
        nl = df["new_low" ].fillna(False).astype(bool).to_numpy()

        dsh, last_hi = [None] * len(df), None
        for i, brk in enumerate(nh):
            if brk:
                dsh[i] = 0 if last_hi is None else (i - last_hi)
                last_hi = i
            else:
                dsh[i] = None if last_hi is None else (i - last_hi)
        df["bars_since_high"] = dsh

        dsl, last_lo = [None] * len(df), None
        for i, brk in enumerate(nl):
            if brk:
                dsl[i] = 0 if last_lo is None else (i - last_lo)
                last_lo = i
            else:
                dsl[i] = None if last_lo is None else (i - last_lo)
        df["bars_since_low"] = dsl

        for i, row in df.iterrows():
            cursor.execute("""  REPLACE INTO process 
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""", 
                                (row["ticker"], row["timeframe"], row["timestamp"], 
                                row["high_entry"], row["low_entry"], row["high_exit"], 
                                row["low_exit"], row["prev_high"], row["prev_low"], 
                                row["bars_since_high"], row["bars_since_low"])
                           )
            count += 1
        logger.info(f"{ticker}: {count} rows processed.")
    
    connection.commit()
    connection.close()
    logger.info("Data processed.")

def update_signals(*, multiplier: int = 1, timespan: str = 'day', update_all: bool = False):
    connection =  sqlite3.connect(DB_PATH)
    cursor = connection.cursor()

    cursor.execute(SIGNALS_TABLE)

    timeframe = f"{multiplier}{timespan[0].upper()}"
    logger.info(f"Processing timeframe: {timeframe}")  

    for ticker in TICKERS:
        count = 0
        cursor.execute("SELECT ticker, timeframe, timestamp, open, high, low, close FROM bars WHERE ticker = ? AND timeframe = ? ORDER BY timestamp ASC", (ticker, timeframe))
        bars = cursor.fetchall()

        cursor.execute("SELECT * FROM process WHERE ticker = ? AND timeframe = ? ORDER BY timestamp ASC", (ticker, timeframe))
        process = cursor.fetchall()

        df = pd.DataFrame(bars, columns=['ticker', 'timeframe', 'timestamp', 'open', 'high', 'low', 'close'])
        df2 = pd.DataFrame(process, columns=['ticker', 'timeframe', 'timestamp', 'high_entry', 'low_entry', 'high_exit', 'low_exit', 'prev_high', 'prev_low', 'bars_since_high', 'bars_since_low'])
        df_combined = pd.merge(
            df,
            df2,
            on=["ticker", "timeframe", "timestamp"],   
            how="inner"                                
        )

        try:
            sig_df = calculate_signals(df_combined, ticker)
        except Exception:
            logger.error(f"{ticker}: error in calculate_signals", exc_info=True)
            continue

        for i, row in sig_df.iterrows():
            cursor.execute("""  REPLACE INTO signals 
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""", 
                                (row["ticker"], row["timeframe"], row["timestamp"], 
                                row["signal"], row["position"], row["entry_price"], 
                                row["stop_price"], row["target_price"], row["position_basis"], 
                                row["unit_size"], row["account_value"])  
                           )
            count += 1
        logger.info(f"{ticker}: {count} signals calculated.")
        
    connection.commit()
    connection.close()
    logger.info("Signals calculated.")

def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[logging.FileHandler("data_handler.log"), logging.StreamHandler()],
    )

    global logger
    logger = logging.getLogger(__name__)

def refresh_data(*, multiplier: int = 1, timespan: str = 'day'):
    setup_logging()
    update_database(multiplier=multiplier, timespan=timespan)
    process_data(multiplier=multiplier, timespan=timespan)
    update_signals(multiplier=multiplier, timespan=timespan)

if __name__ == "__main__":
    refresh_data()