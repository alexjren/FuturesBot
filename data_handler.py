from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Dict, List, Any

import numpy as np
import pandas as pd
import time
from collections import deque
from datetime import datetime, timedelta
from dataclasses import asdict
from polygon import RESTClient

from config import API_KEY, TICKERS, FILE_PATH, ENTRY_PERIOD, EXIT_PERIOD
from strategy import calculate_signals

FEATURE_COLS: List[str] = ["high_entry", "low_entry", "high_exit", "low_exit","prev_high", "prev_low", "new_high", "new_low","bars_since_high", "bars_since_low",]

PERSIST_SIGNAL_COLS: List[str] = ["position", "signal", "entry_price", "stop_price","target_price", "position_basis", "unit_size", "account_value",]

REQUIRED_RAW_COLS: List[str] = ["timestamp", "high", "low"]

logger = logging.getLogger("data_handler")

def _load_json(path: str | Path) -> Dict[str, List[Dict[str, Any]]]:
    path = Path(path)
    if not path.exists() or path.stat().st_size == 0:
        logger.warning(f"No existing data file found at {path}. Creating a new one.")
        return {ticker: [] for ticker in TICKERS}
    try:
        with path.open("r") as f:
            return json.load(f)
    except json.JSONDecodeError:
        logger.error(f"Could not decode JSON from {path}. Starting with an empty dataset.", exc_info=True)
        return {ticker: [] for ticker in TICKERS}
    except Exception:
        logger.error(f"Failed to load data from {path}.", exc_info=True)
        return {ticker: [] for ticker in TICKERS}

def _dump_json(obj: Any, path: str | Path) -> None:
    path = Path(path)
    with path.open("w") as f:
        json.dump(obj, f, indent=4)

def _require_cols(df: pd.DataFrame, cols: List[str], ctx: str) -> None:
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise ValueError(f"{ctx}: missing required columns: {', '.join(missing)}")

def _prep_df(rows: List[Dict[str, Any]]) -> pd.DataFrame:
    df = pd.DataFrame(rows)
    _require_cols(df, REQUIRED_RAW_COLS, "prep_df")
    df = df.sort_values("timestamp").reset_index(drop=True)
    df["date"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
    df = df.set_index("date")
    return df

def _json_assign(row_dict: Dict[str, Any], key: str, v: Any) -> None:
    if pd.isna(v):
        row_dict[key] = None
    elif isinstance(v, (np.generic,)):
        row_dict[key] = v.item()
    else:
        row_dict[key] = v

def update_data(file_path: str = FILE_PATH,*,multiplier: int = 1,timespan: str = "day",max_per_min: int = 5, max_retries: int = 3) -> None:

    class RateLimiter:
        def __init__(self, max_calls: int, per_seconds: float):
            self.max_calls = max_calls
            self.per = per_seconds
            self._hits = deque()

        def wait(self):
            now = time.time()
            while self._hits and (now - self._hits[0]) > self.per:
                self._hits.popleft()
            if len(self._hits) < self.max_calls:
                self._hits.append(now)
                return
            sleep_for = self.per - (now - self._hits[0])
            if sleep_for > 0:
                time.sleep(sleep_for)
            self._hits.append(time.time())

    client = RESTClient(API_KEY)
    all_aggs_by_ticker = _load_json(file_path)

    end_time = int(time.time() * 1000)
    logger.info(f"Fetching timeframe: multiplier={multiplier}, timespan='{timespan}'")

    limiter = RateLimiter(max_per_min, 60.0)

    for ticker in TICKERS:
        all_aggs_by_ticker.setdefault(ticker, [])
        existing = all_aggs_by_ticker[ticker]
        existing_ts = {agg.get("timestamp") for agg in existing if "timestamp" in agg}

        if existing_ts:
            last_ts_ms = max(existing_ts)
            start_time = last_ts_ms
        else:
            start_time = int((datetime.now() - timedelta(days=730)).timestamp() * 1000)

        logger.info(f"Checking for new data for {ticker} from {datetime.utcfromtimestamp(start_time / 1000)} to {datetime.utcfromtimestamp(end_time / 1000)}...")

        new_aggs: List[Dict[str, Any]] = []

        tries = 0
        while True:
            tries += 1
            try:
                limiter.wait()
                for a in client.list_aggs(
                    ticker, multiplier, timespan, start_time, end_time, sort="asc", limit=50000
                ):
                    ad = asdict(a)
                    if ad.get("timestamp") not in existing_ts:
                        new_aggs.append(ad)
                break 
            except Exception as e:
                retry_after = None
                resp = getattr(e, "response", None)
                if resp is not None:
                    try:
                        retry_after = resp.headers.get("Retry-After")
                    except Exception:
                        retry_after = None

                if tries >= max_retries:
                    logger.error(f"{ticker}: API error after {tries} tries; giving up.", exc_info=True)
                    break

                if retry_after:
                    try:
                        wait_s = float(retry_after)
                    except ValueError:
                        wait_s = 15.0
                else:
                    wait_s = 60.0
                logger.warning(f"{ticker}: API error '{e}'. Retrying in {wait_s:.1f}s (attempt {tries}/{max_retries})")
                time.sleep(wait_s)

        if new_aggs:
            logger.info(f"Found {len(new_aggs)} new aggregate(s) for {ticker}.")
            existing.extend(new_aggs)
        else:
            logger.info(f"No new data for {ticker}.")

        try:
            if existing:
                before = len(existing)
                df_ex = pd.DataFrame(existing)
                if "timestamp" in df_ex.columns:
                    df_ex = (
                        df_ex.sort_values("timestamp")
                             .drop_duplicates(subset="timestamp", keep="last")
                    )
                    after = len(df_ex)
                    if after != before:
                        logger.info(f"{ticker}: removed {before - after} duplicate row(s).")
                    all_aggs_by_ticker[ticker] = df_ex.to_dict(orient="records")
                else:
                    logger.warning(f"{ticker}: cannot dedupe—missing 'timestamp' column.")
        except Exception:
            logger.error(f"{ticker}: error during deduplication step", exc_info=True)

    _dump_json(all_aggs_by_ticker, file_path)
    logger.info(f"Successfully updated and saved aggregates to {file_path}")


def process_data(file_path: str = FILE_PATH) -> None:
    all_aggs_by_ticker = _load_json(file_path)

    for ticker, rows in all_aggs_by_ticker.items():
        if not isinstance(rows, list) or not rows:
            continue

        try:
            df = _prep_df(rows)
        except Exception as e:
            logger.warning(f"{ticker}: prep error: {e}. Skipping.")
            continue

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

        persist_df = df[FEATURE_COLS + ["timestamp"]].set_index("timestamp")

        for r in rows:
            ts = r.get("timestamp")
            if ts in persist_df.index:
                vals = persist_df.loc[ts]
                for col in FEATURE_COLS:
                    v = vals[col]
                    if pd.isna(v):
                        r[col] = None
                    else:
                        if isinstance(v, (np.integer, int)) or col.startswith("bars"):
                            r[col] = int(v)
                        else:
                            r[col] = float(v)

        all_aggs_by_ticker[ticker] = rows

    _dump_json(all_aggs_by_ticker, file_path)
    logger.info(f"Successfully enriched and saved to {file_path}")

def add_signals(file_path: str = FILE_PATH) -> None:
    all_aggs_by_ticker = _load_json(file_path)
    tickers = list(all_aggs_by_ticker.keys())
    if not tickers:
        logger.info("No tickers found in data file.")
        return

    for ticker in tickers:
        rows = all_aggs_by_ticker.get(ticker, [])
        if not isinstance(rows, list) or not rows:
            logger.info(f"{ticker}: no rows to process; skipping.")
            continue

        df = pd.DataFrame(rows)
        if "timestamp" not in df.columns:
            logger.warning(f"{ticker}: missing 'timestamp' column; skipping.")
            continue

        try:
            sig_df = calculate_signals(df, ticker)
        except Exception:
            logger.error(f"{ticker}: error in calculate_signals", exc_info=True)
            continue

        if "timestamp" not in sig_df.columns:
            sig_df = sig_df.reset_index(drop=False)
        merge_df = sig_df[["timestamp"] + PERSIST_SIGNAL_COLS].copy()

        lookup = {
            int(row["timestamp"]): {k: row[k] for k in PERSIST_SIGNAL_COLS}
            for _, row in merge_df.iterrows()
        }

        updated = 0
        for r in rows:
            ts = int(r.get("timestamp", -1))
            if ts in lookup:
                for col in PERSIST_SIGNAL_COLS:
                    _json_assign(r, col, lookup[ts][col])
                updated += 1

        all_aggs_by_ticker[ticker] = rows
        logger.info(f"{ticker}: updated signals on {updated} rows.")

    _dump_json(all_aggs_by_ticker, file_path)
    logger.info(f"Signals written to {file_path}")

def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[logging.FileHandler("data_handler.log"), logging.StreamHandler()],
    )

def refresh_data(file_path: str = FILE_PATH,*, multiplier: int = 1, timespan: str = "day") -> None:

    setup_logging()
    update_data(file_path, multiplier=multiplier, timespan=timespan)
    process_data(file_path)
    add_signals(file_path)

if __name__ == "__main__":
    refresh_data()
