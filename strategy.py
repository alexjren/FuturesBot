import pandas as pd
import numpy as np
from math import floor
from config import DOLLAR_PER_POINT, RISK_PERCENT, INIT_ACCOUNT_VALUE, TICK_SIZE, COMMISSIONS

def calculate_signals(df, ticker):
    if df.empty:
        return df

    out = df.copy()

    if "date" not in out.columns:
        out["date"] = pd.to_datetime(out["timestamp"], unit="ms", utc=True)
    out = out.sort_values("date").reset_index(drop=True)
    out.set_index("date", inplace=True)

    required = {"prev_low", "prev_high", "bars_since_low", "bars_since_high", "low_exit", "high_exit", "low", "high", "close"}
    missing = set(required) - set(out.columns)
    if missing:
        raise ValueError(f"DataFrame lacks required columns: {', '.join(sorted(missing))}")

    out["position"]        = np.nan   
    out["signal"]          = None    
    out["entry_price"]     = np.nan 
    out["stop_price"]      = np.nan  
    out["target_price"]    = np.nan   
    out["position_basis"]  = np.nan   
    out["unit_size"]       = np.nan   
    out["account_value"] = np.nan

    first_idx = out.index[0]
    out.at[first_idx, "position"]        = 0.0
    out.at[first_idx, "signal"]          = "no signal"
    out.at[first_idx, "entry_price"]     = np.nan
    out.at[first_idx, "stop_price"]      = np.nan
    out.at[first_idx, "target_price"]    = np.nan
    out.at[first_idx, "position_basis"]  = np.nan
    out.at[first_idx, "unit_size"]       = np.nan
    out.at[first_idx, "account_value"] = INIT_ACCOUNT_VALUE

    tick = TICK_SIZE[ticker]
    dollars_per_point = DOLLAR_PER_POINT[ticker]
    commissions = COMMISSIONS[ticker]
    slippage = 4*tick*dollars_per_point

    idx = list(out.index)

    for i in range(1, len(out)):
        prev_i = idx[i-1]
        cur_i  = idx[i]

        prev_pos        = out.at[prev_i, "position"]
        prev_stop       = out.at[prev_i, "stop_price"]
        prev_target     = out.at[prev_i, "target_price"]
        prev_unit_size  = out.at[prev_i, "unit_size"]
        prev_position_basis = out.at[prev_i, "position_basis"]
        prev_account_value = out.at[prev_i, "account_value"]

        out.at[cur_i, "position"]       = prev_pos
        out.at[cur_i, "signal"]         = "no_signal"
        out.at[cur_i, "stop_price"]     = prev_stop
        out.at[cur_i, "target_price"]   = prev_target
        out.at[cur_i, "unit_size"]      = prev_unit_size
        out.at[cur_i, "position_basis"] = prev_position_basis
        out.at[cur_i, "account_value"] = prev_account_value

        if prev_pos == 1.0:
            low_bar  = out.at[cur_i, "low"]
            high_bar = out.at[cur_i, "high"]

            stop_hit   = pd.notna(prev_stop)   and (low_bar  <= prev_stop)
            target_hit = pd.notna(prev_target) and (high_bar >= prev_target)

            if stop_hit:
                out.at[cur_i, "account_value"] = prev_account_value + (prev_stop - prev_position_basis) * prev_unit_size - 2*commissions*prev_unit_size - slippage
                out.at[cur_i, "position"]       = 0.0
                out.at[cur_i, "signal"]         = "close"
                out.at[cur_i, "stop_price"]     = np.nan
                out.at[cur_i, "target_price"]   = np.nan
                out.at[cur_i, "position_basis"] = np.nan
                out.at[cur_i, "unit_size"]      = np.nan
                continue
            elif target_hit:
                out.at[cur_i, "account_value"] = prev_account_value + (prev_target - prev_position_basis) * prev_unit_size - 2*commissions*prev_unit_size - slippage
                out.at[cur_i, "position"]       = 0.0
                out.at[cur_i, "signal"]         = "close"
                out.at[cur_i, "stop_price"]     = np.nan
                out.at[cur_i, "target_price"]   = np.nan
                out.at[cur_i, "position_basis"] = np.nan
                out.at[cur_i, "unit_size"]      = np.nan
                continue

        elif prev_pos == -1.0:
            low_bar  = out.at[cur_i, "low"]
            high_bar = out.at[cur_i, "high"]

            stop_hit   = pd.notna(prev_stop)   and (high_bar  >= prev_stop)
            target_hit = pd.notna(prev_target) and (low_bar <= prev_target)

            if stop_hit:
                out.at[cur_i, "account_value"] = prev_account_value - (prev_stop - prev_position_basis) * prev_unit_size * dollars_per_point - 2*commissions*prev_unit_size - slippage
                out.at[cur_i, "position"]       = 0.0
                out.at[cur_i, "signal"]         = "close"
                out.at[cur_i, "stop_price"]     = np.nan
                out.at[cur_i, "target_price"]   = np.nan
                out.at[cur_i, "position_basis"] = np.nan
                out.at[cur_i, "unit_size"]      = np.nan
                continue
            elif target_hit:
                out.at[cur_i, "account_value"] = prev_account_value - (prev_target - prev_position_basis) * prev_unit_size * dollars_per_point - 2*commissions*prev_unit_size - slippage
                out.at[cur_i, "position"]       = 0.0
                out.at[cur_i, "signal"]         = "close"
                out.at[cur_i, "stop_price"]     = np.nan
                out.at[cur_i, "target_price"]   = np.nan
                out.at[cur_i, "position_basis"] = np.nan
                out.at[cur_i, "unit_size"]      = np.nan
                continue

        if prev_pos == 0.0:
            y_signal = out.at[prev_i, "signal"]
            y_entry  = out.at[prev_i, "entry_price"]
            y_units  = out.at[prev_i, "unit_size"]
            y_stop   = out.at[prev_i, "stop_price"]

            cur_high = out.at[cur_i, "high"]
            cur_low  = out.at[cur_i, "low"]
            cur_open= out.at[cur_i, "open"]

            if y_signal == "long" and pd.notna(y_entry) and pd.notna(y_units) and y_units >= 1:
                if pd.notna(cur_high) and (cur_high >= y_entry):
                    fill_price = float(max(cur_open, y_entry))

                    out.at[cur_i, "position"]       = 1.0
                    out.at[cur_i, "signal"]         = "no signal"
                    out.at[cur_i, "position_basis"] = fill_price
                    out.at[cur_i, "unit_size"]      = int(y_units)
                    out.at[cur_i, "stop_price"]     = float(y_stop) if pd.notna(y_stop) else np.nan

                    high_exit_bar = out.at[cur_i, "high_exit"]
                    out.at[cur_i, "target_price"] = float(high_exit_bar) if pd.notna(high_exit_bar) else np.nan

            elif y_signal == "short" and pd.notna(y_entry) and pd.notna(y_units) and y_units >= 1:
                if pd.notna(cur_low) and (cur_low <= y_entry):
                    fill_price = float(min(cur_open, y_entry))

                    out.at[cur_i, "position"]       = -1.0
                    out.at[cur_i, "signal"]         = "no signal"
                    out.at[cur_i, "position_basis"] = fill_price
                    out.at[cur_i, "unit_size"]      = int(y_units)
                    out.at[cur_i, "stop_price"]     = float(y_stop) if pd.notna(y_stop) else np.nan

                    low_exit_bar = out.at[cur_i, "low_exit"]
                    out.at[cur_i, "target_price"] = float(low_exit_bar) if pd.notna(low_exit_bar) else np.nan

        if out.at[cur_i, "position"] == 0.0:
            prevL_bar   = out.at[cur_i, "prev_low"]
            bars_since_low  = out.at[cur_i, "bars_since_low"]
            close_bar     = out.at[cur_i, "close"]
            low_bar       = out.at[cur_i, "low"]

            cond_breakdown = (pd.notna(prevL_bar) and pd.notna(close_bar) and (close_bar < prevL_bar))
            cond_old_long       = (pd.notna(bars_since_low) and bars_since_low > 3)

            if cond_breakdown and cond_old_long and pd.notna(low_bar):
                planned_entry_L = float(prevL_bar)
                planned_stop_L  = float(low_bar - tick)

                risk_points = abs(planned_entry_L - planned_stop_L)
                if risk_points > 0 and dollars_per_point > 0:
                    planned_units_L = floor((RISK_PERCENT * prev_account_value) / (risk_points * dollars_per_point))
                else:
                    planned_units_L = 0

                out.at[cur_i, "signal"]      = "long"
                out.at[cur_i, "entry_price"] = planned_entry_L
                out.at[cur_i, "stop_price"]  = planned_stop_L
                out.at[cur_i, "unit_size"]   = int(planned_units_L) if planned_units_L >= 1 else np.nan
                out.at[cur_i, "target_price"]    = out.at[cur_i, "high_exit"]

            prevH_bar   = out.at[cur_i, "prev_high"]
            bars_since_high  = out.at[cur_i, "bars_since_high"]
            high_bar       = out.at[cur_i, "high"]

            cond_breakup = (pd.notna(prevH_bar) and pd.notna(close_bar) and (close_bar > prevH_bar))
            cond_old_short       = (pd.notna(bars_since_high) and bars_since_high > 3)

            if cond_breakup and cond_old_short and pd.notna(high_bar):
                planned_entry_S = float(prevH_bar)
                planned_stop_S  = float(high_bar + tick)

                risk_points = abs(planned_entry_S - planned_stop_S)
                if risk_points > 0 and dollars_per_point > 0:
                    planned_units_S = floor((RISK_PERCENT * prev_account_value) / (risk_points * dollars_per_point))
                else:
                    planned_units_S = 0

                out.at[cur_i, "signal"]      = "short"
                out.at[cur_i, "entry_price"] = planned_entry_S
                out.at[cur_i, "stop_price"]  = planned_stop_S
                out.at[cur_i, "unit_size"]   = int(planned_units_S) if planned_units_S >= 1 else np.nan
                out.at[cur_i, "target_price"]    = out.at[cur_i, "low_exit"]

        if out.at[cur_i, "position"] == 1.0:
            low_bar = out.at[cur_i, "low"]
            if pd.notna(low_bar) and low_bar > out.at[cur_i, "position_basis"]:
                out.at[cur_i, "stop_price"] = float(low_bar - tick)

            if "high_exit" in out.columns and pd.notna(out.at[cur_i, "high_exit"]):
                out.at[cur_i, "target_price"] = float(out.at[cur_i, "high_exit"])
        elif out.at[cur_i, "position"] == -1.0:
            high_bar = out.at[cur_i, "high"]
            if pd.notna(high_bar) and high_bar < out.at[cur_i, "position_basis"]:
                out.at[cur_i, "stop_price"] = float(high_bar + tick)

            if "low_exit" in out.columns and pd.notna(out.at[cur_i, "low_exit"]):
                out.at[cur_i, "target_price"] = float(out.at[cur_i, "low_exit"])

    return out
