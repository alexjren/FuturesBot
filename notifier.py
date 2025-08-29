import requests
import pandas as pd
import numpy as np
import json
from datetime import datetime
from config import DISCORD_WEBHOOK_URL, FILE_PATH

def _fmt(x):
        if x is None or (isinstance(x, float) and np.isnan(x)):
            return "n/a"
        return f"{float(x):.4f}".rstrip("0").rstrip(".")

def build_discord_message(file_path: str = FILE_PATH):
    with open(file_path, "r") as f:
        data = json.load(f)

    messages = []
    for ticker, rows in (data or {}).items():
        if not isinstance(rows, list) or not rows:
            messages.append(f"{ticker}: No signal for next bar")
            continue

        df = pd.DataFrame(rows)

        if "timestamp" in df.columns:
            df = df.sort_values("timestamp")
        elif "date" in df.columns:
            df["__date_parsed__"] = pd.to_datetime(df["date"], utc=True, errors="coerce")
            df = df.sort_values("__date_parsed__")

        r = df.iloc[-1]

        sig = (r.get("signal") or "").strip().lower()
        entry = r.get("entry_price")
        stop  = r.get("stop_price")
        targ  = r.get("target_price")

        if sig in ("long", "short") and pd.notna(entry):
            messages.append(f"**{ticker}**: {sig.capitalize()} at {_fmt(entry)}, stop {_fmt(stop)}, target {_fmt(targ)}")
        else:
            messages.append(f"**{ticker}**: No signal for next bar")
    return messages

def post_discord(messages):
    
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

    header = f"**Soup of the Day - {now_str}**\n{line}\n"
    body   = "\n".join(messages if messages else ["No signals."])
    footer = f"\n{line}"

    content = f"{header}{body}{footer}"

    if len(content) > 2000:
        chunks = [content[i:i + 1999] for i in range(0, len(content), 1999)]
        for chunk in chunks:
            payload = {"content": chunk}
            headers = {"Content-Type": "application/json"}
            response = requests.post(DISCORD_WEBHOOK_URL, json=payload, headers=headers)
            try:
                response.raise_for_status()
            except requests.HTTPError as e:
                print(f"Failed to send chunk to Discord: {e} - {response.text}")
                raise
    else:
        payload = {"content": content}
        headers = {"Content-Type": "application/json"}
        response = requests.post(DISCORD_WEBHOOK_URL, json=payload, headers=headers)
        try:
            response.raise_for_status()
        except requests.HTTPError as e:
            print(f"Failed to send signals to Discord: {e} - {response.text}")
            raise

    return response

def post_image(image_path: str = "account_value.png", caption: str = "Account Value Backtest"):
    with open(image_path, "rb") as f:
        files = {"file": (image_path, f)}
        payload = {"content": caption}
        response = requests.post(DISCORD_WEBHOOK_URL, data=payload, files=files)
        try:
            response.raise_for_status()
        except requests.HTTPError as e:
            print(f"Failed to send image to Discord: {e} - {response.text}")
            raise
    return response

def send_discord_message(file_path: str = FILE_PATH):
    messages = build_discord_message(file_path)
    post_discord(messages)
    post_image()

if __name__ == "__main__":
    send_discord_message()