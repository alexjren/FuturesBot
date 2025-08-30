import os

def load_env(path=".env"):
    if not os.path.exists(path):
        return
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            key, value = line.split("=", 1)
            os.environ[key] = value

load_env()

DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL")
API_KEY = os.getenv("API_KEY")

TICKERS = ["X:BTCUSD","X:ETHUSD", "C:EURUSD", "C:GBPUSD", "C:USDJPY", "I:NDX"]
# TICKERS = ["I:NDX"]
TIMEFRAMES = ["1 minute", "5 minute", "15 minute", "30 minute", "1 hour", "4 hour", "1 day"]
# TIMEFRAMES = ["30 minute", "1 day"]
DB_PATH = "market.db"
ATR_PERIOD = 20
ENTRY_PERIOD = 20
EXIT_PERIOD = 6
RISK_PERCENT = 0.02
INIT_ACCOUNT_VALUE = 10000.0  # Example account value.

DOLLAR_PER_POINT = {
    "QQQ": 1.0,
    "SPY": 1.0,
    "X:BTCUSD": 1.0,
    "X:ETHUSD": 1.0,
    "C:EURUSD": 10000.0,
    "C:GBPUSD": 10000.0,
    "C:USDJPY": 67.8,
    "I:NDX": 2.0,
}

TICK_SIZE = {
    "QQQ": 0.01,
    "SPY": 0.01,
    "X:BTCUSD": 0.01,
    "X:ETHUSD": 0.01,
    "C:EURUSD": 0.0001,
    "C:GBPUSD": 0.0001,
    "C:USDJPY": 0.01,
    "I:NDX": 0.25,
}

COMMISSIONS = {
    "QQQ": 0.00,
    "SPY": 0.00,
    "X:BTCUSD": 0.00,
    "X:ETHUSD": 0.00,
    "C:EURUSD": 0.00,
    "C:GBPUSD": 0.00,
    "C:USDJPY": 0.00,
    "I:NDX": 0.39,
}