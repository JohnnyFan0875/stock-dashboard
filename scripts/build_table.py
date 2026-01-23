import pandas as pd
import numpy as np
import os
import re
from pathlib import Path

# =========================
# Config
# =========================
RAW_DIR = Path("data/raw")
OUT_DIR = Path("data/processed")
OUT_DIR.mkdir(exist_ok=True)

OUT_FILE_PARQUET = OUT_DIR / "daily.parquet"
OUT_FILE_CSV = OUT_DIR / "daily.csv"

# define patterns
STOCK_PATTERN = re.compile(r"^\d{4}$")   # 2330
ETF_PATTERN   = re.compile(r"^00\d{2,3}$") # 0050, 00878

# =========================
# Helpers
# =========================
def is_stock_or_etf(code: str) -> bool:
    if not isinstance(code, str):
        return False
    return bool(STOCK_PATTERN.match(code) or ETF_PATTERN.match(code))


def clean_numeric(s):
    return pd.to_numeric(
        s.astype(str)
         .str.replace(",", "", regex=False)
         .str.replace(r"<.*?>", "", regex=True)
         .replace(["--", "", "nan"], pd.NA),
        errors="coerce"
    )

def add_ma_features(df):
    df["MA5"]  = df["close"].rolling(5).mean()
    df["MA10"] = df["close"].rolling(10).mean()
    df["MA20"] = df["close"].rolling(20).mean()
    return df

def add_kd_features(df, n=9):
    low_n  = df["low"].rolling(n, min_periods=1).min()
    high_n = df["high"].rolling(n, min_periods=1).max()

    denom = (high_n - low_n).replace(0, np.nan)
    rsv = 100 * (df["close"] - low_n) / denom

    df["K"] = rsv.ewm(alpha=1/3, adjust=False).mean()
    df["D"] = df["K"].ewm(alpha=1/3, adjust=False).mean()
    return df

def add_macd_features(df, fast=12, slow=26, signal=9):
    ema_fast = df["close"].ewm(span=fast, adjust=False).mean()
    ema_slow = df["close"].ewm(span=slow, adjust=False).mean()

    df["DIF"] = ema_fast - ema_slow
    df["MACD"] = df["DIF"].ewm(span=signal, adjust=False).mean()
    df["MACD_hist"] = df["DIF"] - df["MACD"]
    return df

# =========================
# Main
# =========================
all_rows = []

for day_dir in sorted(RAW_DIR.iterdir()):
    if not day_dir.is_dir():
        continue

    date_str = day_dir.name  # YYYYMMDD
    year, month, day = int(date_str[:4]), int(date_str[4:6]), int(date_str[6:])
    csv_filename = f"{year-1911}年{month:02d}月{day:02d}日 每日收盤行情(全部).csv"
    csv_path = day_dir / csv_filename

    if not csv_path.exists():
        continue

    print(f"Processing {date_str}")

    df = pd.read_csv(csv_path, dtype=str)

    required_cols = [
        "證券代號",
        "證券名稱",
        "開盤價",
        "最高價",
        "最低價",
        "收盤價",
        "成交股數",
    ]

    missing = set(required_cols) - set(df.columns)
    if missing:
        print(f"  Skip {date_str}, missing columns: {missing}")
        continue

    # filter by stock or etf
    df = df[df["證券代號"].apply(is_stock_or_etf)].copy()

    # clean
    df["open"]   = clean_numeric(df["開盤價"])
    df["high"]   = clean_numeric(df["最高價"])
    df["low"]    = clean_numeric(df["最低價"])
    df["close"]  = clean_numeric(df["收盤價"])
    df["volume"] = clean_numeric(df["成交股數"])

    # add date
    df["date"] = pd.to_datetime(date_str)
    df["stock_id"] = df["證券代號"]
    df["stock_name"] = df["證券名稱"]

    df = df[[
        "date",
        "stock_id",
        "stock_name",
        "open",
        "high",
        "low",
        "close",
        "volume",
    ]]

    all_rows.append(df)

# Concat & Save
if not all_rows:
    raise RuntimeError("No valid trading data found.")

final_df = (
    pd.concat(all_rows, ignore_index=True)
    .sort_values(["stock_id", "date"])
    .groupby("stock_id", group_keys=False)
    .apply(add_ma_features)
    .pipe(lambda d: d.groupby("stock_id", group_keys=False).apply(add_kd_features))
    .pipe(lambda d: d.groupby("stock_id", group_keys=False).apply(add_macd_features))
)

print(final_df.dtypes)
bad_rows = final_df[
    final_df["open"].apply(lambda x: isinstance(x, str))
]
print(bad_rows[["date", "stock_id", "stock_name", "open"]].head())


final_df.to_parquet(OUT_FILE_PARQUET, index=False)
final_df.to_csv(OUT_FILE_CSV, index=False)

print(final_df.head())
