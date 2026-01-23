import pandas as pd
import numpy as np


def calculate_signals(df: pd.DataFrame) -> pd.DataFrame:

    df = df.sort_values("date").copy()

    # =========================
    # Trend definition
    # =========================
    df["trend_ok"] = (
        (df["DIF"] > 0) &
        (df["MACD"] > 0) &
        (df["DIF"] > df["MACD"])
    )

    # =========================
    # KD cross & bars since cross
    # =========================
    df["kd_cross"] = (
        (df["K"] > df["D"]) &
        (df["K"].shift(1) <= df["D"].shift(1))
    )

    df["bars_after_kd_cross"] = df["kd_cross"].cumsum()
    df["bars_after_kd_cross"] = df.groupby("bars_after_kd_cross").cumcount()

    # if never crossed
    df.loc[df["kd_cross"].cumsum() == 0, "bars_after_kd_cross"] = 999

    kd_gap = df["K"] - df["D"]

    # =========================
    # Entry strategies
    # =========================
    df["entry_pre_pullback"] = (
        df["trend_ok"] &
        (df["K"] < df["D"]) &
        ((df["D"] - df["K"]) < 3) &
        (df["close"] > df["MA10"])
    )

    df["entry_pullback"] = (
        df["trend_ok"] &
        (df["bars_after_kd_cross"] <= 2) &
        (kd_gap > kd_gap.shift(1)) &
        (df["K"] < 80) &
        (df["close"] > df["MA10"])
    )

    df["entry_breakout"] = (
        df["trend_ok"] &
        (df["K"] > 50) &
        (df["close"] > df["MA10"]) &
        (df["close"].shift(1) <= df["MA10"])
    )

    df["entry_continuation"] = (
        df["trend_ok"] &
        (df["K"] > 50) &
        (df["K"] < 80) &
        (df["close"] > df["MA10"])
    )

    df["any_entry"] = (
        df["entry_pullback"] |
        df["entry_breakout"] |
        df["entry_continuation"]
    )

    df["bars_since_entry"] = df["any_entry"].cumsum()
    df["bars_since_entry"] = df.groupby("any_entry").cumcount()

    # =========================
    # Exit strategies
    # =========================
    df["exit_emergency"] = df["close"] < (df["MA20"] * 0.97)

    exit_allowed = df["bars_since_entry"] > 3

    kd_death_cross = (
        (df["K"] < df["D"]) &
        (df["K"].shift(1) >= df["D"].shift(1))
    )

    high_level_exit = kd_death_cross & (df["K"] > 70)

    exit_price = df["close"] < df["MA20"]
    exit_macd = (df["DIF"] < df["MACD"]) & (df["MACD_hist"] < 0)

    df["exit_trend"] = (
        (exit_price | exit_macd | high_level_exit) &
        exit_allowed
    )

    return df
