import pandas as pd
import numpy as np

# =========================
# Signal Labeling
# =========================
def get_signal_label(r):
    sig_list = []
    if r["entry_pullback"]: sig_list.append("pullback")
    if r["entry_breakout"]: sig_list.append("breakout")
    if r["entry_continuation"]: sig_list.append("continuation")
    if r["exit_trend"]: sig_list.append("exit")
    if r["exit_emergency"]: sig_list.append("emergency")
    
    return "+".join(sig_list) if sig_list else "none"


def calculate_signals(df: pd.DataFrame) -> pd.DataFrame:

    df = df.sort_values("date").copy()

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
        (df["DIF"] > 0) &
        (df["MACD"] > 0) &
        (df["DIF"] > df["MACD"]) &
        (df["K"] < df["D"]) &
        ((df["D"] - df["K"]) < 3) &
        (df["close"] > df["MA10"])
    )

    df["entry_pullback"] = (
        (df["DIF"] > 0) &
        (df["MACD"] > 0) &
        (df["DIF"] > df["MACD"]) &
        (df["bars_after_kd_cross"] <= 2) &
        (kd_gap > kd_gap.shift(1)) &
        (df["K"] < 80) &
        (df["close"] > df["MA10"])
    )

    df["entry_breakout"] = (
        (df["DIF"] > 0) &
        (df["MACD"] > 0) &
        (df["DIF"] > df["MACD"]) &
        (df["K"] > 50) &
        (df["close"] > df["MA10"]) &
        (df["close"].shift(1) <= df["MA10"])
    )

    df["entry_continuation"] = (
        (df["DIF"] > 0) &
        (df["MACD"] > 0) &
        (df["DIF"] > df["MACD"]) &
        (df["K"] > 50) &
        (df["K"] < 80) &
        (df["close"] > df["MA10"])
    )

    df["any_entry"] = (
        df["entry_pullback"] |
        df["entry_breakout"] |
        df["entry_continuation"]
    )

    entry_groups = df["any_entry"].cumsum()
    df["bars_since_entry"] = df.groupby(entry_groups).cumcount()

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

    # =========================
    # Signal labeling
    # =========================
    df["signal_today"] = df.apply(get_signal_label, axis=1)

    return df
