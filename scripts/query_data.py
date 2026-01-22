import requests
import pandas as pd
import numpy as np
import os
import sys
from datetime import datetime, timedelta
import time
import re


# =========================
# Config
# =========================
BASE_DIR = "data"
SLEEP_SEC = 2

latest_folder = sorted(os.listdir(f"{BASE_DIR}/raw"))[-1]
START_DATE = datetime.strptime(latest_folder, "%Y%m%d") + timedelta(days=1)
# START_DATE = datetime(2025, 9, 19)
END_DATE = datetime.today()


# =========================
# Utils
# =========================
def clean_numeric_series(s):
    s = s.astype(str)
    s = s.str.replace(r"<.*?>", "", regex=True)
    s = s.str.replace(",", "", regex=False)
    s = s.replace(["--", ""], np.nan)

    try:
        return s.astype(float)
    except ValueError:
        return s


def fetch_mi_index(date_str: str):
    url = (
        "https://www.twse.com.tw/exchangeReport/MI_INDEX"
        f"?response=json&date={date_str}&type=ALL"
    )
    r = requests.get(url, timeout=10)
    r.raise_for_status()
    return r.json()


# =========================
# Main
# =========================
def main():
    current_date = START_DATE

    while current_date <= END_DATE:
        date_str = current_date.strftime("%Y%m%d")
        print(f"Processing {date_str} ...")

        try:
            data = fetch_mi_index(date_str)
        except Exception as e:
            print(f"Request failed: {e}")
            current_date += timedelta(days=1)
            continue

        tables = data.get("tables", [])        

        # not a trading day
        if not tables or all(t == {} for t in tables):
            print("Not a trading day")
            current_date += timedelta(days=1)
            time.sleep(SLEEP_SEC)
            continue

        # check date
        table0_title = tables[0]["title"]
        table0_title_date = table0_title.split(" ")[0]
        date_match = re.search(r"(\d+)年(\d+)月(\d+)日", table0_title_date)
        roc_year, month, day = map(int, date_match.groups())
        data_date = f"{roc_year + 1911}{month:02d}{day:02d}"
        if data_date != date_str:
            sys.exit(f"Date mismatch: {date_str} != {table0_title_date}")
        else:
            pass
        
        # trading day → create folder
        day_dir = os.path.join(BASE_DIR, 'raw', date_str)
        os.makedirs(day_dir, exist_ok=True)

        for table in tables:
            if not table:
                continue

            title = table["title"]
            filename = os.path.join(day_dir, f"{title}.csv")

            df = pd.DataFrame(table["data"], columns=table["fields"])
            df.to_csv(filename, index=False)

        print(f"Saved {date_str}")
        current_date += timedelta(days=1)
        time.sleep(SLEEP_SEC)


if __name__ == "__main__":
    main()
