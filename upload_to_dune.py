#!/usr/bin/env python3
"""
Incremental fetch of stock prices from MarketStack with CSV cache and upload to Dune.
Includes exponential backoff and rate-limited API calls.
"""

import os
import io
import time
import requests
import pandas as pd
from datetime import datetime, timezone, timedelta
from dune_client.client import DuneClient

# ---------------- CONFIG ----------------
START_DATE = "2025-01-01"
CSV_FILENAME = "stock_prices.csv"
TABLE_NAME = "stock_prices"

# ---------------- SECRETS ----------------
DUNE_API_KEY = os.environ.get("DUNE_API_KEY")
MARKETSTACK_API_KEY = os.environ.get("MARKETSTACK_API_KEY")

if not DUNE_API_KEY:
    raise ValueError("Set DUNE_API_KEY as environment variable")

if not MARKETSTACK_API_KEY:
    raise ValueError("Set MARKETSTACK_API_KEY as environment variable")

# ---------------- TICKERS ----------------
dune = DuneClient(DUNE_API_KEY)
query_result = dune.get_latest_result(5617999)
rows = query_result.result.rows
dune_symbols = [row["token_symbol"] for row in rows]

manual_symbols = ["EXOD", "C3M", "CSPX", "IB01"]
SYMBOLS = list(set(dune_symbols + manual_symbols))


# ---------------- HELPERS ----------------

def fetch_symbol_data(symbol, start_date):
    """Fetch EOD stock prices from MarketStack for a given symbol, with retries."""
    all_data = []
    limit = 100  # MarketStack recommended max
    offset = 0
    start_date_str = start_date.strftime("%Y-%m-%d")

    while True:
        url = (
            f"https://api.marketstack.com/v2/eod?"
            f"access_key={MARKETSTACK_API_KEY}&"
            f"date_from={start_date_str}&"
            f"symbols={symbol}&limit={limit}&offset={offset}"
        )

        # Exponential backoff loop
        backoff = 1
        max_backoff = 60

        while True:
            r = requests.get(url)
            if r.status_code == 200:
                break

            if r.status_code == 429:  # Rate limit
                print(f"[429] Too many requests for {symbol}. Sleeping {backoff}s")
                time.sleep(backoff)
                backoff = min(backoff * 2, max_backoff)
                continue

            print(f"Error fetching {symbol}: {r.status_code}")
            return pd.DataFrame()

        data = r.json().get("data", [])
        if not data:
            break

        all_data.extend(data)
        offset += limit

    if not all_data:
        return pd.DataFrame()

    df = pd.DataFrame(all_data)
    df["date"] = pd.to_datetime(df["date"]).dt.date
    df["symbol"] = symbol
    df["close"] = df["close"].astype(float)

    return df[["date", "symbol", "close"]]


def fill_missing_dates(df, symbols):
    """Fill missing daily rows per symbol with forward-fill."""
    all_filled = []
    for symbol in symbols:
        symbol_df = df[df["symbol"] == symbol].sort_values("date").copy()
        if symbol_df.empty:
            continue

        first_date = symbol_df["date"].min()
        end_date = datetime.now(timezone.utc).date()

        all_days = pd.date_range(first_date, end_date, freq="D").date

        full_index = pd.MultiIndex.from_product(
            [all_days, [symbol]], names=["date", "symbol"]
        )
        full_df = pd.DataFrame(index=full_index).reset_index()

        full_df = full_df.merge(symbol_df, on=["date", "symbol"], how="left")
        full_df["close"] = full_df.groupby("symbol")["close"].ffill()

        full_df = full_df.dropna(subset=["close"])
        all_filled.append(full_df)

    if all_filled:
        return pd.concat(all_filled, ignore_index=True)

    return pd.DataFrame(columns=["date", "symbol", "close"])


def upload_to_dune_csv(df, api_key, table_name):
    """Upload DataFrame as CSV to Dune."""
    df.columns = [c.replace(" ", "_").lower() for c in df.columns]

    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    csv_data = csv_buffer.getvalue()

    url = "https://api.dune.com/api/v1/table/upload/csv"
    headers = {"X-DUNE-API-KEY": api_key}

    payload = {
        "table_name": table_name,
        "description": "Daily stock prices",
        "data": csv_data,
    }

    r = requests.post(url, headers=headers, json=payload)

    if r.status_code != 200:
        raise ValueError(f"Upload failed: {r.status_code} {r.text}")

    print("Upload successful:", r.text)


# ---------------- MAIN ----------------

# Replace the MAIN section:

if __name__ == "__main__":
    print("Checking for existing CSV cache...")

    if os.path.exists(CSV_FILENAME):
        existing_df = pd.read_csv(CSV_FILENAME)
        existing_df['date'] = pd.to_datetime(existing_df['date']).dt.date
        last_date = existing_df["date"].max()
        print(f"Last cached date: {last_date}")
    else:
        existing_df = pd.DataFrame(columns=["date", "symbol", "close"])
        last_date = datetime.strptime(START_DATE, "%Y-%m-%d").date()
        print("No existing CSV found. Starting from START_DATE.")

    fetch_start_date = last_date + timedelta(days=1)
    today = datetime.now(timezone.utc).date()
    
    # Skip if already up to date
    if fetch_start_date > today:
        print(f"Cache is already up to date (last date: {last_date})")
        full_df = existing_df
    else:
        print(f"Fetching new data from {fetch_start_date} to {today}...")

        new_data_parts = []
        for s in SYMBOLS:
            print(f"Fetching {s}…")
            df_s = fetch_symbol_data(s, fetch_start_date)
            new_data_parts.append(df_s)
            time.sleep(0.3)

        new_data = pd.concat(new_data_parts, ignore_index=True) if new_data_parts else pd.DataFrame()

        if new_data.empty:
            print("No new data fetched from API.")
            full_df = existing_df
        else:
            print(f"Fetched {len(new_data)} new rows. Filling missing dates for new data only…")
            
            # Only fill missing dates for the NEW data range
            filled_new = fill_missing_dates(new_data, SYMBOLS)
            
            # Simple append - no re-processing of old data
            full_df = pd.concat([existing_df, filled_new], ignore_index=True)
            
            # Remove any duplicates (keep latest)
            full_df = full_df.drop_duplicates(subset=["date", "symbol"], keep="last")
            full_df = full_df.sort_values(["symbol", "date"]).reset_index(drop=True)

            full_df.to_csv(CSV_FILENAME, index=False)
            print(f"Updated CSV cache saved ({len(full_df)} total rows, {len(filled_new)} new).")

    print(f"Uploading {len(full_df)} rows to Dune…")
    upload_to_dune_csv(full_df, DUNE_API_KEY, TABLE_NAME)
