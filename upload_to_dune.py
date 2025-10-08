#!/usr/bin/env python3
"""
Fetch stock prices from MarketStack and upload a daily CSV to Dune.
Both API keys are read from environment variables.
"""

import os
import io
import requests
import pandas as pd
from datetime import datetime, timezone
from dune_client.client import DuneClient

# ---------------- CONFIG ----------------
START_DATE = "2022-01-01"
CSV_FILENAME = "stock_prices.csv"  # Name of CSV file uploaded to Dune

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
SYMBOLS = [row['token_symbol'] for row in rows]

# ---------------- HELPERS ----------------
def fetch_symbol_data(symbol):
    """Fetch EOD stock prices from MarketStack for a given symbol, with pagination."""
    all_data = []
    limit = 100
    offset = 0

    while True:
        url = (
            f"https://api.marketstack.com/v2/eod?"
            f"access_key={MARKETSTACK_API_KEY}&"
            f"date_from={START_DATE}&"
            f"symbols={symbol}&limit={limit}&offset={offset}"
        )
        r = requests.get(url)
        if r.status_code != 200:
            print(f"Error fetching {symbol}: {r.status_code}")
            break

        data = r.json().get("data", [])
        if not data:
            break

        all_data.extend(data)
        offset += limit  # move to next page

    if not all_data:
        return pd.DataFrame()

    df = pd.DataFrame(all_data)
    df["date"] = pd.to_datetime(df["date"]).dt.date
    df["symbol"] = symbol
    df["close"] = df["close"].astype(float)
    return df[["date", "symbol", "close"]]

def fill_missing_dates(df, symbols):
    """Fill missing dates per symbol with forward-fill starting from first available date."""
    all_filled = []

    for symbol in symbols:
        symbol_df = df[df["symbol"] == symbol].sort_values("date").copy()
        if symbol_df.empty:
            continue

        first_date = symbol_df["date"].min()
        end_date = pd.to_datetime(datetime.now(timezone.utc).date())
        all_days = pd.date_range(first_date, end_date, freq="D").date

        full_index = pd.MultiIndex.from_product([all_days, [symbol]], names=["date", "symbol"])
        full_df = pd.DataFrame(index=full_index).reset_index()
        full_df = full_df.merge(symbol_df, on=["date", "symbol"], how="left")

        # Forward-fill missing close values
        full_df["close"] = full_df.groupby("symbol")["close"].ffill()

        # Drop any rows still null (before stock existed)
        full_df = full_df.dropna(subset=["close"])

        all_filled.append(full_df)

    return pd.concat(all_filled, ignore_index=True)

def upload_to_dune_csv(df, api_key, filename):
    """Upload DataFrame as CSV to Dune using JSON payload."""
    # Clean column names
    df.columns = [c.replace(" ", "_").lower() for c in df.columns]

    # Convert DataFrame to CSV string
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    csv_data = csv_buffer.getvalue()

    # Dune API URL and headers
    url = "https://api.dune.com/api/v1/table/upload/csv"
    headers = {"X-DUNE-API-KEY": api_key}

    payload = {
        "table_name": "stock_prices",
        "description": "Daily stock prices",
        "data": csv_data
    }

    r = requests.post(url, headers=headers, json=payload)
    if r.status_code != 200:
        raise ValueError(f"Upload failed: {r.status_code} {r.text}")
    print("Upload successful:", r.text)


# ---------------- MAIN ----------------
if __name__ == "__main__":
    print("Fetching stock data...")
    all_data = pd.concat([fetch_symbol_data(s) for s in SYMBOLS], ignore_index=True)

    print("Filling missing dates...")
    full_data = fill_missing_dates(all_data, SYMBOLS)

    print(f"Prepared {len(full_data)} rows for upload")
    print(f"Uploading CSV file '{CSV_FILENAME}' to Dune...")
    upload_to_dune_csv(full_data, DUNE_API_KEY, CSV_FILENAME)
