import requests
import json
import pandas as pd
from datetime import date, timedelta, datetime, timezone
from pathlib import Path

import os
from dotenv import load_dotenv

load_dotenv(dotenv_path='./.env')

apikey = os.getenv("API_KEY")

def fetch_all_stock_data() -> str | None:

    try:

        time_series_base_url =  "https://api.twelvedata.com/time_series"

        params = {
            "interval" : "1day",
            "start_date" : "2026-03-01",
            "end_date" : "2026-03-31",
            "symbol" : "AAPL,GOOG,NVDA"
        }

        headers = {
            "Authorization" : f"apikey {apikey}"
        }

        time_series_url = f"{time_series_base_url}"

        response = requests.get(
            time_series_url,
            params=params,
            headers=headers
        )
        response.raise_for_status()

        data = response.json()

        # print(json.dumps(data, indent=2))

        all_rows = [] # values of a dictionary can be appended to a list
        stock_dim_rows = []

        for symbol, stock_data in data.items():

            meta = stock_data.get("meta", [])
            values = stock_data.get("values", [])

            for row in values:
                all_rows.append({
                    "symbol" : symbol,
                    "datetime" : row.get("datetime"),
                    "open" : row.get("open"),
                    "high" : row.get("high"),
                    "low" : row.get("low"),
                    "close" : row.get("close"),
                    "volume" : row.get("volume")
                })

            for row in values:
                stock_dim_rows.append({
                    "symbol" : symbol,
                    "interval" : meta.get("interval"),
                    "currency" : meta.get("currency"),
                    "exchange" : meta.get("exchange"),
                    "mic_code" : meta.get("mic_code"),
                    "type" : meta.get("type")
                })    

        # print(all_rows)

        df_stock_fact = pd.DataFrame(all_rows)  # a list with dictionary values is converted into a tabular dataframe
        df_stock_fact["extracted_at_utc"] = datetime.now(timezone.utc).isoformat()

        df_stock_dim = pd.DataFrame(stock_dim_rows).drop_duplicates()  # a list with dictionary values is converted into a tabular dataframe
        df_stock_dim["extracted_at_utc"] = datetime.now(timezone.utc).isoformat()

        print(df_stock_fact.head())
        print('/n')
        print(df_stock_dim.head())

    except requests.exceptions.RequestsException as e:
        raise e

    # print(df_stock_fact.head())
    # print('/n')
    # print(df_stock_dim.head())


    return None



if __name__ == "__main__":
    fetch_all_stock_data()



