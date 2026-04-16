import json
import os
import boto3
import urllib.request
import urllib.parse
import csv
import io
from datetime import datetime, timezone

s3 = boto3.client("s3")
secrets_client = boto3.client("secretsmanager")

BUCKET_NAME = os.environ["BUCKET_NAME"]
SECRET_NAME = os.environ["SECRET_NAME"]


def get_api_key():
    response = secrets_client.get_secret_value(SecretId=SECRET_NAME)
    secret_string = response["SecretString"]
    secret_json = json.loads(secret_string)
    return secret_json["api_key"]

def rows_to_csv_string(rows, fieldnames):
    buffer = io.StringIO() # creates an in memory space to store the data. 
    writer = csv.DictWriter(buffer, fieldnames=fieldnames) # the csv writer writes into the buffer. Uses fieldnames as column order
    writer.writeheader() # writies the first line with just the column names. 
    writer.writerows(rows)

    return buffer.getvalue()



def lambda_handler(event, context):
    event = event or {}

    symbols = event.get("symbols", ["AAPL", "GOOG", "NVDA"])
    start_date = event.get("start_date", "2026-01-01")
    end_date = event.get("end_date", "2026-03-31")

    api_key = get_api_key()

    base_url = "https://api.twelvedata.com/time_series"
    params = {
        "interval": "1day",
        "start_date": start_date,
        "end_date": end_date,
        "symbol": ",".join(symbols),
        "apikey": api_key
    }

    query_string = urllib.parse.urlencode(params)
    url = f"{base_url}?{query_string}"

    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": "finance-stock-lambda/1.0"
        }
    )

    with urllib.request.urlopen(req, timeout=30) as response:
        response_body = response.read().decode("utf-8")
        data = json.loads(response_body)

    run_ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    extract_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    extracted_date = datetime.now(timezone.utc).isoformat()

    all_rows = []
    stock_dim_rows = []

    for symbol, stock_data in data.items():
        meta = stock_data.get("meta", {})
        values = stock_data.get("values", [])

        # extracting the stock dimension. 

        stock_dim_rows.append({
            "symbol": symbol, 
            "interval": meta.get("interval"),
            "currency": meta.get("currency"),
            "exchange": meta.get("exchange"),
            "mic_code": meta.get("mic_code"),
            "type": meta.get("type"),
            "extracted_at_utc": extracted_date
        })

        for row in values:
            all_rows.append({
                "symbol": symbol,
                "price_date": row.get("datetime"),
                "open": row.get("open"),
                "high": row.get("high"),
                "low": row.get("low"),
                "close": row.get("close"),
                "volume": row.get("volume"),
                "extracted_at_utc": extracted_date
            })

    raw_s3_key = f"raw/twelvedata/stocks_api/extract_date={extract_date}/run_id={run_ts}/response.json"
    fact_s3_key = f"raw/twelvedata/stocks_fact/extract_date={extract_date}/run_id={run_ts}/fact.csv"
    dim_s3_key = f"raw/twelvedata/stocks_dim/extract_date={extract_date}/run_id={run_ts}/dim.csv"

    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=raw_s3_key,
        Body=json.dumps(data).encode("utf-8"),
        ContentType="application/json"
    )

    fact_csv = rows_to_csv_string(
        all_rows, 
        fieldnames=[
            "symbol",
            "price_date",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "extracted_at_utc"
        ]
    )

    dim_csv = rows_to_csv_string(
        stock_dim_rows,
        fieldnames=[
            "symbol",
            "interval",
            "currency",
            "exchange",
            "mic_code",
            "type",
            "extracted_at_utc"
        ]
    )

    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=fact_s3_key,
        Body=fact_csv.encode("utf-8"),
        ContentType="text/csv"
    )

    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=dim_s3_key,
        Body=dim_csv.encode("utf-8"),
        ContentType="text/csv"
    )

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": "Stock API raw, fact, and dim files ingested successfully",
            "raw_s3_key": raw_s3_key,
            "fact_s3_key": fact_s3_key,
            "dim_s3_key": dim_s3_key,
            "symbols": symbols,
            "start_date": start_date,
            "end_date": end_date,
            "fact_row_count": len(all_rows),
            "dim_row_count": len(stock_dim_rows)
        })
    }