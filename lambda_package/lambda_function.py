import json
import os
import boto3
import requests
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

def lambda_handler(event, context):

    symbols = event.get("symbols", ["AAPL","GOOG","NVDA"])
    start_date = event.get("start_date", "2026-03-01")
    end_date = event.get("end_date", "2026-03-31")
    
    api_key = get_api_key()
    url = "https://api.twelvedata.com/time_series"
    params = {
        "interval": "1day",
        "start_date": start_date,
        "end_date": end_date,
        "symbol": ",".join(symbols)
    }
    headers = {
        "Authorization": f"apikey {api_key}"
    }

    response = requests.get(url, params=params, headers=headers, timeout=30)
    response.raise_for_status()
    data = response.json()

    run_ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    extract_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    s3_key = f"raw/twelvedata/stocks_api/extract_date={extract_date}/run_id={run_ts}/response.json"

    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=s3_key,
        Body=json.dumps(data).encode("utf-8"),
        ContentType="application/json"
    )

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": "Stock API Data ingested successfully",
            "s3_key": s3_key,
            "symbols": symbols,
            "start_date": start_date,
            "end_date": end_date
        })
    }