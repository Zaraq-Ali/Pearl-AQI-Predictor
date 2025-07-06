import requests
import pandas as pd
from datetime import datetime, timedelta
import os
import json

def fetch_data(city: str, date: str) -> dict:
    # Coordinates for Delhi
    lat, lon = 33.7373, 72.8006
    
    # Air quality endpoint
    url = (
        f"https://air-quality-api.open-meteo.com/v1/air-quality?"
        f"latitude={lat}&longitude={lon}"
        f"&hourly=pm10,pm2_5,carbon_monoxide,nitrogen_dioxide,ozone"
        f"&start_date={date}&end_date={date}&timezone=auto"
    )

    response = requests.get(url)
    print("API URL:", url)
    print("Status Code:", response.status_code)
    print("DEBUG RAW DATA:", json.dumps(response.json(), indent=2))
    return response.json()

def compute_features(raw: dict) -> pd.DataFrame:
    if 'hourly' not in raw:
        raise ValueError(f"Missing 'hourly' key in API response. Got keys: {list(raw.keys())}")
    
    df = pd.DataFrame(raw['hourly'])
    df['datetime'] = pd.to_datetime(df['time'])
    df.set_index('datetime', inplace=True)
    df.drop(columns=['time'], inplace=True)
    
    # You can define AQI from available fields, or just use pm2_5 for now
    df['aqi'] = df['pm2_5']  # simple proxy
    return df


def save_to_feature_store(df: pd.DataFrame, date: str):
    os.makedirs("feature_store", exist_ok=True)
    df.to_parquet(f"feature_store/features_{date}.parquet")

if __name__ == "__main__":
    today = datetime.utcnow().strftime('%Y-%m-%d')
    raw = fetch_data("Delhi", today)
    features = compute_features(raw)
    save_to_feature_store(features, today)
