"""
Senior Data Scientist.: Dr. Eddy Giusepe Chirinos Isidro

DAG weather_dag — ETL OpenWeather → CSV → S3 (Airflow 3.x, TaskFlow API)
========================================================================
Pipeline diário que:
  1. Extrai o clima atual de Blumenau/SC via OpenWeather (geocoding + weather).
  2. Transforma o registro em CSV (1 linha por execução).
  3. Carrega no bucket S3-compatível (LocalStack local / Floci em produção).
Destino: s3://{S3_BUCKET}/weather/blumenau_YYYYMMDD_HHMMSS.csv
Agendamento: @daily | Tags: openweather, etl, floci
"""

from __future__ import annotations

import csv
import io
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

import boto3
import requests
from dotenv import load_dotenv

from airflow.decorators import dag, task

AIRFLOW_HOME = Path(__file__).resolve().parents[1]
load_dotenv(AIRFLOW_HOME / ".env")

GEOCODING_URL = "https://api.openweathermap.org/geo/1.0/direct"
WEATHER_URL = "https://api.openweathermap.org/data/2.5/weather"
S3_PREFIX = "weather"


@dag(
    dag_id="weather_dag",
    schedule="@daily",
    start_date=datetime(2026, 9, 16, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["openweather", "etl", "floci"],
)
def weather_etl():
    @task
    def extract_weather() -> dict:
        api_key = os.environ["API_KEY_OPENWEATHER"]
        geo = requests.get(
            GEOCODING_URL,
            params={"q": "Blumenau,BR", "limit": 5, "appid": api_key},
            timeout=30,
        )
        geo.raise_for_status()
        locations = geo.json()
        match = next(
            (loc for loc in locations if loc.get("state") == "Santa Catarina"),
            None,
        )
        if not match:
            raise ValueError("Geocoding não achou Blumenau, Santa Catarina")

        lat, lon = match["lat"], match["lon"]
        wx = requests.get(
            WEATHER_URL,
            params={
                "lat": lat,
                "lon": lon,
                "appid": api_key,
                "units": "metric",
                "lang": "pt_br",
            },
            timeout=30,
        )
        wx.raise_for_status()
        data = wx.json()
        return {
            "city": match.get("name", "Blumenau"),
            "state": match.get("state", "Santa Catarina"),
            "country": data["sys"]["country"],
            "lat": lat,
            "lon": lon,
            "temp_c": data["main"]["temp"],
            "feels_like_c": data["main"]["feels_like"],
            "humidity": data["main"]["humidity"],
            "description": data["weather"][0]["description"],
            "recorded_at_utc": datetime.now(timezone.utc).isoformat(),
        }

    @task
    def transform_to_csv(weather: dict) -> str:
        buf = io.StringIO()
        writer = csv.DictWriter(buf, fieldnames=list(weather.keys()))
        writer.writeheader()
        writer.writerow(weather)
        return buf.getvalue()

    @task
    def load_to_s3(csv_content: str) -> str:
        bucket = os.environ.get("S3_BUCKET", "my-s3-for-cv")
        endpoint = os.environ.get("AWS_ENDPOINT_URL", "http://172.17.0.1:4566")
        s3 = boto3.client(
            "s3",
            endpoint_url=endpoint,
            aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID", "test"),
            aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY", "test"),
            region_name=os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
        )
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        key = f"{S3_PREFIX}/blumenau_{timestamp}.csv"
        s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=csv_content.encode("utf-8"),
            ContentType="text/csv",
        )
        return f"s3://{bucket}/{key}"

    w = extract_weather()
    csv_data = transform_to_csv(w)
    load_to_s3(csv_data)


weather_etl()