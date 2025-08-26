from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from datetime import timedelta, datetime
import requests
import pandas as pd
import psycopg2


LAT = 6.5244
LON = 3.3792
API_KEY = "Your API Key here"

## Current Weather
WEATHER_URL = f"https://api.openweathermap.org/data/2.5/weather?lat={LAT}&lon={LON}&appid={API_KEY}&units=metric"

## Air Pollution (AQI)
AQI_URL = f"https://api.openweathermap.org/data/2.5/air_pollution?lat={LAT}&lon={LON}&appid={API_KEY}"

# AQI Category Mapping
AQI_CATEGORIES = {
    1: "Good",
    2: "Fair",
    3: "Moderate",
    4: "Poor",
    5: "Very Poor"
}

# PostgreSQL connection config
DB_CONFIG = {
    'host': 'localhost',
    'port': '5432',
    'dbname': 'air_quality_db',
    'user': 'postgres',
    'password': 'Password#123'
}

# ========== DAG DEFINITION ========== #
@dag(
    dag_id="iot_air_quality_etl",
    schedule_interval=timedelta(minutes=5),  # ⏱️ Change this to hourly or seconds as needed
    start_date=days_ago(1),
    catchup=False,
    tags=["iot", "airquality", "etl"]
)

def air_quality_etl():

    @task()
    def extract():
        """Fetch and combine weather + AQI data into a single record."""
        try:
            weather = requests.get(WEATHER_URL).json()
            aqi = requests.get(AQI_URL).json()

            data = aqi["list"][0]
            weather_data = {
                "timestamp": datetime.utcnow().isoformat(),
                "temperature": weather["main"]["temp"],
                "humidity": weather["main"]["humidity"],
                "weather": weather["weather"][0]["description"],
                "aqi_index": data["main"]["aqi"],
                "pm2_5": data["components"]["pm2_5"],
                "pm10": data["components"]["pm10"],
                "no2": data["components"]["no2"],
                "o3": data["components"]["o3"],
                "co": data["components"]["co"]
            }
            return [weather_data]  # Return as list for transform
        except Exception as e:
            raise ValueError(f"❌ Extract failed: {e}")

    @task()
    def transform(raw_data):
        """Clean and enrich data: categorize AQI, detect spikes."""
        df = pd.DataFrame(raw_data)
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df['aqi_category'] = df['aqi_index'].map(AQI_CATEGORIES)
        df = df.sort_values(by='timestamp')
        df['aqi_change'] = df['aqi_index'].diff().fillna(0)
        df['spike_detected'] = df['aqi_change'].abs() >= 2
        alerts_df = df[df['spike_detected']][['timestamp', 'aqi_index', 'aqi_change', 'aqi_category']]
        return {
            "readings": df.to_dict(orient='records'),
            "alerts": alerts_df.to_dict(orient='records')
        }

    @task()
    def load(data_dict):
        """Load readings and alerts into PostgreSQL using psycopg2."""
        try:
            conn = psycopg2.connect(**DB_CONFIG)
            cur = conn.cursor()

            # Create tables if they don't exist
            cur.execute("""
                CREATE TABLE IF NOT EXISTS sensor_readings (
                    id SERIAL PRIMARY KEY,
                    timestamp TIMESTAMP,
                    temperature FLOAT,
                    humidity FLOAT,
                    weather TEXT,
                    aqi_index INT,
                    pm2_5 FLOAT,
                    pm10 FLOAT,
                    no2 FLOAT,
                    o3 FLOAT,
                    co FLOAT,
                    aqi_category TEXT,
                    aqi_change FLOAT,
                    spike_detected BOOLEAN
                );
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS aqi_alerts (
                    id SERIAL PRIMARY KEY,
                    timestamp TIMESTAMP,
                    aqi_index INT,
                    aqi_change FLOAT,
                    aqi_category TEXT
                );
            """)

            for row in data_dict["readings"]:
                cur.execute("""
                    INSERT INTO sensor_readings (
                        timestamp, temperature, humidity, weather,
                        aqi_index, pm2_5, pm10, no2, o3, co,
                        aqi_category, aqi_change, spike_detected
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    row['timestamp'], row['temperature'], row['humidity'], row['weather'],
                    row['aqi_index'], row['pm2_5'], row['pm10'], row['no2'],
                    row['o3'], row['co'], row['aqi_category'],
                    row['aqi_change'], row['spike_detected']
                ))

            for alert in data_dict["alerts"]:
                cur.execute("""
                    INSERT INTO aqi_alerts (
                        timestamp, aqi_index, aqi_change, aqi_category
                    ) VALUES (%s, %s, %s, %s)
                """, (
                    alert['timestamp'], alert['aqi_index'],
                    alert['aqi_change'], alert['aqi_category']
                ))

            conn.commit()
            cur.close()
            conn.close()
            print("✅ Load complete.")

        except Exception as e:
            raise ValueError(f"❌ Load failed: {e}")

    # DAG task execution order
    raw = extract()
    transformed = transform(raw)
    load(transformed)

# Register the DAG
air_quality_etl()
