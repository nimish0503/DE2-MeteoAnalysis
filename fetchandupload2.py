import requests
import pandas as pd
import os
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/Users/nimishmathur/Desktop/WIKIPEDIA-PIPELINE/Streamlit.json"
from google.cloud import storage
from google.cloud import pubsub_v1
import datetime

# --- Parameters ---
latitude = 49.4875   # Mannheim, Germany
longitude = 8.4660
today = datetime.date.today()
start_date = today.replace(month=1, day=1).isoformat()
end_date = (today - datetime.timedelta(days=1)).isoformat()  # up to yesterday
#end_date = today.isoformat()

variables = [
    "temperature_2m",
    "soil_temperature_0_to_7cm",
    "soil_moisture_0_to_7cm",
    "dew_point_2m",
    "relative_humidity_2m"
]
BUCKET_NAME = 'weather-data-nimish'
CSV_FILENAME = "weather_backup.csv"
PUBSUB_PROJECT_ID = "wikipedia-462519"
PUBSUB_TOPIC_ID = "weather-demo-topic"

# --- 1. Fetch Data ---
url = (
    "https://archive-api.open-meteo.com/v1/era5?"
    f"latitude={latitude}&longitude={longitude}"
    f"&start_date={start_date}&end_date={end_date}"
    f"&hourly={','.join(variables)}"
    "&timezone=Europe/Berlin"
)
response = requests.get(url)
data = response.json()["hourly"]
df = pd.DataFrame(data)

# --- REMOVE EMPTY ROWS ---
df = df.dropna(
    subset=["temperature_2m", "soil_temperature_0_to_7cm", "soil_moisture_0_to_7cm", "dew_point_2m", "relative_humidity_2m"],
    how='all'
)

df.to_csv(CSV_FILENAME, index=False)
print(f"Data saved to {CSV_FILENAME}")

# --- 2. Upload to GCS ---
client = storage.Client()
bucket = client.bucket(BUCKET_NAME)
blob = bucket.blob(CSV_FILENAME)
blob.upload_from_filename(CSV_FILENAME)
print(f"File {CSV_FILENAME} uploaded to gs://{BUCKET_NAME}/{CSV_FILENAME}")

# --- 3. Publish to Pub/Sub ---
try:
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(PUBSUB_PROJECT_ID, PUBSUB_TOPIC_ID)
    for i, row in df.head(24).iterrows():
        msg = f"Weather event {i}: Temp {row['temperature_2m']}C"
        publisher.publish(topic_path, msg.encode("utf-8"))
        print(f"Published: {msg}")
except Exception as e:
    print("Skipping Pub/Sub publish. Error:", e)
