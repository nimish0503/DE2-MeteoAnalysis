import streamlit as st
import pandas as pd
import io

# --- Import Handling (ONLY CHANGE MADE) ---
try:
    import matplotlib.pyplot as plt
    from google.cloud import storage
except ImportError as e:
    st.error(f"Critical dependency error: {str(e)}")
    st.stop()  # Halt if imports fail

# --- REST IS IDENTICAL TO YOUR ORIGINAL CODE ---
st.set_page_config(page_title="🌦️ Weather Data Dashboard", layout="wide")
st.title("🌤️ Smart Weather Data Dashboard")

# --- GCP BUCKET SETTINGS ---
BUCKET_NAME = "weather-data-nimish"
BLOB_NAME = "weather_backup.csv"

@st.cache_data(show_spinner=True)
def load_gcs_csv(bucket_name, blob_name):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    data = blob.download_as_bytes()
    df = pd.read_csv(io.BytesIO(data))
    return df

st.sidebar.header("Load Data")
use_gcs = st.sidebar.checkbox("Load from Google Cloud Storage", value=True)

if "df" not in st.session_state:
    st.session_state.df = None

if use_gcs:
    if st.sidebar.button("Load latest from GCS"):
        try:
            st.session_state.df = load_gcs_csv(BUCKET_NAME, BLOB_NAME)
            st.success("✅ Data loaded from GCS!")
        except Exception as e:
            st.error(f"Failed to load from GCS: {e}")
else:
    uploaded_file = st.file_uploader("Upload your weather CSV", type=["csv"])
    if uploaded_file is not None:
        st.session_state.df = pd.read_csv(uploaded_file)
        st.success("✅ Data loaded from local upload!")

df = st.session_state.df

if df is not None:
    st.subheader("Raw Weather Data")
    st.dataframe(df.head(), use_container_width=True)

    if "time" in df.columns:
        df["time"] = pd.to_datetime(df["time"])
        df = df.sort_values("time")

        df_kpi = df.dropna(subset=["temperature_2m", "soil_moisture_0_to_7cm"])
        if len(df_kpi) == 0:
            st.warning("No complete row found for KPIs! Check your data for missing values.")
            latest = df.iloc[-1]
        else:
            latest = df_kpi.iloc[-1]

        df["temp_rolling_24h"] = df["temperature_2m"].rolling(window=24, min_periods=1).mean()

        kpi1, kpi2, kpi3 = st.columns(3)
        kpi1.metric(label="🌡️ Latest Temperature", value=f"{latest['temperature_2m']:.1f} °C")
        kpi2.metric(label="💧 Latest Soil Moisture", value=f"{latest['soil_moisture_0_to_7cm']:.2f}")
        kpi3.metric(label="📆 Records Ingested", value=f"{len(df)}")

        st.subheader("Temperature Trend (with 24h rolling avg)")
        st.line_chart(df.set_index("time")[["temperature_2m", "temp_rolling_24h"]])

        st.subheader("Soil Moisture Trend")
        st.area_chart(df.set_index("time")["soil_moisture_0_to_7cm"])

        col1, col2 = st.columns([2, 1])
        with col1:
            st.subheader("📊 Compare Metrics")
            metric_option = st.selectbox(
                "Select metric to plot:",
                options=[
                    "temperature_2m",
                    "soil_temperature_0_to_7cm",
                    "soil_moisture_0_to_7cm",
                    "dew_point_2m",
                    "relative_humidity_2m",
                ],
                index=0,
                key="metric_select"
            )
            st.line_chart(df.set_index("time")[metric_option])

        with col2:
            st.subheader("Latest Row Details")
            st.dataframe(latest.to_frame(), use_container_width=True)

        st.markdown("### Recent Weather Events Table")
        st.dataframe(df.tail(20), use_container_width=True)

        st.markdown("#### 📉 Temperature & Soil Moisture (Dual Axis)")
        fig, ax1 = plt.subplots()
        ax2 = ax1.twinx()
        ax1.plot(df["time"], df["temperature_2m"], color='tab:blue', marker='o', label="Temperature (C)")
        ax2.plot(df["time"], df["soil_moisture_0_to_7cm"], color='tab:orange', marker='s', label="Soil Moisture")
        ax1.set_xlabel("Time")
        ax1.set_ylabel("Temperature (C)", color='tab:blue')
        ax2.set_ylabel("Soil Moisture", color='tab:orange')
        plt.title("Temperature & Soil Moisture Over Time")
        fig.tight_layout()
        st.pyplot(fig)
    else:
        st.warning("No 'time' column found! Please upload a valid weather CSV or check your GCS file.")
else:
    st.info("⬆️ Please upload or load weather data to see tables and visualizations.")

st.caption("Dashboard created for weather")
