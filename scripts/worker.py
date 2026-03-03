import os
import time
import logging
import joblib
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, timezone
from pymongo import MongoClient
from fmiopendata.wfs import download_stored_query
import time
# --- 1. CONFIGURATION & LOGGING ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

print(f"--- Job started at {datetime.now()} ---")
# Paths
MODEL_PATH = os.path.join("models/weather_model_1to3h.joblib")
MONGO_URI = os.getenv("MONGO_URI")

model = joblib.load(MODEL_PATH)

reliable_stations = {
    "101786": {"city": "Oulu", "name": "Oulu Airport", "lat": 64.93, "lon": 25.35},
    "101004": {"city": "Helsinki", "name": "Helsinki Kumpula", "lat": 60.20, "lon": 24.96},
    "101118": {"city": "Tampere", "name": "Tampere Airport", "lat": 61.41, "lon": 23.60},
    "101124": {"city": "Tampere", "name": "Tampere Harmala", "lat": 61.47, "lon": 23.75},
    "100949": {"city": "Turku", "name": "Turku Artukainen", "lat": 60.51, "lon": 22.20},
    "101933": {"city": "Rovaniemi", "name": "Rovaniemi Airport", "lat": 66.56, "lon": 25.83},
    "101485": {"city": "Vaasa", "name": "Vaasa Klemettila", "lat": 63.09, "lon": 21.65},
    "101462": {"city": "Vaasa", "name": "Vaasa Airport", "lat": 63.05, "lon": 21.76}
}

# --- 2. RETRIEVAL LOGIC (PHASE 1) ---
def get_fmi_data(fmisid, info):
    now = datetime.now(timezone.utc)
    start = now - timedelta(hours=6) # 6h window to ensure we get a clean 4-hour hourly series
    
    try:
        logging.info(f"Getting data from FMI for {info['name']}")
        obs = download_stored_query(
            "fmi::observations::weather::hourly::multipointcoverage",
            args=[f"fmisid={fmisid}", 
                  f"starttime={start.strftime('%Y-%m-%dT%H:%M:%SZ')}", 
                  f"endtime={now.strftime('%Y-%m-%dT%H:%M:%SZ')}"]
        )
        
        if not obs.data: return pd.DataFrame()

        rows = []
        for timestamp, stations in obs.data.items():
            for _, parameters in stations.items():
                row = {
                    'time': timestamp,
                    'station_id': fmisid,
                    'city': info['city'],
                    'lat': info['lat'],
                    'lon': info['lon']
                }
                for p_name, p_data in parameters.items():
                    row[p_name.replace(' ', '_').lower()] = p_data.get('value')
                rows.append(row)
        
        df = pd.DataFrame(rows)
        return df.rename(columns={'t2m': 'air_temperature', 'p_sea': 'air_pressure', 'rh': 'relative_humidity'})
    except Exception as e:
        logger.error(f"Error fetching {info['city']}: {e}")
        return pd.DataFrame()


def get_historical_context(station_id, db):
    """Retrieves the last 5 hours of 'actual' data from our own records."""
    cursor = db.forecasts.find(
        {"station_id": station_id},
        {"actual_data": 1, "timestamp": 1}
    ).sort("timestamp", -1).limit(5)
    
    records = []
    for doc in cursor:
        data = doc['actual_data']
        data['time'] = doc['timestamp']
        records.append(data)
    
    return pd.DataFrame(records)

def preprocess_batch(df, db):
    results = []
    for sid, group in df.groupby('station_id'):
        # 1. If FMI is missing data, try to fill from MongoDB
        if len(group) < 4:
            logger.info(f"⚠️ FMI gap detected for {sid}. Attempting DB fallback...")
            db_history = get_historical_context(sid, db)
            
            if not db_history.empty:
                # Combine FMI data with DB history, drop duplicates by time
                group = pd.concat([group, db_history]).drop_duplicates('time').sort_values('time')
        
        # 2. Final Sanity Check: If we STILL don't have enough, use ffill
        # This handles cases where a station was down for both the API and our last run
        group = group.sort_values('time').ffill().bfill()
        
        if len(group) >= 4:
            latest = group.iloc[-1].copy()
            # Calculate features using the 'healed' group
            latest['temp_roll_mean_3h'] = group['air_temperature'].tail(3).mean()
            latest['pressure_trend_3h'] = latest['air_temperature'] - group.iloc[-4]['air_temperature']
            # ... (rest of feature engineering)
            results.append(latest)
            
    return pd.DataFrame(results)

def run_worker():
    client = MongoClient(MONGO_URI)
    collection = client.weather_db.forecasts
    logger.info("🚀 Starting Master Update Cycle")

    # Step 1: Gather
    raw_dfs = []
    for fmisid, info in reliable_stations.items():
        station_df = get_fmi_data(fmisid, info)
        if not station_df.empty:
            raw_dfs.append(station_df)
            time.sleep(1)
            
    if not raw_dfs:
        logger.error("No data collected. Exiting.")
        
    # Step 2: Preprocess
    master_df = pd.concat(raw_dfs, ignore_index=True)
    clean_df = preprocess_batch(master_df, collection)
    # --- Step 3: Predict and Upload ---
    try:
        
        
        # Define current hour reference
        now_utc = datetime.now(timezone.utc)
        start_of_hour = now_utc.replace(minute=0, second=0, microsecond=0)
        
        hour_val = start_of_hour.hour
        h_sin = np.sin(2 * np.pi * hour_val / 24)
        h_cos = np.cos(2 * np.pi * hour_val / 24)
        
        features = ['lat', 'air_temperature', 'temp_roll_mean_3h', 'pressure_trend_3h', 'relative_humidity', 'hour_sin', 'hour_cos']
        
        for _, row in clean_df.iterrows():
            # 1. Update the row dictionary with the time features
            row_dict = row.to_dict()
            row_dict['hour_sin'] = h_sin
            row_dict['hour_cos'] = h_cos

            # 2. Use the row_dict (which now has all features) to create the input_vec
            # We select [features] to ensure the order matches the model training
            input_vec = pd.DataFrame([row_dict])[features]
            
            preds = model.predict(input_vec)[0]
            
            # 3. Build the document
            doc = {
                "timestamp": start_of_hour,
                "city": row['city'],
                "station_id": row['station_id'],
                "actual_data": {f: row_dict[f] for f in features}, # Save all features used
                "predictions": {
                    "1h": round(float(preds[0]), 2),
                    "2h": round(float(preds[1]), 2),
                    "3h": round(float(preds[2]), 2)
                }
            }

            # Filter logic: One unique entry per City per Hour
            filter_criteria = {
                "city": doc["city"],
                "timestamp": start_of_hour
            }

            # Upsert (Update if exists, Insert if new)
            collection.replace_one(filter_criteria, doc, upsert=True)
            logger.info(f"✅ Upserted {row['city']} (ID: {row['station_id']}) for {start_of_hour.strftime('%H:%M')}")
            
    except Exception as e:
        logger.error(f"❌ Critical Error in inference/upload: {e}")


if __name__ == "__main__":
    run_worker()