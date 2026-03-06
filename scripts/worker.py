import os
import logging
import joblib
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, timezone
from pymongo import MongoClient, UpdateOne
from fmiopendata.wfs import download_stored_query
from concurrent.futures import ThreadPoolExecutor, as_completed

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# ── Constants ──────────────────────────────────────────────
MONGO_URI   = os.getenv("MONGO_URI")
FETCH_HOURS = 8

PARAMETERS_HOURLY = "TA_PT1H_AVG,RH_PT1H_AVG,WS_PT1H_AVG,WS_PT1H_MAX,WD_PT1H_AVG,PA_PT1H_AVG,WAWA_PT1H_RANK"
PARAMETERS_10MIN  = "n_man,td"

PARAM_MAP_HOURLY = {
    "air temperature":        "temp",
    "relative humidity":      "humidity",
    "wind speed":             "wind_speed",
    "maximum wind speed":     "wind_speed_max",
    "wind direction":         "wind_dir",
    "air pressure":           "pressure_sea",
    "present weather (auto)": "present_weather_(auto)",
}

PARAM_MAP_10MIN = {
    "cloud amount":          "cloud_cover",
    "dew-point temperature": "dew_point",
}

COASTAL_DISTANCES = {
    "Oulu": 0, "Helsinki": 0, "Turku": 0, "Vaasa": 0,
    "Tampere": 130, "Rovaniemi": 100
}

CITY_NAMES = {
    "Oulu": "oulu", "Helsinki": "helsinki", "Tampere": "tampere",
    "Turku": "turku", "Rovaniemi": "rovaniemi", "Vaasa": "vaasa"
}

reliable_stations = {
    "101786": {"city": "Oulu",      "name": "Oulu Airport",      "lat": 64.93, "lon": 25.35},
    "108040": {"city": "Oulu",      "name": "Oulu Kaukovainio",  "lat": 65.01, "lon": 25.47},
    "101004": {"city": "Helsinki",  "name": "Helsinki Kumpula",  "lat": 60.20, "lon": 24.96},
    "101118": {"city": "Tampere",   "name": "Tampere Airport",   "lat": 61.41, "lon": 23.60},
    "101124": {"city": "Tampere",   "name": "Tampere Harmala",   "lat": 61.47, "lon": 23.75},
    "100949": {"city": "Turku",     "name": "Turku Artukainen",  "lat": 60.51, "lon": 22.20},
    "101933": {"city": "Rovaniemi", "name": "Rovaniemi Apukka",  "lat": 66.56, "lon": 25.83},
    "137190": {"city": "Rovaniemi", "name": "Rovaniemi Airport", "lat": 66.56, "lon": 25.83},
    "101485": {"city": "Vaasa",     "name": "Vaasa Klemettila",  "lat": 63.09, "lon": 21.65},
    "101462": {"city": "Vaasa",     "name": "Vaasa Airport",     "lat": 63.05, "lon": 21.76},
}

TEMP_FEATURES = [
    "air_temperature", "relative_humidity", "wind_speed",
    "air_pressure", "dew_point",
    "u_wind", "v_wind",
    "hour_sin", "hour_cos", "day_sin", "day_cos",
    "lat", "lon", "coastal_dist"
]

CODE_FEATURES = [
    "temp", "humidity", "pressure_sea",
    "wind_speed_max", "u_wind", "v_wind",
    "cloud_cover", "code",
    "pressure_change_1h", "pressure_change_3h", "pressure_change_6h",
    "day_sin", "day_cos",
]


# Load model
def load_models():
    temp_models = {
        col: joblib.load(f"models/temp_model_{col}.joblib")
        for col in ["target_6h", "target_12h", "target_24h"]
    }
    code_models = {
        col: joblib.load(f"models/code_model_{col}.joblib")
        for col in ["target_3h", "target_6h", "target_12h"]
    }
    logger.info("Models loaded (6 total)")
    return temp_models, code_models

# fetch present data
def fetch_station_hourly(fmisid, info, start, end):
    try:
        obs = download_stored_query(
            "fmi::observations::weather::hourly::multipointcoverage",
            args=[
                f"fmisid={fmisid}",
                f"starttime={start.strftime('%Y-%m-%dT%H:%M:%SZ')}",
                f"endtime={end.strftime('%Y-%m-%dT%H:%M:%SZ')}",
                f"parameters={PARAMETERS_HOURLY}",
            ]
        )
        if not obs.data:
            return fmisid, None

        rows = []
        for timestamp, stations in obs.data.items():
            for _, params in stations.items():
                row = {
                    "time":   timestamp,
                    "city":   info["city"],
                    "fmisid": fmisid,
                    "lat":    info["lat"],
                    "lon":    info["lon"],
                }
                for p_name, p_data in params.items():
                    col = PARAM_MAP_HOURLY.get(p_name.lower(), p_name.replace(" ", "_").lower())
                    row[col] = p_data.get("value")
                rows.append(row)

        return fmisid, pd.DataFrame(rows)

    except Exception as e:
        logger.error(f"Hourly fetch error {info['name']}: {e}")
        return fmisid, None


def fetch_station_10min(fmisid, info, start, end):
    try:
        obs = download_stored_query(
            "fmi::observations::weather::multipointcoverage",
            args=[
                f"fmisid={fmisid}",
                f"starttime={start.strftime('%Y-%m-%dT%H:%M:%SZ')}",
                f"endtime={end.strftime('%Y-%m-%dT%H:%M:%SZ')}",
                f"parameters={PARAMETERS_10MIN}",
            ]
        )
        if not obs.data:
            return fmisid, None

        rows = []
        for timestamp, stations in obs.data.items():
            for _, params in stations.items():
                row = {"time": timestamp, "fmisid": fmisid, "city": info["city"]}
                for p_name, p_data in params.items():
                    col = PARAM_MAP_10MIN.get(p_name.lower(), p_name.replace(" ", "_").lower())
                    row[col] = p_data.get("value")
                rows.append(row)

        df = pd.DataFrame(rows)
        df["time"] = pd.to_datetime(df["time"]).dt.floor("h")

        POSSIBLE_COLS = {"cloud_cover": "mean", "dew_point": "mean"}
        agg_cols = {col: agg for col, agg in POSSIBLE_COLS.items() if col in df.columns}
        agg_cols["city"] = "first"

        return fmisid, df.groupby(["fmisid", "time"]).agg(agg_cols).reset_index()

    except Exception as e:
        logger.error(f"10min fetch error {info['name']}: {e}")
        return fmisid, None


def fetch_all(start, end):
    hourly_dfs, min10_dfs = [], []

    # Increase workers to match total requests (10 stations × 2 endpoints = 20)
    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = {}
        for fmisid, info in reliable_stations.items():
            futures[executor.submit(fetch_station_hourly, fmisid, info, start, end)] = ("hourly", fmisid, info)
            futures[executor.submit(fetch_station_10min,  fmisid, info, start, end)] = ("10min",  fmisid, info)

        for future in as_completed(futures):
            source, fmisid, info = futures[future]
            _, df = future.result()
            if df is not None:
                if source == "hourly": hourly_dfs.append(df)
                else:                  min10_dfs.append(df)

    if not hourly_dfs:
        raise RuntimeError("No hourly data fetched — aborting")

    df_fmi = pd.concat(hourly_dfs, ignore_index=True)
    df_fmi["time"] = pd.to_datetime(df_fmi["time"]).dt.floor("h")

    if min10_dfs:
        df_10min = pd.concat(min10_dfs, ignore_index=True)
        for col in ["cloud_cover", "dew_point"]:
            if col not in df_10min.columns:
                df_10min[col] = np.nan

        df_fmi = df_fmi.merge(
            df_10min[["fmisid", "time", "cloud_cover", "dew_point"]],
            on=["fmisid", "time"], how="left"
        )

    logger.info(f"Fetched {len(df_fmi)} station-hour rows")
    return df_fmi


# FMI forecast
def derive_fmi_code(precip, temp):
    if precip is None or precip < 0.1:
        return 0   # Clear
    elif temp is not None and temp <= 0:
        return 2   # Snow
    else:
        return 1   # Rain


def fetch_fmi_forecast(city, now):
    end = now + timedelta(hours=24)

    try:
        obs = download_stored_query(
            "fmi::forecast::harmonie::surface::point::multipointcoverage",
            args=[
                f"place={CITY_NAMES[city]}",
                f"starttime={now.strftime('%Y-%m-%dT%H:%M:%SZ')}",
                f"endtime={end.strftime('%Y-%m-%dT%H:%M:%SZ')}",
                "timestep=60",
            ]
        )

        if not obs.data:
            return {}

        rows = []
        for timestamp, locations in obs.data.items():
            for _, params in locations.items():
                rows.append({
                    "time":        timestamp,
                    "fmi_temp":    params.get("Air temperature",     {}).get("value"),
                    "fmi_humidity": params.get("Humidity",           {}).get("value"),
                    "fmi_precip":  params.get("Precipitation amount",{}).get("value"),
                    "fmi_wind":    params.get("Wind speed",          {}).get("value"),
                    "fmi_clouds":  params.get("Total cloud cover",   {}).get("value"),
                })

        df = pd.DataFrame(rows).sort_values("time").reset_index(drop=True)

        result = {}
        for h in [6, 12, 24]:
            target_time = (now + timedelta(hours=h)).replace(tzinfo=None)
            match = df[df["time"] == target_time]
            if not match.empty:
                r = match.iloc[0]
                temp   = round(float(r["fmi_temp"]),   2) if pd.notna(r["fmi_temp"])   else None
                precip = round(float(r["fmi_precip"]), 2) if pd.notna(r["fmi_precip"]) else None
                result[f"{h}h"] = {
                    "temp":     temp,
                    "humidity": round(float(r["fmi_humidity"]), 2) if pd.notna(r["fmi_humidity"]) else None,
                    "precip":   precip,
                    "wind":     round(float(r["fmi_wind"]),     2) if pd.notna(r["fmi_wind"])     else None,
                    "clouds":   round(float(r["fmi_clouds"]),   2) if pd.notna(r["fmi_clouds"])   else None,
                    "code":     derive_fmi_code(precip, temp),
                }

        return result

    except Exception as e:
        logger.error(f"FMI forecast failed for {city}: {e}")
        return {}


def aggregate_cities(df_raw):
    df_raw["u_wind"] = df_raw["wind_speed"] * np.sin(np.radians(df_raw["wind_dir"]))
    df_raw["v_wind"] = df_raw["wind_speed"] * np.cos(np.radians(df_raw["wind_dir"]))
    df_raw = df_raw.sort_values(["city", "time", "fmisid"])

    agg_dict = {
        "lat":                    "first",
        "lon":                    "first",
        "temp":                   "mean",
        "humidity":               "mean",
        "pressure_sea":           "mean",
        "wind_speed":             "mean",
        "wind_speed_max":         "max",
        "u_wind":                 "mean",
        "v_wind":                 "mean",
        "cloud_cover":            "mean",
        "dew_point":              "mean",
        "present_weather_(auto)": "first",
    }
    agg_dict = {k: v for k, v in agg_dict.items() if k in df_raw.columns}

    df_city = df_raw.groupby(["city", "time"]).agg(agg_dict).reset_index()

    # Fill sensor gaps (up to 3 consecutive missing hours)
    fill_cols = [c for c in ["temp", "humidity", "pressure_sea", "wind_speed",
                              "wind_speed_max", "u_wind", "v_wind",
                              "cloud_cover", "dew_point"] if c in df_city.columns]
    df_city = df_city.sort_values(["city", "time"])
    df_city[fill_cols] = df_city.groupby("city")[fill_cols].ffill(limit=3)

    return df_city



def engineer_features(df):
    df = df.sort_values(["city", "time"])

    df["pressure_change_1h"] = df.groupby("city")["pressure_sea"].diff(1)
    df["pressure_change_3h"] = df.groupby("city")["pressure_sea"].diff(3)
    df["pressure_change_6h"] = df.groupby("city")["pressure_sea"].diff(6)

    df["hour"]     = df["time"].dt.hour
    df["day"]      = df["time"].dt.day_of_year
    df["hour_sin"] = np.sin(2 * np.pi * df["hour"] / 24)
    df["hour_cos"] = np.cos(2 * np.pi * df["hour"] / 24)
    df["day_sin"]  = np.sin(2 * np.pi * df["day"] / 365.25)
    df["day_cos"]  = np.cos(2 * np.pi * df["day"] / 365.25)

    df["coastal_dist"] = df["city"].map(COASTAL_DISTANCES)

    def map_code(c):
        if pd.isna(c):  return 0
        if c <= 49:     return 0
        elif c <= 69:   return 1
        elif c <= 89:   return 2
        return 0

    df["code"] = df["present_weather_(auto)"].apply(map_code)

    # Aliases for temp model compatibility
    df["air_temperature"]   = df["temp"]
    df["relative_humidity"] = df["humidity"]
    df["air_pressure"]      = df["pressure_sea"]

    return df


def run_worker():
    logger.info(f"--- Job started at {datetime.now()} ---")

    temp_models, code_models = load_models()

    client     = MongoClient(MONGO_URI)
    collection = client.weather_db.forecastV2

    now   = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    start = now - timedelta(hours=FETCH_HOURS)

    # Fetch observations
    logger.info("Fetching FMI observations...")
    df_raw = fetch_all(start, now)

    # PreProcess
    df_city  = aggregate_cities(df_raw)
    df       = engineer_features(df_city)
    latest   = df.sort_values("time").groupby("city").last().reset_index()

    # Predict + fetch Harmonie forecasts in parallel
    logger.info("Running predictions + fetching Harmonie forecasts...")

    def process_city(row):
        city = row["city"]
        try:
            temp_input = pd.DataFrame([row])[TEMP_FEATURES]
            temp_preds = {
                col: round(float(temp_models[col].predict(temp_input)[0]), 2)
                for col in temp_models
            }

            code_input = pd.DataFrame([row])[CODE_FEATURES]
            code_preds = {
                col: int(code_models[col].predict(code_input)[0])
                for col in code_models
            }

            fmi_forecast = fetch_fmi_forecast(city, now)

            return city, {
                "timestamp": now,
                "city":      city,
                "current": {
                    "temp":     round(float(row["temp"]), 2),
                    "humidity": round(float(row["humidity"]), 2),
                    "pressure": round(float(row["pressure_sea"]), 2),
                    "code":     int(row["code"]),
                },
                "temp_forecast": {
                    "6h":  temp_preds.get("target_6h"),
                    "12h": temp_preds.get("target_12h"),
                    "24h": temp_preds.get("target_24h"),
                },
                "code_forecast": {
                    "3h":  code_preds.get("target_3h"),
                    "6h":  code_preds.get("target_6h"),
                    "12h": code_preds.get("target_12h"),
                },
                "fmi_forecast": fmi_forecast,
            }

        except Exception as e:
            logger.error(f" {city} failed: {e}")
            return city, None

    # Run all 6 cities in parallel (prediction is fast, Harmonie fetch benefits most)
    rows = [row for _, row in latest.iterrows()]
    with ThreadPoolExecutor(max_workers=6) as executor:
        results = list(executor.map(process_city, rows))

    # Bulk upsert to MongoDB
    ops = [
        UpdateOne({"city": city, "timestamp": now}, {"$set": doc}, upsert=True)
        for city, doc in results if doc is not None
    ]
    if ops:
        collection.bulk_write(ops, ordered=False)

    succeeded = sum(1 for _, doc in results if doc is not None)
    logger.info(f"Predictions complete — {succeeded}/{len(rows)} cities uploaded")

    client.close()
    logger.info("--- Job complete ---")


if __name__ == "__main__":
    run_worker()