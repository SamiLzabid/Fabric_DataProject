# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "5415b106-597c-4a98-906c-b8b15e7ef384",
# META       "default_lakehouse_name": "LH_bronze",
# META       "default_lakehouse_workspace_id": "fe4aac4e-7569-4ec8-b8f2-0200e2c8ff25",
# META       "known_lakehouses": [
# META         {
# META           "id": "5415b106-597c-4a98-906c-b8b15e7ef384"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql.functions import *
import requests
import pandas as pd
import time
from datetime import datetime

DATE_FROM          = "2022-01-01T00:00:00Z"
DATE_TO            = "2022-12-31T23:59:59Z"
PROVIDER_ID        = 119
COUNTRY_ID         = 155
TARGET_PARAM_IDS   = {2, 7, 10}    # pm25, o3, no2
BASE_URL           = "https://api.openaq.org/v3"
TABLE_NAME         = "AirQuality_data_22"
SLEEP_SEC          = 1.1
MAX_RETRIES        = 3

HEADERS = {
    "Accept": "application/json",
    "X-API-Key": "",
}


def throttled_get(url, retries=MAX_RETRIES):
    for attempt in range(1, retries + 1):
        try:
            resp = requests.get(url, headers=HEADERS, timeout=30)
            if resp.status_code == 429:
                wait = 30 * attempt
                print(f"    [429] Rate limited. Sleeping {wait}s (attempt {attempt})...")
                time.sleep(wait)
                continue
            if resp.status_code in (500, 502, 503):
                print(f"    [HTTP {resp.status_code}] Server error. Retrying...")
                time.sleep(5 * attempt)
                continue
            resp.raise_for_status()
            return resp
        except requests.exceptions.RequestException as e:
            print(f"    [ERROR] {e} — attempt {attempt}/{retries}")
            time.sleep(5)
    return None

def get_all_locations():
    locations, page = [], 1
    print("=" * 55)
    print("STEP 1: Fetching all AirNow US locations...")
    print("=" * 55)
    while True:
        url = (
            f"{BASE_URL}/locations"
            f"?providers_id={PROVIDER_ID}"
            f"&countries_id={COUNTRY_ID}"
            f"&limit=1000&page={page}"
            f"&order_by=id&sort_order=asc"
        )
        resp = throttled_get(url)
        time.sleep(SLEEP_SEC)

        if resp is None:
            print(f"  Failed to fetch page {page}. Stopping.")
            break

        data  = resp.json()
        batch = data.get("results", [])
        if not batch:
            break

        locations.extend(batch)
        print(f"  Page {page}: +{len(batch)} locations | Total: {len(locations)}")
        if len(batch) < 100:
            break
        page += 1

    print(f"\nTotal locations fetched: {len(locations)}\n")
    return locations

def extract_filtered_sensors(locations):
    rows = []
    skipped = 0
    for loc in locations:
        coords = loc.get("coordinates") or {}
        for s in loc.get("sensors", []):
            param_id = s["parameter"]["id"]
            if param_id not in TARGET_PARAM_IDS:
                skipped += 1
                continue
            rows.append({
                "location_id":       loc["id"],
                "location_name":     loc["name"],
                "locality":          loc.get("locality"),
                "timezone":          loc.get("timezone"),
                "latitude":          coords.get("latitude"),
                "longitude":         coords.get("longitude"),
                "sensor_id":         s["id"],
                "parameter_id":      s["parameter"]["id"],
                "parameter_name":    s["parameter"]["name"],
                "parameter_units":   s["parameter"]["units"],
                "parameter_display": s["parameter"].get("displayName"),
            })

    print("=" * 55)
    print("STEP 2: Sensor filtering results")
    print("=" * 55)
    print(f"  Sensors kept  (param IDs {TARGET_PARAM_IDS}): {len(rows)}")
    print(f"  Sensors skipped (other params)            : {skipped}")
    est_mins = round((len(rows) * SLEEP_SEC) / 60, 1)
    print(f"  Estimated runtime @ 60 req/min            : ~{est_mins} min\n")
    return rows

def get_sensor_days(sensor_id):
    records, page = [], 1
    while True:
        url = (
            f"{BASE_URL}/sensors/{sensor_id}/days"
            f"?date_from={DATE_FROM}"
            f"&date_to={DATE_TO}"
            f"&limit=1000&page={page}"
        )
        resp = throttled_get(url)
        time.sleep(SLEEP_SEC)

        if resp is None:
            print(f"    Skipping sensor {sensor_id}: all retries failed.")
            break

        data  = resp.json()
        batch = data.get("results", [])
        if not batch:
            break

        records.extend(batch)
        if len(batch) < 1000:
            break
        page += 1

    return records

def flatten_records(sensor_meta, raw_records):
    rows = []
    for r in raw_records:
        period = r.get("period", {})

        dt_utc = period.get("datetimeFrom", {}).get("utc", "")
        date   = dt_utc[:10] if dt_utc else None  

        rows.append({
            "location_id":       sensor_meta["location_id"],
            "location_name":     sensor_meta["location_name"],
            "locality":          sensor_meta["locality"],
            "timezone":          sensor_meta["timezone"],
            "latitude":          sensor_meta["latitude"],
            "longitude":         sensor_meta["longitude"],
            "sensor_id":         sensor_meta["sensor_id"],
            "parameter_id":      sensor_meta["parameter_id"],
            "parameter_name":    sensor_meta["parameter_name"],
            "parameter_units":   sensor_meta["parameter_units"],
            "parameter_display": sensor_meta["parameter_display"],
            "date":              date,
            "value":         r.get("value"),
        })
    return rows

locations   = get_all_locations()
sensor_meta = extract_filtered_sensors(locations)

all_rows, errors = [], []
total            = len(sensor_meta)

print("=" * 55)
print("STEP 3: Fetching daily sensor data...")
print("=" * 55)

for i, smeta in enumerate(sensor_meta):
    sid = smeta["sensor_id"]
    try:
        raw = get_sensor_days(sid)
        if raw:
            all_rows.extend(flatten_records(smeta, raw))

        if (i + 1) % 50 == 0 or (i + 1) == total:
            print(
                f"  [{i+1}/{total}] "
                f"Sensor {sid} | "
                f"param: {smeta['parameter_name']:5s} | "
                f"rows: {len(raw):4d} | "
                f"total: {len(all_rows)}"
            )
    except Exception as e:
        errors.append({"sensor_id": sid, "error": str(e)})
        print(f"  [ERROR] Sensor {sid}: {e}")

print(f"\nCollection complete.")
print(f"  Total rows : {len(all_rows)}")
print(f"  Errors     : {len(errors)}")

print("\n" + "=" * 55)
print("STEP 4: Writing to OneLake Delta Table...")
print("=" * 55)

df_pd    = pd.DataFrame(all_rows)
df_spark = spark.createDataFrame(df_pd)

(
    df_spark.write
    .format("delta")
    .mode("overwrite")          
    .option("overwriteSchema", "true")
    .saveAsTable(TABLE_NAME)
)

print(f"Table '{TABLE_NAME}' written to OneLake successfully.")

if errors:
    df_err = spark.createDataFrame(pd.DataFrame(errors))
    df_err.write.format("delta").mode("overwrite") \
          .saveAsTable(f"{TABLE_NAME}_errors")
    print(f"{len(errors)} errors logged to '{TABLE_NAME}_errors'")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

!pip install openmeteo_requests
!pip install requests_cache
!pip install retry_requests

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import openmeteo_requests
import pandas as pd
import requests_cache
from retry_requests import retry

cache_session = requests_cache.CachedSession('.cache', expire_after = -1)
retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
openmeteo = openmeteo_requests.Client(session = retry_session)

zones_config = [
    {"ZoneName": "Manhattan", "lat": 40.7580, "lon": -73.9855},
    {"ZoneName": "Queens", "lat": 40.6413, "lon": -73.7781},
    {"ZoneName": "Brooklyn", "lat": 40.6782, "lon": -73.9442},
    {"ZoneName": "Bronx", "lat": 40.8448, "lon": -73.8648},
    {"ZoneName": "Staten Island", "lat": 40.5795, "lon": -74.1502},
    {"ZoneName": "EWR", "lat": 40.6895, "lon": -74.1745}
]

url = "https://archive-api.open-meteo.com/v1/archive"
params = {
	"latitude": [z["lat"] for z in zones_config],   
	"longitude": [z["lon"] for z in zones_config], 
	"start_date": "2023-01-01",                    
	"end_date": "2026-05-14",                     
	"hourly": ["temperature_2m", "relative_humidity_2m", "precipitation", "wind_speed_10m", "rain", "snowfall", "wind_direction_10m"],
	"timezone": "America/New_York",
}

responses = openmeteo.weather_api(url, params = params)


all_zones_data = []

for i, response in enumerate(responses):
    zone_meta = zones_config[i]
    print(f"Processing weather timeline for Zone: {zone_meta['ZoneName']}")
    
    hourly = response.Hourly()

    hourly_data = {
        "date": pd.date_range(
            start = pd.to_datetime(hourly.Time(), unit = "s", utc = True),
            end =  pd.to_datetime(hourly.TimeEnd(), unit = "s", utc = True),
            freq = pd.Timedelta(seconds = hourly.Interval()),
            inclusive = "left"
        ).tz_convert(response.Timezone().decode()),
        
        "ZoneName": zone_meta["ZoneName"], 
        "temperature_2m": hourly.Variables(0).ValuesAsNumpy(),
        "relative_humidity_2m": hourly.Variables(1).ValuesAsNumpy(),
        "precipitation": hourly.Variables(2).ValuesAsNumpy(),
        "wind_speed_10m": hourly.Variables(3).ValuesAsNumpy(),
        "rain": hourly.Variables(4).ValuesAsNumpy(),
        "snowfall": hourly.Variables(5).ValuesAsNumpy(),
        "wind_direction_10m": hourly.Variables(6).ValuesAsNumpy()
    }

    zone_df = pd.DataFrame(data = hourly_data)
    all_zones_data.append(zone_df)

master_weather_df = pd.concat(all_zones_data, ignore_index=True)

spark_weather_df = spark.createDataFrame(master_weather_df)
display(spark_weather_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(spark_weather_df.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark_weather_df.write.format("delta").mode("append").saveAsTable("WeatherData")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
