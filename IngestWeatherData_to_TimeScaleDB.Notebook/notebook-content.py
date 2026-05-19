# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "3dbb6804-e902-4cdd-88e5-8a95e6986c24",
# META       "default_lakehouse_name": "LH_Silver",
# META       "default_lakehouse_workspace_id": "fe4aac4e-7569-4ec8-b8f2-0200e2c8ff25",
# META       "known_lakehouses": [
# META         {
# META           "id": "3dbb6804-e902-4cdd-88e5-8a95e6986c24"
# META         }
# META       ]
# META     },
# META     "environment": {
# META       "environmentId": "06ef6ce1-c33f-9930-4509-e3d404ac5b69",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# CELL ********************

import psycopg2
from psycopg2.extras import execute_batch   
import pandas as pd

TIMESCALE_CONN = "postgres://tsdbadmin:Z4BiD_tiger007@hqy6qzinbi.lc7gkwv51k.tsdb.cloud.timescale.com:32067/tsdb?sslmode=require"

def get_connection():
    """Always create a fresh connection — Fabric sessions can be long-lived."""
    return psycopg2.connect(TIMESCALE_CONN, sslmode="require") 


with get_connection() as conn:
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS weather_metrics (
                time           TIMESTAMPTZ    NOT NULL,
                zone           TEXT,
                temp           DOUBLE PRECISION,
                humidity       DOUBLE PRECISION,
                precipitation  DOUBLE PRECISION,
                rain           DOUBLE PRECISION,
                snow           DOUBLE PRECISION,
                wind_speed     DOUBLE PRECISION,
                wind_direction INTEGER
            );
        """)
        cur.execute("""
            SELECT create_hypertable(
                'weather_metrics', 'time',
                if_not_exists => TRUE
            );
        """)
        cur.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS weather_metrics_time_zone_idx
            ON weather_metrics (time, zone);
        """)
    conn.commit()
    print("Table and hypertable ready.")


with get_connection() as conn:
    with conn.cursor() as cur:
        cur.execute("SELECT MAX(time) FROM weather_metrics")
        result = cur.fetchone()[0]
    
    watermark = result.strftime('%Y-%m-%dT%H:%M:%SZ') if result else "2023-01-01T00:00:00Z"
    print(f"Watermark: {watermark}")

    new_data_df = spark.sql(f"""
        SELECT * FROM LH_Silver.weather_silverdata_ny
        WHERE weather_timestamp > '{watermark}'
          AND weather_timestamp <= TIMESTAMP('{watermark}') + INTERVAL 24 HOURS
        ORDER BY weather_timestamp ASC
    """)

    row_count = new_data_df.count()
    print(f"Rows in chunk: {row_count}")

    if row_count > 0:
        pdf = new_data_df.toPandas()

        records = [
            (
                row["weather_timestamp"],
                row["zone_name"],
                float(row["temperature_c"]),
                float(row["humidity_%"]),
                float(row["precipitation_mm"]),
                float(row["rain_mm"]),
                float(row["snowfall_cm"]),
                float(row["wind_speed_kmh"]),
                int(row["wind_direction_deg"])
            )
            for _, row in pdf.iterrows()
        ]

        with conn.cursor() as cur:
            execute_batch(cur, """
                INSERT INTO weather_metrics
                    (time, zone, temp, humidity, precipitation,
                     rain, snow, wind_speed, wind_direction)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (time, zone) DO NOTHING
            """, records, page_size=500)
        conn.commit()

        print(f"Synced! Watermark → {pdf['weather_timestamp'].iloc[-1]}")
    else:
        print("Fully caught up! No new data.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
