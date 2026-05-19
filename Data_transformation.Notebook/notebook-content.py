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
# META           "id": "5415b106-597c-4a98-906c-b8b15e7ef384"
# META         },
# META         {
# META           "id": "3dbb6804-e902-4cdd-88e5-8a95e6986c24"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql.functions import *
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, LongType, DoubleType, StringType
from pyspark.sql import DataFrame

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Taxi_Trip_Data**

# CELL ********************

INPUT_FOLDER = "Files/NYC_Taxi/trip-data/"
OUTPUT_PATH  = "Files/NYC_Taxi/trip-data/yellow_tripdata_merged"

CANONICAL_COLS = {
    "vendorid":              IntegerType(),
    "tpep_pickup_datetime":  None,          
    "tpep_dropoff_datetime": None,
    "passenger_count":       DoubleType(),
    "trip_distance":         DoubleType(),
    "ratecodeid":            DoubleType(),
    "store_and_fwd_flag":    StringType(),
    "pulocationid":          IntegerType(),
    "dolocationid":          IntegerType(),
    "payment_type":          LongType(),
    "fare_amount":           DoubleType(),
    "extra":                 DoubleType(),
    "mta_tax":               DoubleType(),
    "tip_amount":            DoubleType(),
    "tolls_amount":          DoubleType(),
    "improvement_surcharge": DoubleType(),
    "total_amount":          DoubleType(),
    "congestion_surcharge":  DoubleType(),
    "airport_fee":           DoubleType(),
}

def normalize_df(df: DataFrame) -> DataFrame:
    
    for original in df.columns:
        lowered = original.lower()
        if original != lowered:
            df = df.withColumnRenamed(original, lowered)

    for col_name, target_type in CANONICAL_COLS.items():
        if col_name in df.columns:
            if target_type is not None:                         
                df = df.withColumn(col_name, F.col(col_name).cast(target_type))
        else:
            fill_type = target_type if target_type else DoubleType()
            df = df.withColumn(col_name, F.lit(None).cast(fill_type))
            print(f"  ⚠️  '{col_name}' missing → added as NULL")

    return df.select(list(CANONICAL_COLS.keys()))

files = [
    f.path for f in mssparkutils.fs.ls(INPUT_FOLDER)
    if f.name.endswith(".parquet")
]

print(f"Found {len(files)} parquet file(s)")
for f in files:
    print(f"  {f}")

merged_df = None

for path in files:
    print(f"\nProcessing: {path.split('/')[-1]}")
    
    df_raw  = spark.read.parquet(path)   
    df_norm = normalize_df(df_raw)

    print(f"  Rows: {df_raw.count():,}")

    if merged_df is None:
        merged_df = df_norm
    else:
        merged_df = merged_df.union(df_norm) 

print(f"\nTotal merged rows : {merged_df.count():,}")
print("\nFinal schema:")
merged_df.printSchema()

print(f"\nSaving to: {OUTPUT_PATH}")

(merged_df
    .write
    .mode("overwrite")
    .parquet(OUTPUT_PATH)
)

print("Saved successfully!")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

bronze_paths = "Files/NYC_Taxi/trip-data/yellow_tripdata_merged"
lookup_path = "Files/taxi_zone_lookup (1).csv"  

df_raw = spark.read.parquet(bronze_paths)
df_lookup = spark.read.csv(lookup_path, header=True, inferSchema=True)

df_selected = df_raw.select(
    col("tpep_pickup_datetime").cast("timestamp_ltz").alias("pickup_datetime"), 
    col("tpep_dropoff_datetime").cast("timestamp_ltz").alias("dropoff_datetime"),
    col("passenger_count").cast("int"),
    col("trip_distance").cast("double"),
    col("PULocationID").alias("pickup_location_id"),
    col("fare_amount").cast("decimal(10,2)"),      
    col("total_amount").cast("decimal(10,2)"),
    col("congestion_surcharge").cast("decimal(10,2)")
)

df_filled = df_selected.fillna({
    "passenger_count": 1,
    "congestion_surcharge": 0.00
})

df_transformed = df_filled.withColumn(
    "passenger_count", 
    when(col("passenger_count") == 0, 1).otherwise(col("passenger_count"))
).withColumn(
    "congestion_surcharge", 
    when(col("congestion_surcharge") < 0, 0).otherwise(col("congestion_surcharge"))
)

df_silver = df_transformed.filter(
    (col("fare_amount") > 0) &                   
    (col("total_amount") > 0) & 
    (col("trip_distance") > 0) &
    (year(col("pickup_datetime")).isin(2021,2022,2023,2024)) &
    (year(col("dropoff_datetime")).isin(2021,2022,2023,2024))
)                         

df_enriched = df_silver.withColumn("pickup_year", year("pickup_datetime")) \
                       .withColumn("pickup_hour", hour("pickup_datetime")) \
                       .withColumn("pickup_day", dayofweek("pickup_datetime")) \
                       .withColumn("pickup_month", month("pickup_datetime")) \
                       .dropDuplicates()

df_final = df_enriched.join(df_lookup, df_enriched["pickup_location_id"] == df_lookup["LocationID"], "left") \
                      .withColumnRenamed("Borough", "pickup_borough") \
                      .withColumnRenamed("Zone", "pickup_zone") \
                      .drop("pickup_location_id", "LocationID", "service_zone", "fare_amount")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("--- STARTING DATA QUALITY CHECKS ON df_final ---")

print(df_final.count())
null_counts_df = df_final.select([
    count(when(isnull(c), c)).alias(c) for c in df_final.columns
])

null_dict = null_counts_df.collect()[0].asDict()
found_nulls = False
for column_name, null_count in null_dict.items():
    if null_count > 0:
        print(f"Column '{column_name}' has {null_count} null values.")
        found_nulls = True

if not found_nulls:
    print(" There are 0 null values in your entire dataset.")

unique_dates = df_final.select(to_date(col("pickup_datetime")).alias("date")).distinct()
unique_date_count = unique_dates.count()
min_date = unique_dates.agg({"date": "min"}).collect()[0][0]
max_date = unique_dates.agg({"date": "max"}).collect()[0][0]

print(f"\n2. Date Bounds Check:")
print(f"   - Total Unique Days: {unique_date_count}")
print(f"   - Earliest Date: {min_date}")
print(f"   - Latest Date: {max_date}")

neg_totals = df_final.filter(col("total_amount") <= 0).count()
neg_dist = df_final.filter(col("trip_distance") <= 0).count()

neg_surcharge = df_final.filter(col("congestion_surcharge") < 0).count()

print(f"\n3. Negative Value Anomalies (Target is 0):")
print(f"   - Negative Total Amount: {neg_totals}")
print(f"   - Negative Trip Distance: {neg_dist}")
print(f"   - Negative Congestion Surcharge: {neg_surcharge}")

zero_passengers = df_final.filter(col("passenger_count") == 0).count()
print(f"\n4. Passenger Anomalies:")
print(f"   - Trips with 0 Passengers: {zero_passengers}")
print("\n--- DATA QUALITY CHECKS COMPLETE ---")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_final.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("TaxiTrip_data_silver")

print("Data successfully written to the Silver Lakehouse!")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **AirQuality_Dataset**


# CELL ********************

parameters = ["21", "22", "23", "24"]
df_list = []

for param in parameters:
    query = f"SELECT * FROM LH_bronze.airquality_data_{param}"
    temp_df = spark.sql(query)
    
    clean_df = temp_df.select(
        col("date").cast("date"),
        col("location_name"),
        col("locality"),            
        col("timezone"),            
        col("parameter_name"),
        col("value").cast("double"),
        col("parameter_units"),
        col("latitude"),
        col("longitude")
    ).filter(col("value") >= 0)

    processed_df = clean_df.filter(year(col("date")).isin(2021, 2022, 2023, 2024)) \
                           .withColumn("timezone", regexp_replace(col("timezone"), "/", "")) \
                           .withColumn("year", year(col("date"))) \
                           .withColumn("month", month(col("date"))) \
                           .withColumn("dayofweek", dayofweek(col("date"))) \
                           .dropDuplicates()
    
    df_list.append(processed_df)

df_final_air = df_list[0]
for next_df in df_list[1:]:
    df_final_air = df_final_air.union(next_df)

df_final_air = df_final_air.fillna({"locality": "Unknown"})

print(f"Total rows prepared for Silver: {df_final_air.count()}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("--- STARTING AIR QUALITY DATA CHECKS ---")

print(f"\n2. Null Value Checks:")
null_counts_df = df_final_air.select([
    count(when(isnull(c), c)).alias(c) for c in df_final_air.columns
])
null_dict = null_counts_df.collect()[0].asDict()

found_nulls = False
for column, null_count in null_dict.items():
    if null_count > 0:
        print(f"Column '{column}' has {null_count} null values.")
        found_nulls = True
if not found_nulls:
    print("0 null values found across all columns.")

negative_readings = df_final_air.filter(col("value") < 0).count()
print(f"\n3. Negative Value Anomalies (Target is 0):")
print(f"   - Negative sensor readings: {negative_readings}")

date_bounds = df_final_air.select(min("date").alias("min_date"), max("date").alias("max_date")).collect()[0]
print(f"\n4. Date Bounds Check:")
print(f"   - Earliest Date: {date_bounds['min_date']}")
print(f"   - Latest Date: {date_bounds['max_date']}")

print(f"\n5. Row Count by Parameter:")
df_final_air.groupBy("parameter_name").count().show()

print(f"\n6. Sample of Localities in Dataset:")
df_final_air.groupBy("locality").count().orderBy(col("count").desc()).show(10, truncate=False)

print("--- DATA QUALITY CHECKS COMPLETE ---")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_final_air.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("AirQuality_data_silver")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **ECB_DailyFX_Data**

# CELL ********************

from pyspark.sql.window import Window

df_fx_bronze = spark.read.table("LH_bronze.ECB_dailyFX")

df_fx_clean = df_fx_bronze.select(
    to_date(col("TIME_PERIOD")).alias("date"),
    col("CURRENCY").alias("base_currency"),        
    col("CURRENCY_DENOM").alias("target_currency"),
    col("OBS_VALUE").cast("decimal(10,4)").alias("exchange_rate")
)

df_calendar = spark.sql("""
    SELECT explode(sequence(to_date('1999-01-04'), to_date('2026-04-30'), interval 1 day)) AS date
""")

df_currency_pairs = df_fx_clean.select("base_currency", "target_currency").dropDuplicates()
df_spine = df_calendar.crossJoin(df_currency_pairs)
df_joined = df_spine.join(df_fx_clean, ["date", "base_currency", "target_currency"], "left")

window_spec = Window.partitionBy("base_currency", "target_currency").orderBy("date").rowsBetween(Window.unboundedPreceding, Window.currentRow)

df_fx_final = df_joined.withColumn(
    "exchange_rate",
    last(col("exchange_rate"), ignorenulls=True).over(window_spec)
)

df_fx_final = df_fx_final.filter(col("exchange_rate").isNotNull())

print(f"Total Rows: {df_fx_final.count()}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("--- ANOMALY CHECK REPORT ---")

print("1. Null Value Counts:")
df_fx_final.select([
    count(when(col(c).isNull(), c)).alias(c) for c in df_fx_final.columns
]).show()

print("2. Date Range:")
df_fx_final.select(
    min("date").alias("Min_Date"), 
    max("date").alias("Max_Date")
).show()

print("3. Duplicate Dates Check:")
df_duplicates = df_fx_final.groupBy("date", "base_currency", "target_currency") \
                           .count() \
                           .filter(col("count") > 1)

duplicate_count = df_duplicates.count()
print(f"Total Duplicates Found: {duplicate_count}")

if duplicate_count > 0:
    print("Showing sample of duplicates:")
    df_duplicates.show(5)

anomalous_count = df_fx_final.filter(col("exchange_rate") <= 0).count()
print(f"4. Impossible Exchange Rates (<= 0): {anomalous_count}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_fx_final.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("ECB_DailyFX_data_silver")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **US_GDP_DATA**

# CELL ********************

df_gdp_bronze = spark.read.table("LH_bronze.WB_USA_GDP")

df_gdp_clean = df_gdp_bronze.select(
    col("date").cast("int").alias("year"),
    col("countryiso3code").alias("country_code"),
    col("value").cast("long").alias("gdp_usd")
)

df_gdp_filtered = df_gdp_clean.filter(col("year") != 2025)
df_gdp_final = df_gdp_filtered.dropDuplicates()

print("GDP Data successfully cleaned, year 2025 dropped, and saved!")
print(f"Total Years Available: {df_gdp_final.count()}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("--- GDP DATA ANOMALY CHECK REPORT ---")

print("1. Null Value Counts:")
df_gdp_final.select([
    count(when(col(c).isNull(), c)).alias(c) for c in df_gdp_final.columns
]).show()

print("2. Year Range:")
df_gdp_final.select(
    min("year").alias("Min_Year"), 
    max("year").alias("Max_Year")
).show()

print("3. GDP USD Statistics:")
df_gdp_final.select("gdp_usd").summary("count", "min", "25%", "50%", "mean", "75%", "max").show()

anomalous_count = df_gdp_final.filter(col("gdp_usd") <= 0).count()
print(f"4. Impossible GDP Values (<= 0): {anomalous_count}")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_gdp_final.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("US_gdp_silver")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Weather Data**

# CELL ********************

bronze_weather_df = spark.read.table("LH_bronze.weatherdata")

standardized_df = bronze_weather_df.select(
    F.col("date").cast(TimestampType()).alias("weather_timestamp"),
    F.col("ZoneName").cast(StringType()).alias("zone_name"),
    F.round(F.col("temperature_2m").cast(DoubleType()), 2).alias("temperature_c"),
    F.round(F.col("relative_humidity_2m").cast(DoubleType()), 2).alias("humidity_%"),
    F.round(F.col("precipitation").cast(DoubleType()), 2).alias("precipitation_mm"),
    F.round(F.col("rain").cast(DoubleType()), 2).alias("rain_mm"),
    F.round(F.col("snowfall").cast(DoubleType()), 2).alias("snowfall_cm"),
    F.round(F.col("wind_speed_10m").cast(DoubleType()), 2).alias("wind_speed_kmh"),
    F.col("wind_direction_10m").cast(IntegerType()).alias("wind_direction_deg")
)

START_DATE = "2023-01-01"
END_DATE = "2026-05-10"

cleaned_df = standardized_df \
    .filter((F.col("weather_timestamp") >= START_DATE) & (F.col("weather_timestamp") <= END_DATE)) \
    .dropDuplicates(["weather_timestamp", "zone_name"])

enriched_df = cleaned_df.withColumn(
    "DateKey", F.date_format(F.col("weather_timestamp"), "yyyyMMdd").cast(IntegerType())
).withColumn(
    "hour_of_day", F.hour(F.col("weather_timestamp"))
).withColumn(
    "year", F.year(F.col("weather_timestamp"))
).withColumn(
    "month", F.month(F.col("weather_timestamp"))
).withColumn(
    "day_of_week", F.dayofweek(F.col("weather_timestamp"))
)

final_silver_df = enriched_df.select(
    "DateKey", "weather_timestamp", "zone_name", "hour_of_day", "year", "month", "day_of_week",
    "temperature_c", "humidity_%", "precipitation_mm", "rain_mm", "snowfall_cm", 
    "wind_speed_kmh", "wind_direction_deg"
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(final_silver_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

final_silver_df.write \
    .format("delta") \
    .mode("overwrite") \
    .partitionBy("year") \
    .saveAsTable("Weather_Silverdata_NY")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
