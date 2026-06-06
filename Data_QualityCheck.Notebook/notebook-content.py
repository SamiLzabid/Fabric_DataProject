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
# META     }
# META   }
# META }

# CELL ********************

%pip install great_expectations==0.18.19 discord-webhook --quiet

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import great_expectations as gx
from great_expectations.dataset import PandasDataset
from discord_webhook import DiscordWebhook, DiscordEmbed
import pandas as pd
import numpy as np
from datetime import datetime

DISCORD_WEBHOOK_URL = "https://discord.com/api/webhooks/1503774267341279436/cMH3ENrAQsBo-hjd11OEGrWpjNdUQUqyHVbS9Drp65aqrSMZ-DSC5YIphDBILvT8uwFi"


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def send_discord_report(results: dict, suite_name: str = "Weather Data Quality"):

    total        = results["total_expectations"]
    passed       = results["passed"]
    failed       = results["failed"]
    success_rate = (passed / total * 100) if total > 0 else 0
    all_passed   = failed == 0

    webhook = DiscordWebhook(url=DISCORD_WEBHOOK_URL, username="Fabric QA Bot")

    header = DiscordEmbed(
        title=f"{'✅' if all_passed else '❌'} {suite_name} Report",
        description=f"Validation run at `{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}`",
        color="03b2f8" if all_passed else "ff0000"
    )
    header.add_embed_field(name="Total Checks",    value=str(total),  inline=True)
    header.add_embed_field(name="✅ Passed",        value=str(passed), inline=True)
    header.add_embed_field(name="❌ Failed",        value=str(failed), inline=True)
    header.add_embed_field(name="Success Rate",    value=f"{success_rate:.1f}%", inline=False)

    header.set_footer(text=f"Great Expectations • {suite_name}")
    webhook.add_embed(header)

    if not all_passed:
        fail_embed = DiscordEmbed(
            title="⚠️ Failed Checks Detail",
            color="ff0000"
        )

        max_visible_failures = 15
        recorded_failures = results["failures"]
        
        for idx, failure in enumerate(recorded_failures):
            if idx < max_visible_failures:
                detail_text = failure['detail']
                if len(detail_text) > 900:
                    detail_text = detail_text[:900] + "... [Truncated]"

                fail_embed.add_embed_field(
                    name=f"❌ {failure['check']}",
                    value=f"```{detail_text}```",
                    inline=False
                )
            else:
                remaining_count = len(recorded_failures) - max_visible_failures
                fail_embed.add_embed_field(
                    name="⚠️ Overflow Alert",
                    value=f"```And {remaining_count} more column validation checks failed. Please check notebook console logs for details.```",
                    inline=False
                )
                break
                
        webhook.add_embed(fail_embed)

    if results.get("stats"):
        stats_embed = DiscordEmbed(title="📊 Dataset Statistics", color="5865f2")
        for stat_name, stat_val in results["stats"].items():
            stats_embed.add_embed_field(name=stat_name, value=str(stat_val), inline=True)
        webhook.add_embed(stats_embed)

    response = webhook.execute()
    print(f"📨 Discord report sent! Status: {response.status_code}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Taxi Trip Data Report**

# CELL ********************

def check_taxi_quality(df: pd.DataFrame) -> dict:
    """Runs data quality checks on the Taxi Trip dataset with comprehensive null coverage."""
    gx_df = gx.dataset.PandasDataset(df)
    results = {
        "total_expectations": 0,
        "passed": 0,
        "failed": 0,
        "failures": [],
        "stats": {}
    }

    def check(expectation_result, check_name: str, detail: str):
        results["total_expectations"] += 1
        if expectation_result["success"]:
            results["passed"] += 1
        else:
            results["failed"] += 1
            results["failures"].append({"check": check_name, "detail": detail})

    print(f"📊 Running checks on {len(df):,} Taxi Trip rows...")

    total_nulls = 0
    for col in df.columns:
        result = gx_df.expect_column_values_to_not_be_null(col)
        null_count = int(df[col].isnull().sum())
        total_nulls += null_count
        check(result, f"No Nulls → {col}", f"Found {null_count} null rows")

    if "total_amount" in df.columns:
        # strict_min=True forces the check to be strictly > 0 (failing anything <= 0)
        result = gx_df.expect_column_values_to_be_between("total_amount", min_value=0, strict_min=True)
        fail_count = result["result"].get("unexpected_count", 0)
        
        if fail_count > 0:
            bad_values = result["result"].get("unexpected_list", [])
            unique_bad_values = list(set(bad_values))
            sample_str = ", ".join([str(v) for v in unique_bad_values[:5]])
            if len(unique_bad_values) > 5:
                sample_str += "..."
            detail_msg = f"Found {fail_count} zero or negative values (<= 0). Examples: [{sample_str}]"
        else:
            detail_msg = "All total amounts are strictly greater than 0"
            
        check(result, "Positive Total Amount", detail_msg)

    if "trip_distance" in df.columns:
        # strict_min=True forces the check to be strictly > 0 (failing anything <= 0)
        result = gx_df.expect_column_values_to_be_between("trip_distance", min_value=0, strict_min=True)
        fail_count = result["result"].get("unexpected_count", 0)
        
        if fail_count > 0:
            bad_values = result["result"].get("unexpected_list", [])
            unique_bad_values = list(set(bad_values))
            sample_str = ", ".join([str(v) for v in unique_bad_values[:5]])
            if len(unique_bad_values) > 5:
                sample_str += "..."
            detail_msg = f"Found {fail_count} zero or negative distance metrics (<= 0). Examples: [{sample_str}]"
        else:
            detail_msg = "All distance metrics are strictly greater than 0"
            
        check(result, "Positive Trip Distance", detail_msg)


    # 3. Zero Passenger Verification
    if "passenger_count" in df.columns:
        result = gx_df.expect_column_values_to_be_between("passenger_count", min_value=1)
        fail_count = result["result"].get("unexpected_count", 0)
        
        if fail_count > 0:
            detail_msg = f"Found {fail_count} trips violating min limit of 1 passenger"
        else:
            detail_msg = "All trips contain at least 1 passenger"
            
        check(result, "Passenger Imputation Check", detail_msg)

    # 4. Extract Date Range
    date_range_str = "N/A"
    if "pickup_datetime" in df.columns and len(df) > 0:
        try:
            converted_dates = pd.to_datetime(df["pickup_datetime"])
            min_d = converted_dates.min().strftime('%Y-%m-%d')
            max_d = converted_dates.max().strftime('%Y-%m-%d')
            date_range_str = f"{min_d} to {max_d}"
        except Exception:
            date_range_str = "Invalid Date Format"

    # Stats compilation
    results["stats"] = {
        "📋 Total Records": f"{len(df):,}",
        "🚖 Unique Zones": f"{df['pickup_zone'].nunique():,}" if "pickup_zone" in df.columns else "N/A",
        "🔲 Total Null Values": f"{total_nulls:,}",
        "📅 Processing Window": date_range_str,
        "💰 Total Revenue": f"${df['total_amount'].sum():,.2f}" if "total_amount" in df.columns else "N/A"
    }

    send_discord_report(results, suite_name="Taxi Data Quality")
    return results

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

master_taxi_df = spark.sql("SELECT * FROM LH_Silver.taxitrip_data_silver LIMIT 10000000")
taxi_pdf = master_taxi_df.toPandas()
taxi_res = check_taxi_quality(taxi_pdf)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Air Quality Data Report**

# CELL ********************

def check_airquality_quality(df: pd.DataFrame) -> dict:
    """Runs data quality checks on the Air Quality monitoring dataset with comprehensive null coverage."""
    gx_df = gx.dataset.PandasDataset(df)
    results = {
        "total_expectations": 0,
        "passed": 0,
        "failed": 0,
        "failures": [],
        "stats": {}
    }

    def check(expectation_result, check_name: str, detail: str):
        results["total_expectations"] += 1
        if expectation_result["success"]:
            results["passed"] += 1
        else:
            results["failed"] += 1
            results["failures"].append({"check": check_name, "detail": detail})

    print(f"📊 Running checks on {len(df):,} Air Quality rows...")

    # 1. Null Checks (Dynamically targets ALL columns)
    total_nulls = 0
    for col in df.columns:
        result = gx_df.expect_column_values_to_not_be_null(col)
        null_count = int(df[col].isnull().sum())
        total_nulls += null_count
        check(result, f"No Nulls → {col}", f"Found {null_count} null metrics")

    # 2. Value Range Boundary (Upgraded to print specific failing samples)
    if "value" in df.columns:
        result = gx_df.expect_column_values_to_be_between("value", min_value=0)
        fail_count = result["result"].get("unexpected_count", 0)
        
        if fail_count > 0:
            bad_values = result["result"].get("unexpected_list", [])
            unique_bad_values = list(set(bad_values))
            sample_str = ", ".join([str(v) for v in unique_bad_values[:5]])
            if len(unique_bad_values) > 5:
                sample_str += "..."
            detail_msg = f"Found {fail_count} negative pollutant readings (< 0). Examples: [{sample_str}]"
        else:
            detail_msg = "All concentration values are non-negative (>= 0)"
            
        check(result, "Non-Negative Concentration Value", detail_msg)

    # 3. Extract Date Range (Looks for date)
    date_range_str = "N/A"
    if "date" in df.columns and len(df) > 0:
        try:
            converted_dates = pd.to_datetime(df["date"])
            min_d = converted_dates.min().strftime('%Y-%m-%d')
            max_d = converted_dates.max().strftime('%Y-%m-%d')
            date_range_str = f"{min_d} to {max_d}"
        except Exception:
            date_range_str = "Invalid Date Format"

    # 4. Extract Dynamic Parameter Stats
    distinct_params = "N/A"
    max_vals_str = "N/A"
    
    if "parameter_name" in df.columns:
        # Grab a comma-separated list of all distinct parameters (e.g., "PM2.5, NO2, O3")
        unique_params = df["parameter_name"].dropna().unique()
        distinct_params = ", ".join([str(p) for p in unique_params])
        
        # Group by the parameter and find the max value for each
        if "value" in df.columns:
            max_per_param = df.groupby("parameter_name")["value"].max()
            # Format into a clean string (e.g., "PM2.5: 145.00 | NO2: 80.50")
            max_vals_str = " | ".join([f"{param}: {val:,.2f}" for param, val in max_per_param.items()])

    # Stats compilation
    results["stats"] = {
        "📋 Total Readings": f"{len(df):,}",
        "🏙️ Unique Localities": f"{df['locality'].nunique():,}" if "locality" in df.columns else "N/A",
        "🧪 Parameters Checked": distinct_params,
        "📈 Max Value by Parameter": max_vals_str,
        "🔲 Total Null Values": f"{total_nulls:,}",
        "📅 Processing Window": date_range_str
    }

    send_discord_report(results, suite_name="Air Quality Data Quality")
    return results

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

master_air_df = spark.sql("SELECT * FROM LH_Silver.airquality_data_silver")
air_pdf = master_air_df.toPandas()
air_res = check_airquality_quality(air_pdf)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Daily FX Data Report**

# CELL ********************

def check_fx_quality(df: pd.DataFrame) -> dict:
    """Runs data quality checks on the ECB Daily Exchange Rates dataset with comprehensive null coverage."""
    gx_df = gx.dataset.PandasDataset(df)
    results = {
        "total_expectations": 0,
        "passed": 0,
        "failed": 0,
        "failures": [],
        "stats": {}
    }

    def check(expectation_result, check_name: str, detail: str):
        results["total_expectations"] += 1
        if expectation_result["success"]:
            results["passed"] += 1
        else:
            results["failed"] += 1
            results["failures"].append({"check": check_name, "detail": detail})

    print(f"📊 Running checks on {len(df):,} FX Rate tracking entries...")

    # 1. Null Checks (Dynamically targets ALL columns)
    total_nulls = 0
    for col in df.columns:
        result = gx_df.expect_column_values_to_not_be_null(col)
        null_count = int(df[col].isnull().sum())
        total_nulls += null_count
        check(result, f"No Nulls → {col}", f"Found {null_count} broken fields")

    # 2. Realistic Boundary Market Constraints
    if "exchange_rate" in df.columns:
        outlier_rates = df[(df["exchange_rate"] < 0.2) | (df["exchange_rate"] > 3.0)].shape[0]
        check(gx_df.expect_column_values_to_be_between("exchange_rate", min_value=0.2, max_value=3.0), 
              "Market Rationality Boundary (0.2 - 3.0)", f"{outlier_rates} pricing metrics outside bounds")

    # 3. Extract Date Range (Looks for date)
    date_range_str = "N/A"
    if "date" in df.columns and len(df) > 0:
        try:
            converted_dates = pd.to_datetime(df["date"])
            min_d = converted_dates.min().strftime('%Y-%m-%d')
            max_d = converted_dates.max().strftime('%Y-%m-%d')
            date_range_str = f"{min_d} to {max_d}"
        except Exception:
            date_range_str = "Invalid Date Format"

    # Stats compilation
    results["stats"] = {
        "📋 Total Trading Days": f"{len(df):,}",
        "🔲 Total Null Values": f"{total_nulls:,}",
        "📅 Processing Window": date_range_str,
        "💵 Max Historical Rate": f"{df['exchange_rate'].max():.4f}" if "exchange_rate" in df.columns else "N/A"
    }

    send_discord_report(results, suite_name="FX Rate Data Quality")
    return results

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

master_FX_df = spark.sql("SELECT * FROM LH_Silver.ecb_dailyfx_data_silver")
FX_pdf = master_FX_df.toPandas()
FX_res = check_fx_quality(FX_pdf)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **GDP Data Report**

# CELL ********************

def check_gdp_quality(df: pd.DataFrame) -> dict:
    """Runs data quality checks on the US GDP annual historical tracking dataset with comprehensive null coverage."""
    gx_df = gx.dataset.PandasDataset(df)
    results = {
        "total_expectations": 0,
        "passed": 0,
        "failed": 0,
        "failures": [],
        "stats": {}
    }

    def check(expectation_result, check_name: str, detail: str):
        results["total_expectations"] += 1
        if expectation_result["success"]:
            results["passed"] += 1
        else:
            results["failed"] += 1
            results["failures"].append({"check": check_name, "detail": detail})

    print(f"📊 Running checks on {len(df):,} GDP timeline rows...")

    # 1. Null Checks (Dynamically targets ALL columns)
    total_nulls = 0
    for col in df.columns:
        result = gx_df.expect_column_values_to_not_be_null(col)
        null_count = int(df[col].isnull().sum())
        total_nulls += null_count
        check(result, f"No Nulls → {col}", f"Found {null_count} empty fields")

    # 2. Year Value Boundaries
    if "year" in df.columns:
        invalid_years = df[(df["year"] < 1900) | (df["year"] > datetime.now().year)].shape[0]
        check(gx_df.expect_column_values_to_be_between("year", min_value=1900, max_value=datetime.now().year), 
              "Valid Operational History Years", f"Found {invalid_years} invalid historical dates")

    # 3. Macroeconomic Positivity Constraints
    if "gdp_usd" in df.columns:
        negative_gdp = df[df["gdp_usd"] <= 0].shape[0]
        check(gx_df.expect_column_values_to_be_between("gdp_usd", min_value=1), 
              "Positive Absolute Value GDP", f"Found {negative_gdp} rows experiencing asset collapses <= 0")

    # 4. Extract Year Range (Uses numeric year column directly)
    date_range_str = "N/A"
    if "year" in df.columns and len(df) > 0:
        min_y = int(df["year"].min())
        max_y = int(df["year"].max())
        date_range_str = f"{min_y} to {max_y}"

    # Stats compilation
    results["stats"] = {
        "📋 Historical Profiles": f"{len(df)} Years Monitored",
        "🔲 Total Null Values": f"{total_nulls:,}",
        "📅 Processing Window": date_range_str,
        "🎯 Peak US Valuation": f"${df['gdp_usd'].max():,.2f}" if "gdp_usd" in df.columns else "N/A"
    }

    send_discord_report(results, suite_name="US GDP Data Quality")
    return results

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

master_GDP_df = spark.sql("SELECT * FROM LH_Silver.us_gdp_silver")
GDP_pdf = master_GDP_df.toPandas()
GDP_res = check_gdp_quality(GDP_pdf)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Weather Data Report**

# CELL ********************

def check_weather_quality(df: pd.DataFrame) -> dict:

    gx_df   = gx.dataset.PandasDataset(df)
    results = {
        "total_expectations": 0,
        "passed":  0,
        "failed":  0,
        "failures": [],
        "stats":   {}
    }

    def check(expectation_result, check_name: str, detail: str):
        """Helper to register each check result."""
        results["total_expectations"] += 1
        if expectation_result["success"]:
            results["passed"] += 1
        else:
            results["failed"] += 1
            results["failures"].append({
                "check":  check_name,
                "detail": detail
            })

    # ── 1. NULL CHECKS ────────────────────────────────────────────
    total_nulls = 0
    updated_cols = ["weather_timestamp", "zone_name", "temperature_c", "humidity_%",
                    "precipitation_mm", "wind_speed_kmh", "rain_mm", "snowfall_cm", "wind_direction_deg"]
    
    for col in updated_cols:
        if col in df.columns:
            result = gx_df.expect_column_values_to_not_be_null(col)
            null_count = df[col].isnull().sum()
            total_nulls += int(null_count)
            check(result, f"No Nulls → {col}", f"Found {null_count} null values")

    # ── 2. TEMPERATURE RANGE (NYC realistic: -30°C to 45°C) ───────
    if "temperature_c" in df.columns:
        result = gx_df.expect_column_values_to_be_between(
            "temperature_c", min_value=-30, max_value=45
        )
        out_of_range = df[
            (df["temperature_c"] < -30) | (df["temperature_c"] > 45)
        ].shape[0]
        check(result, "Temperature Range (-30°C to 45°C)",
              f"{out_of_range} values outside realistic NYC range")

    # ── 3. HUMIDITY RANGE (0–100%) ────────────────────────────────
    if "humidity_%" in df.columns:
        result = gx_df.expect_column_values_to_be_between(
            "humidity_%", min_value=0, max_value=100
        )
        bad_humidity = df[
            (df["humidity_%"] < 0) | (df["humidity_%"] > 100)
        ].shape[0]
        check(result, "Humidity Range (0–100%)",
              f"{bad_humidity} values outside 0–100 range")

    # ── 4. PRECIPITATION NON-NEGATIVE ─────────────────────────────
    if "precipitation_mm" in df.columns:
        result = gx_df.expect_column_values_to_be_between(
            "precipitation_mm", min_value=0
        )
        neg_precip = df[df["precipitation_mm"] < 0].shape[0]
        check(result, "Precipitation Non-Negative",
              f"{neg_precip} negative precipitation values")

    # ── 5. RAIN NON-NEGATIVE ──────────────────────────────────────
    if "rain_mm" in df.columns:
        result = gx_df.expect_column_values_to_be_between("rain_mm", min_value=0)
        neg_rain = df[df["rain_mm"] < 0].shape[0]
        check(result, "Rain Non-Negative", f"{neg_rain} negative rain values")

    # ── 6. SNOWFALL NON-NEGATIVE ──────────────────────────────────
    if "snowfall_cm" in df.columns:
        result = gx_df.expect_column_values_to_be_between("snowfall_cm", min_value=0)
        neg_snow = df[df["snowfall_cm"] < 0].shape[0]
        check(result, "Snowfall Non-Negative", f"{neg_snow} negative snowfall values")

    # ── 7. WIND SPEED NON-NEGATIVE ────────────────────────────────
    if "wind_speed_kmh" in df.columns:
        result = gx_df.expect_column_values_to_be_between(
            "wind_speed_kmh", min_value=0
        )
        neg_wind = df[df["wind_speed_kmh"] < 0].shape[0]
        check(result, "Wind Speed Non-Negative",
              f"{neg_wind} negative wind speed values")

    # ── 8. WIND DIRECTION RANGE (0–360°) ─────────────────────────
    if "wind_direction_deg" in df.columns:
        result = gx_df.expect_column_values_to_be_between(
            "wind_direction_deg", min_value=0, max_value=360
        )
        bad_dir = df[
            (df["wind_direction_deg"] < 0) | (df["wind_direction_deg"] > 360)
        ].shape[0]
        check(result, "Wind Direction Range (0–360°)",
              f"{bad_dir} values outside 0–360 range")

    # ── 9. DATE RANGE CHECK (Time-agnostic date comparison) ───────
    if "weather_timestamp" in df.columns and len(df) > 0:
        min_allowed = pd.Timestamp("2023-01-01").date()
        max_allowed = pd.Timestamp("2026-12-31").date()
        
        # Only grab the raw date component (ignores hours/timezones completely)
        df_dates = pd.to_datetime(df["weather_timestamp"]).dt.date
        out_of_date_range = df[
            (df_dates < min_allowed) | (df_dates > max_allowed)
        ].shape[0]
        
        date_ok = out_of_date_range == 0
        results["total_expectations"] += 1
        if date_ok:
            results["passed"] += 1
        else:
            results["failed"] += 1
            results["failures"].append({
                "check":  f"Date Range (2023-01-01 to 2026-12-31)",
                "detail": f"{out_of_date_range} records outside expected date boundaries"
            })

    # ── 10. DUPLICATE ROWS CHECK ──────────────────────────────────
    if "weather_timestamp" in df.columns and "zone_name" in df.columns:
        duplicate_count = df.duplicated(subset=["weather_timestamp", "zone_name"]).sum()
        dup_ok = duplicate_count == 0
        results["total_expectations"] += 1
        if dup_ok:
            results["passed"] += 1
        else:
            results["failed"] += 1
            results["failures"].append({
                "check":  "No Duplicate (timestamp, zone) pairs",
                "detail": f"{duplicate_count} duplicate rows found"
            })

    # ── 11. ROW COUNT CHECK ───────────────────────────────────────
    expected_min = 100_000
    result = gx_df.expect_table_row_count_to_be_between(
        min_value=expected_min, max_value=None
    )
    actual_rows = len(df)
    check(result, f"Row Count ≥ {expected_min:,}",
          f"Only {actual_rows:,} rows found — possible missing data")

    # ── 12. HOURLY COMPLETENESS PER ZONE ─────────────────────────
    if "zone_name" in df.columns:
        zone_counts   = df.groupby("zone_name").size()
        min_zone_rows = zone_counts.min()
        max_zone_rows = zone_counts.max()
        imbalance     = max_zone_rows - min_zone_rows
        balanced      = imbalance <= 24
        results["total_expectations"] += 1
        if balanced:
            results["passed"] += 1
        else:
            results["failed"] += 1
            results["failures"].append({
                "check":  "Hourly Completeness per Zone",
                "detail": f"Row imbalance across zones: {imbalance} hours. "
                          f"Counts: {zone_counts.to_dict()}"
            })

    # ── Stats summary for Discord ─────────────────────────────────
    if "weather_timestamp" in df.columns and len(df) > 0:
        clean_dates = pd.to_datetime(df["weather_timestamp"]).dt.date
        date_range_str = f"{clean_dates.min()} → {clean_dates.max()}"
    else:
        date_range_str = "N/A"

    results["stats"] = {
        "📋 Total Rows":       f"{len(df):,}",
        "🔲 Total Nulls":      f"{total_nulls:,}",
        "🗺️ Unique Zones":     f"{df['zone_name'].nunique()}" if "zone_name" in df.columns else "N/A",
        "🌡️ Temp Range":       f"{df['temperature_c'].min():.1f}°C to {df['temperature_c'].max():.1f}°C" if "temperature_c" in df.columns else "N/A",
        "💧 Avg Humidity":     f"{df['humidity_%'].mean():.1f}%" if "humidity_%" in df.columns else "N/A",
        "🌧️ Max Precip":       f"{df['precipitation_mm'].max():.2f} mm" if "precipitation_mm" in df.columns else "N/A",
        "❄️ Max Snowfall":     f"{df['snowfall_cm'].max():.2f} cm" if "snowfall_cm" in df.columns else "N/A",
        "💨 Max Wind":         f"{df['wind_speed_kmh'].max():.1f} km/h" if "wind_speed_kmh" in df.columns else "N/A",
        "📅 Date Range":       date_range_str
    }

    # Dispatch to Discord
    send_discord_report(results, suite_name="Weather Data Quality")

    return results

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

master_weather_df = spark.sql("SELECT * FROM LH_Silver.weather_silverdata_ny")
weather_pdf = master_weather_df.toPandas()
weather_res = check_weather_quality(weather_pdf)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
