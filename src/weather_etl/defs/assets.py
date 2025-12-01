from datetime import UTC, datetime, timedelta
from io import BytesIO
from typing import TYPE_CHECKING

import dagster as dg
import polars as pl
from dagster import AssetExecutionContext, MetadataValue, RetryPolicy, asset
from dagster_aws.s3 import S3Resource

from weather_etl.defs.resources import WeatherApiClient
from weather_etl.lake_path import (
    LakePath,
    actual_weather_path,
    forecast_path,
    staged_path,
)
from weather_etl.models import API_URL, ARCHIVE_API_URL, CITIES, HISTORICAL_API_URL

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client


def polars_to_s3(df: pl.DataFrame, lake_path: LakePath, s3_client: "S3Client"):
    """
    Helper function to write a Polars DataFrame to S3 as a Parquet file.
    Args:
        df (pl.DataFrame): The Polars DataFrame to write.
        lake_path (LakePath): The target LakePath in S3.
        s3_client (S3Client): The Boto3 S3 client to use for uploading.
    """
    with BytesIO() as byte_stream:
        df.write_parquet(byte_stream)
        byte_stream.seek(0)

        s3_client.put_object(Bucket=lake_path.bucket, Key=lake_path.key, Body=byte_stream)


monthly_partition = dg.MonthlyPartitionsDefinition(
    # Backfill the data from Jan 2023 onwards
    start_date=datetime(2023, 1, 1, tzinfo=UTC),
)


@asset(
    key_prefix=["raw"],
    retry_policy=RetryPolicy(max_retries=3, delay=10),
    group_name="weather_etl",
    metadata={
        "owner": "ybryz",
        "api-url": MetadataValue.url(HISTORICAL_API_URL),
        "endpoint": "/forecast",
    },
    kinds={"s3"},
    partitions_def=monthly_partition,
)
def historical_forecast(context: AssetExecutionContext, weather_api_client: WeatherApiClient, s3: S3Resource):
    """Fetches historical forecast data from the weather API."""
    now = datetime.now(UTC)
    start_month = datetime.strptime(context.partition_key, monthly_partition.fmt).date()
    end_month = (start_month + timedelta(days=32)).replace(day=1) - timedelta(days=1)

    # Fill in the end_month to not exceed today's date
    if now.date() < end_month:
        end_month = now.date()

    data = weather_api_client.historical_forecast(list(CITIES.values()), start_month, end_month)

    timestamp = now.strftime("%Y%m%dT%H%M%S")
    lake_path = forecast_path(start_month) / f"hourly_forecast_{timestamp}.parquet"

    # Add metadata fields
    df = pl.from_dicts(data)
    df = df.with_columns(pl.lit(timestamp).alias("extracted_at"))

    # Upload data to S3
    polars_to_s3(df, lake_path, s3.get_client())

    return dg.MaterializeResult(
        metadata={
            "uri": MetadataValue.url(lake_path.uri),
            "cities": MetadataValue.json(list(CITIES.keys())),
        }
    )


@asset(
    key_prefix=["raw"],
    retry_policy=RetryPolicy(max_retries=3, delay=10),
    group_name="weather_etl",
    metadata={
        "owner": "ybryz",
        "api-url": MetadataValue.url(API_URL),
        "endpoint": "/forecast",
    },
    kinds={"s3"},
    automation_condition=dg.AutomationCondition.on_cron("0 6 * * *"),  # Daily at 6 AM UTC
)
def hourly_forecast(weather_api_client: WeatherApiClient, s3: S3Resource):
    """Fetches hourly forecast data from the weather API for the next 7 days."""
    now = datetime.now(UTC)
    timestamp = now.strftime("%Y%m%dT%H%M%S")
    lake_path = forecast_path(now.replace(day=1)) / f"hourly_forecast_{timestamp}.parquet"

    data = weather_api_client.hourly_forecast(list(CITIES.values()))

    # Add metadata fields
    df = pl.from_dicts(data)
    df = df.with_columns(pl.lit(timestamp).alias("extracted_at"))

    # Upload data to S3
    polars_to_s3(df, lake_path, s3.get_client())

    return dg.MaterializeResult(
        metadata={
            "cities": MetadataValue.json(list(CITIES.keys())),
            "s3_path": MetadataValue.url(lake_path.uri),
        }
    )


@asset(
    key_prefix=["raw"],
    retry_policy=RetryPolicy(max_retries=3, delay=10),
    group_name="weather_etl",
    metadata={
        "owner": "ybryz",
        "api-url": MetadataValue.url(ARCHIVE_API_URL),
        "endpoint": "/archive",
    },
    kinds={"s3"},
    partitions_def=monthly_partition,
)
def historical_actual_weather(
    context: AssetExecutionContext, weather_api_client: WeatherApiClient, s3: S3Resource
):
    """Historical actual observed weather data from the archive API."""
    now = datetime.now(UTC)
    start_month = datetime.strptime(context.partition_key, monthly_partition.fmt).date()
    end_month = (start_month + timedelta(days=32)).replace(day=1) - timedelta(days=1)

    # Fill in the end_month to not exceed 5 days ago (archive API has 5-day delay)
    max_date = (now - timedelta(days=5)).date()
    if end_month > max_date:
        end_month = max_date

    data = weather_api_client.actual_weather(list(CITIES.values()), start_month, end_month)

    timestamp = now.strftime("%Y%m%dT%H%M%S")
    lake_path = actual_weather_path(start_month) / f"actual_weather_{timestamp}.parquet"

    # Add metadata fields
    df = pl.from_dicts(data)
    df = df.with_columns(pl.lit(timestamp).alias("extracted_at"))

    # Upload data to S3
    polars_to_s3(df, lake_path, s3.get_client())

    return dg.MaterializeResult(
        metadata={
            "cities": MetadataValue.json(list(CITIES.keys())),
            "s3_path": MetadataValue.url(lake_path.uri),
        }
    )


@asset(
    key_prefix=["raw"],
    retry_policy=RetryPolicy(max_retries=3, delay=10),
    group_name="weather_etl",
    metadata={
        "owner": "ybryz",
        "api-url": MetadataValue.url(ARCHIVE_API_URL),
        "endpoint": "/archive",
    },
    kinds={"s3"},
    automation_condition=dg.AutomationCondition.on_cron("0 3 */5 * *"),  # Every 5 days at 3 AM UTC
)
def actual_weather(weather_api_client: WeatherApiClient, s3: S3Resource):
    """Actual observed weather data for the last available period (5 days ago) from the archive API."""
    now = datetime.now(UTC)

    start = (now - timedelta(days=10)).date()
    end = (now - timedelta(days=5)).date()  # Archive API has 5-day delay

    data = weather_api_client.actual_weather(list(CITIES.values()), start, end)
    timestamp = now.strftime("%Y%m%dT%H%M%S")
    lake_path = actual_weather_path(start.replace(day=1)) / f"actual_weather_{timestamp}.parquet"

    # Add metadata fields
    df = pl.from_dicts(data)
    df = df.with_columns(pl.lit(timestamp).alias("extracted_at"))

    # Upload data to S3
    polars_to_s3(df, lake_path, s3.get_client())

    return dg.MaterializeResult(
        metadata={
            "cities": MetadataValue.json(list(CITIES.keys())),
            "uri": MetadataValue.url(lake_path.uri),
            "start_date": MetadataValue.text(str(start)),
            "end_date": MetadataValue.text(str(end)),
        }
    )


def transform_weather_data(df: pl.DataFrame) -> pl.DataFrame:
    """Common transformation logic for weather data normalization."""
    # Normalize the nested JSON structure
    # Explode the hourly arrays into separate rows
    df = (
        df.select(
            [
                pl.col("latitude"),
                pl.col("longitude"),
                pl.col("timezone"),
                pl.col("elevation"),
                pl.col("hourly").struct.field("time").alias("time"),
                pl.col("hourly").struct.field("temperature_2m").alias("temperature_2m"),
                pl.col("hourly").struct.field("precipitation").alias("precipitation"),
                pl.col("hourly").struct.field("windspeed_10m").alias("windspeed_10m"),
                pl.col("hourly").struct.field("snowfall").alias("snowfall"),
                pl.col("hourly").struct.field("rain").alias("rain"),
                pl.col("hourly").struct.field("showers").alias("showers"),
                pl.col("hourly").struct.field("snow_depth").alias("snow_depth"),
                pl.col("hourly").struct.field("visibility").alias("visibility"),
                pl.col("extracted_at"),
            ]
        )
        .explode(
            [
                "time",
                "temperature_2m",
                "precipitation",
                "windspeed_10m",
                "snowfall",
                "rain",
                "showers",
                "snow_depth",
                "visibility",
            ]
        )
        .with_columns(
            pl.col("time").str.strptime(pl.Datetime, "%Y-%m-%dT%H:%M"),
        )
    )

    # Create a mapping dataframe from lat/lon to city_code
    # Round coordinates to nearest integer for fuzzy matching (city-level granularity)
    city_mapping = pl.DataFrame(
        [
            {
                "latitude_rounded": round(city.latitude, 0),
                "longitude_rounded": round(city.longitude, 0),
                "city_code": code,
            }
            for code, city in CITIES.items()
        ]
    )

    # Round coordinates in the data for joining
    df = df.with_columns(
        [
            pl.col("latitude").round(0).alias("latitude_rounded"),
            pl.col("longitude").round(0).alias("longitude_rounded"),
        ]
    )

    # Join to add city_code based on rounded lat/lon
    df = df.join(city_mapping, on=["latitude_rounded", "longitude_rounded"], how="left").drop(
        ["latitude_rounded", "longitude_rounded"]
    )

    # Reorder columns
    df = df.select(
        [
            "city_code",
            "latitude",
            "longitude",
            "timezone",
            "elevation",
            "time",
            "temperature_2m",
            "precipitation",
            "windspeed_10m",
            "snowfall",
            "rain",
            "showers",
            "snow_depth",
            "visibility",
            "extracted_at",
        ]
    )

    # Dedup based on city_code and time, keeping the latest extracted_at
    df = df.sort(["city_code", "time", "extracted_at"], descending=[False, False, True])
    df = df.unique(subset=["city_code", "time"], keep="first")

    return df


@asset(
    key_prefix=["staged"],
    group_name="weather_etl",
    kinds={"polars"},
    deps=[historical_forecast, hourly_forecast],
    automation_condition=dg.AutomationCondition.eager(),
)
def hourly_forecast_table():
    """Hourly forecast table normalized from raw JSON data."""
    path = forecast_path(date=None) / "**" / "*.parquet"

    df = pl.read_parquet(path.uri, missing_columns="insert").pipe(transform_weather_data)
    # Load back to S3 as a single Parquet file
    output_lake_path = staged_path / "hourly_forecast_table.parquet"
    df.write_parquet(output_lake_path.uri)

    # Prepare metadata
    columns = [dg.TableColumn(name=col, type=str(dtype)) for (col, dtype) in df.schema.items()]

    return dg.MaterializeResult(
        metadata={
            "column_schema": dg.TableSchema(columns=columns),
            "row_count": MetadataValue.int(df.height),
        }
    )


@asset(
    key_prefix=["staged"],
    group_name="weather_etl",
    kinds={"polars"},
    deps=[historical_actual_weather, actual_weather],
    automation_condition=dg.AutomationCondition.eager(),
)
def actual_weather_table():
    """Actual weather table normalized from raw JSON data."""

    actual_path = forecast_path(date=None) / "**" / "*.parquet"

    # Use Polars to scan all Parquet files directly from S3 with glob pattern
    df = pl.read_parquet(actual_path.uri, missing_columns="insert").pipe(transform_weather_data)

    # Load back to S3 as a single Parquet file
    output_lake_path = staged_path / "actual_weather_table.parquet"
    df.write_parquet(output_lake_path.uri)

    # Prepare metadata
    columns = [dg.TableColumn(name=col, type=str(dtype)) for (col, dtype) in df.schema.items()]

    return dg.MaterializeResult(
        metadata={
            "column_schema": dg.TableSchema(columns=columns),
            "row_count": MetadataValue.int(df.height),
        }
    )
