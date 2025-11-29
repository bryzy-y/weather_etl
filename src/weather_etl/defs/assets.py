import itertools
from datetime import datetime, timedelta, timezone
from io import BytesIO
from typing import TYPE_CHECKING, cast

import dagster as dg
import polars as pl
from dagster import AssetExecutionContext, MetadataValue, RetryPolicy, asset
from dagster_aws.s3 import S3Resource

from weather_etl.defs.resources import WeatherApiClient
from weather_etl.lake_path import raw_path, staged_path
from weather_etl.models import API_URL, CITIES

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client


daily_partition = dg.DailyPartitionsDefinition(
    # We want to backfill the last 60 days of data at most
    start_date=datetime.now(timezone.utc) - timedelta(days=60),
    end_offset=8,  # Allow scheduling up to 7 days in the future
)
# Static partitioning by region code
city_partition = dg.StaticPartitionsDefinition(list(CITIES.keys()))
# Combine both partitioning schemes for the forecast assets
multi_partitions = dg.MultiPartitionsDefinition(
    {
        "date": daily_partition,
        "city": city_partition,
    }
)


@asset(
    key_prefix=["raw"],
    retry_policy=RetryPolicy(max_retries=3, delay=10),
    group_name="weather_etl",
    partitions_def=multi_partitions,
    metadata={
        "owner": "ybryz",
        "endpoint": MetadataValue.url(API_URL + "/forecast"),
    },
    kinds={"s3"},
)
def raw_hourly_forecast(
    context: AssetExecutionContext, weather_api_client: WeatherApiClient, s3: S3Resource
):
    """Fetches hourly forecast data from the weather API."""
    key = cast(dg.MultiPartitionKey, context.partition_key)

    # Get partition dims
    date_dim = key.keys_by_dimension["date"]
    city_dim = key.keys_by_dimension["city"]

    dt = datetime.strptime(date_dim, "%Y-%m-%d").date()
    city = CITIES[city_dim]

    data = weather_api_client.hourly_forecast(city=city, start_date=dt, end_date=dt)

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    lake_path = (
        raw_path
        / "hourly_forecast"
        / city_dim
        / date_dim
        / f"hourly_forecast_{timestamp}.parquet"
    )

    # Add metadata fields
    data["forecast_timestamp"] = timestamp
    data["city_code"] = city_dim

    # Prepare S3 client
    s3_client: S3Client = s3.get_client()

    # Compress data and upload to S3
    with BytesIO() as byte_stream:
        df = pl.from_dict(data)
        df.write_parquet(byte_stream)
        byte_stream.seek(0)

        s3_client.put_object(
            Bucket=lake_path.bucket, Key=lake_path.key, Body=byte_stream
        )

    return dg.MaterializeResult(
        metadata={
            "uri": MetadataValue.text(lake_path.uri),
        }
    )


@asset(
    key_prefix=["raw"],
    retry_policy=RetryPolicy(max_retries=3, delay=10),
    group_name="weather_etl",
    partitions_def=city_partition,
    metadata={
        "owner": "ybryz",
        "endpoint": MetadataValue.url(API_URL + "/forecast"),
    },
    automation_condition=dg.AutomationCondition.on_cron("@hourly"),
    kinds={"s3"},
)
def raw_current_weather(
    context: AssetExecutionContext, weather_api_client: WeatherApiClient, s3: S3Resource
):
    """Fetches current weather data from the weather API."""
    city = CITIES[context.partition_key]
    data = weather_api_client.current_weather(city)

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    lake_path = (
        raw_path
        / "current_weather"
        / context.partition_key
        / f"current_weather_{timestamp}.parquet"
    )

    # Add metadata fields
    data["city_code"] = context.partition_key

    s3_client: S3Client = s3.get_client()
    with BytesIO() as byte_stream:
        df = pl.from_dict(data)
        df.write_parquet(byte_stream)
        byte_stream.seek(0)

        s3_client.put_object(
            Bucket=lake_path.bucket, Key=lake_path.key, Body=byte_stream
        )

    return dg.MaterializeResult(
        metadata={
            "uri": MetadataValue.text(lake_path.uri),
        }
    )


@asset(
    key_prefix=["staged"],
    group_name="weather_etl",
    kinds={"polars"},
    deps=[raw_hourly_forecast],
    automation_condition=dg.AutomationCondition.eager(),
)
def hourly_forecast_table():
    """Hourly forecast table normalized from raw JSON data."""
    lake_path = raw_path / "hourly_forecast" / "**" / "*.parquet"

    # Use Polars to scan all Parquet files directly from S3 with glob pattern
    # Polars handles gzip automatically
    df = pl.read_parquet(lake_path.uri, missing_columns="insert")

    # Normalize the nested JSON structure
    # Explode the hourly arrays into separate rows
    df = df.select(
        [
            pl.col("city_code"),
            pl.col("forecast_timestamp"),
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
        ]
    ).explode(
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

    # Convert time column to datetime
    df = df.with_columns([pl.col("time").str.strptime(pl.Datetime, "%Y-%m-%dT%H:%M")])

    # Load back to S3 as a single Parquet file
    output_lake_path = staged_path / "hourly_forecast_table.parquet"
    df.write_parquet(output_lake_path.uri)

    # Prepare metadata
    columns = [
        dg.TableColumn(name=col, type=str(dtype)) for (col, dtype) in df.schema.items()
    ]

    return dg.MaterializeResult(
        metadata={
            "uri": dg.MetadataValue.url(output_lake_path.uri),
            "column_schema": dg.TableSchema(columns=columns),
            "row_count": MetadataValue.int(df.height),
        }
    )


@asset(
    key_prefix=["staged"],
    group_name="weather_etl",
    kinds={"polars"},
    deps=[raw_current_weather],
    automation_condition=dg.AutomationCondition.eager(),
)
def current_weather_table():
    """Current weather table normalized from raw JSON data."""
    base_path = raw_path / "current_weather"
    glob_pattern = f"{base_path.uri}/**/*.parquet"

    # Use Polars to scan all Parquet files directly from S3 with glob pattern
    df = pl.read_parquet(glob_pattern)

    # Normalize the nested JSON structure - flatten the 'current' struct
    df = df.select(
        [
            pl.col("city_code"),
            pl.col("latitude"),
            pl.col("longitude"),
            pl.col("timezone"),
            pl.col("elevation"),
            pl.col("current").struct.field("time").alias("time"),
            pl.col("current").struct.field("interval").alias("interval"),
            pl.col("current").struct.field("temperature_2m").alias("temperature_2m"),
            pl.col("current").struct.field("precipitation").alias("precipitation"),
            pl.col("current").struct.field("windspeed_10m").alias("windspeed_10m"),
            pl.col("current").struct.field("snowfall").alias("snowfall"),
            pl.col("current").struct.field("rain").alias("rain"),
            pl.col("current").struct.field("showers").alias("showers"),
            pl.col("current").struct.field("snow_depth").alias("snow_depth"),
            pl.col("current").struct.field("visibility").alias("visibility"),
        ]
    )

    # Convert time column to datetime
    df = df.with_columns([pl.col("time").str.strptime(pl.Datetime, "%Y-%m-%dT%H:%M")])

    # Write to S3 as a single Parquet file
    output_lake_path = staged_path / "current_weather_table.parquet"
    df.write_parquet(output_lake_path.uri)

    # Prepare metadata
    columns = [
        dg.TableColumn(name=col, type=str(dtype)) for (col, dtype) in df.schema.items()
    ]

    return dg.MaterializeResult(
        metadata={
            "uri": dg.MetadataValue.url(output_lake_path.uri),
            "column_schema": dg.TableSchema(columns=columns),
            "row_count": MetadataValue.int(df.height),
        }
    )


@dg.schedule(cron_schedule="0 6 * * *", target=[raw_hourly_forecast])
def daily_hourly_forecast_schedule(context: dg.ScheduleEvaluationContext):
    """Schedule to run the hourly forecast asset daily at 6 AM UTC."""
    start_date = context.scheduled_execution_time.date().strftime("%Y-%m-%d")
    end_date = daily_partition.get_last_partition_key()

    assert end_date is not None, "End date for daily partitioning should not be None"

    # Generate run requests for each date from the current scheduled execution date to the last available partition date.
    date_range = daily_partition.get_partition_keys_in_range(
        dg.PartitionKeyRange(start=start_date, end=end_date)
    )

    for date, city_code in itertools.product(
        date_range, city_partition.get_partition_keys()
    ):
        yield dg.RunRequest(
            run_key=f"hourly_forecast_{city_code}_{date}",
            partition_key=dg.MultiPartitionKey({"date": date, "city": city_code}),
        )
