from datetime import date, datetime
from pathlib import Path
from typing import Iterator, cast
from unittest.mock import MagicMock, patch

import dagster as dg
import polars as pl
import pytest

from weather_etl.defs.assets import (
    current_weather_table,
    daily_hourly_forecast_schedule,
    hourly_forecast_table,
)
from weather_etl.models import CITIES


@pytest.fixture
def sample_hourly_data():
    """Load sample hourly forecast data for testing."""
    sample_path = Path(__file__).parent / "samples" / "hourly_forecast_20251129.parquet"
    return pl.read_parquet(sample_path)


@pytest.fixture
def sample_current_data():
    """Load sample current weather data for testing."""
    sample_path = Path(__file__).parent / "samples" / "current_weather_20251129.parquet"
    return pl.read_parquet(sample_path)


@patch("weather_etl.defs.assets.pl.DataFrame.write_parquet", autospec=True)
@patch("weather_etl.defs.assets.pl.read_parquet")
def test_hourly_forecast_table(
    mock_read_parquet: MagicMock,
    mock_write_parquet: MagicMock,
    sample_hourly_data: pl.DataFrame,
):
    """Test that hourly_forecast_table processes raw data correctly."""
    # Mock input data
    mock_read_parquet.return_value = sample_hourly_data

    # Execute
    hourly_forecast_table()

    assert mock_read_parquet.called
    assert mock_write_parquet.called

    written_df: pl.DataFrame = mock_write_parquet.call_args[0][0]

    expected_columns = [
        "city_code",
        "forecast_timestamp",
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
    ]

    assert set(written_df.columns) == set(expected_columns)
    assert written_df.height > 0
    assert written_df["city_code"][0] == "PHIL"
    # Check time conversion
    assert written_df["time"].dtype == pl.Datetime


@patch("weather_etl.defs.assets.pl.DataFrame.write_parquet", autospec=True)
@patch("weather_etl.defs.assets.pl.read_parquet")
def test_current_weather_table(
    mock_read_parquet: MagicMock,
    mock_write_parquet: MagicMock,
    sample_current_data: pl.DataFrame,
):
    """Test that current_weather_table processes raw data correctly."""
    mock_read_parquet.return_value = sample_current_data

    # Execute
    current_weather_table()

    assert mock_read_parquet.called
    assert mock_write_parquet.called

    # Verify the dataframe passed to write_parquet
    written_df: pl.DataFrame = mock_write_parquet.call_args[0][0]

    expected_columns = [
        "city_code",
        "latitude",
        "longitude",
        "timezone",
        "elevation",
        "time",
        "interval",
        "temperature_2m",
        "precipitation",
        "windspeed_10m",
        "snowfall",
        "rain",
        "showers",
        "snow_depth",
        "visibility",
    ]

    assert set(written_df.columns) == set(expected_columns)
    assert written_df.height > 0
    assert written_df["city_code"][0] == "PHIL"
    assert written_df["time"].dtype == pl.Datetime


@pytest.mark.parametrize(
    "schedule",
    [
        datetime(2025, 11, 29),
    ],
)
def test_daily_hourly_forecast_schedule(schedule: datetime):
    context = dg.build_schedule_context(scheduled_execution_time=schedule)
    run_requests = list(
        cast(Iterator[dg.RunRequest], daily_hourly_forecast_schedule(context))
    )

    # Expecting 8 days (Nov 29 to Dec 6 inclusive) * number of cities
    assert len(run_requests) == 8 * len(CITIES)

    s_requests = sorted(
        run_requests,
        key=lambda rr: datetime.strptime(
            cast(dg.MultiPartitionKey, rr.partition_key).keys_by_dimension["date"],
            "%Y-%m-%d",
        ),
    )
    first_request = s_requests[0]
    last_request = s_requests[-1]

    assert first_request.partition_key and "2025-11-29" in first_request.partition_key
    assert last_request.partition_key and "2025-12-06" in last_request.partition_key
