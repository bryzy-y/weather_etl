from pathlib import Path

import polars as pl
import pytest

from weather_etl.defs.assets import transform_weather_data


@pytest.mark.parametrize(
    "sample_file",
    [
        "hourly_forecast_20251201.parquet",
        "actual_weather_20251201.parquet",
    ],
)
def test_transform_weather_data(sample_file: str):
    """Test that transform_weather_data processes raw data correctly."""
    # Load sample data
    sample_path = Path(__file__).parent / "samples" / sample_file
    raw_df = pl.read_parquet(sample_path)

    # Execute transformation
    transformed_df = transform_weather_data(raw_df)

    # Verify the dataframe structure
    expected_columns = [
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

    assert set(transformed_df.columns) == set(expected_columns), f"Columns mismatch. Got: {transformed_df.columns}"
    assert transformed_df.height > 0, "DataFrame should have rows"

    # Check data types
    assert transformed_df["time"].dtype == pl.Datetime
    assert transformed_df["temperature_2m"].dtype in [pl.Float64, pl.Float32]
    assert transformed_df["precipitation"].dtype in [pl.Float64, pl.Float32]
    assert transformed_df["windspeed_10m"].dtype in [pl.Float64, pl.Float32]
    assert transformed_df["city_code"].dtype == pl.String

    # Verify city_code is properly assigned
    assert transformed_df["city_code"].null_count() == 0, "All rows should have a city_code"
    # Verify we have valid city codes
    valid_cities = {"NYC", "PHIL", "CHI", "DC"}
    assert transformed_df["city_code"].unique().to_list()[0] in valid_cities

    # Check deduplication - should have unique city_code + time combinations
    assert (
        transformed_df.select(["city_code", "time"]).n_unique() == transformed_df.height
    ), "Should have unique city_code + time combinations"

    # Verify exploding worked correctly (should have multiple rows)
    assert transformed_df.height > 1, "Should have multiple rows after exploding"
