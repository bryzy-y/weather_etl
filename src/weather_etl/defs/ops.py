from datetime import date

import dagster as dg
import httpx
import polars as pl
from dagster import OpExecutionContext

from weather_etl.lake_path import staged_path
from weather_etl.models import CITIES


class DiscordWebhook(dg.Config):
    """Configuration for Discord webhook."""

    url: str


@dg.op
def hourly_forecast_to_discord(
    context: OpExecutionContext,
    config: DiscordWebhook,
) -> None:
    """
    Send hourly weather forecast to a Discord channel via webhook.
    """
    city_code = "PHIL"
    forecast_date = date.today()

    city = CITIES[city_code]
    context.log.info(f"Fetching hourly forecast for {city.name} on {forecast_date}")

    # Read from staged S3 table
    table_path = staged_path / "hourly_forecast_table.parquet"

    # Read and filter the data - Polars uses AWS credentials from environment
    df = pl.read_parquet(table_path.uri)

    # Filter for specific city and date
    df = df.filter((pl.col("city_code") == city_code) & (pl.col("time").dt.date() == forecast_date)).sort("time")

    if df.height == 0:
        context.log.warning(f"No forecast data found for {city.name} on {forecast_date}")

    # Extract hourly data
    times = df["time"].to_list()
    temps = df["temperature_2m"].to_list()
    precip = df["precipitation"].to_list()
    wind = df["windspeed_10m"].to_list()

    # Build Discord message
    forecast_lines = []
    forecast_lines.append(f"**🌤️ Weather Forecast for {city.name}**")
    forecast_lines.append(f"📅 Date: {forecast_date}")
    lat_hemisphere = "N" if city.latitude >= 0 else "S"
    lon_hemisphere = "E" if city.longitude >= 0 else "W"
    forecast_lines.append(
        f"🌍 Location: {abs(city.latitude):.4f}°{lat_hemisphere}, {abs(city.longitude):.4f}°{lon_hemisphere}"
    )
    forecast_lines.append("")
    forecast_lines.append("**Hourly Forecast:**")
    forecast_lines.append("```")
    forecast_lines.append(f"{'Time':<8} {'Temp':<8} {'Rain':<8} {'Wind':<8}")
    forecast_lines.append("-" * 40)

    for i in range(len(times)):  # Show every hour
        time_str = times[i].strftime("%H:%M") if hasattr(times[i], "strftime") else str(times[i])[-5:]
        temp = f"{temps[i]:.1f}°C" if i < len(temps) else "N/A"
        rain = f"{precip[i]:.1f}mm" if i < len(precip) else "N/A"
        wind_speed = f"{wind[i]:.1f}km/h" if i < len(wind) else "N/A"
        forecast_lines.append(f"{time_str:<8} {temp:<8} {rain:<8} {wind_speed:<8}")

    forecast_lines.append("```")

    message = "\n".join(forecast_lines)
    # Send to Discord webhook
    payload = {"content": message, "username": "Weather Bot"}
    with httpx.Client() as client:
        response = client.post(config.url, json=payload)
        response.raise_for_status()

    context.log.info("Successfully sent forecast to Discord")


@dg.job(
    config=dg.RunConfig(ops={"hourly_forecast_to_discord": DiscordWebhook(url=dg.EnvVar("DISCORD_WEBHOOK_URL"))})
)
def hourly_forecast_to_discord_job() -> None:
    """Send a message to a Discord channel via webhook."""
    hourly_forecast_to_discord()


@dg.asset_sensor(
    asset_key=dg.AssetKey(["staged", "hourly_forecast_table"]),
    job=hourly_forecast_to_discord_job,
    default_status=dg.DefaultSensorStatus.RUNNING,
)
def hourly_forecast_to_discord_sensor(context: dg.SensorEvaluationContext):
    """Sensor to trigger Discord report when hourly forecast table is updated."""
    return dg.RunRequest(run_key=context.cursor)
