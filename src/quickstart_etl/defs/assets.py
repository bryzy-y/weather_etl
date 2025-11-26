from io import BytesIO
import json

from dagster import AssetExecutionContext, MaterializeResult, MetadataValue, asset, RetryPolicy
from .resources import WeatherApiClient
from .models import City



@asset(
    retry_policy=RetryPolicy(max_retries=3, delay=10), 
    group_name="weather_etl",
    kinds={"s3"}
)
async def weather_forecast(context: AssetExecutionContext, weather_api_client: WeatherApiClient):
    """Fetches weather forecast data for New York City."""
    city = City(name="New York", latitude=40.7128, longitude=-74.0060)
    data = await weather_api_client.get_weather_data(city)

    context.log.info(f"Fetched weather data for {city.name}")
    context.log.info(json.dumps(data, indent=4))

    return data
    
