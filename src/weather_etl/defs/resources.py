from contextlib import contextmanager
from datetime import date

import dagster as dg
import httpx
from dagster_aws.s3 import S3Resource
from pydantic import PrivateAttr

from weather_etl.models import API_URL, City, ForecastParams, WeatherVars


class WeatherApiClient(dg.ConfigurableResource):
    """A simple client for the Open-Meteo weather API.

    See https://open-meteo.com/en/docs for more information.
    """

    base_url: str = API_URL
    concurrent_requests: int = 3

    _client: httpx.Client = PrivateAttr()

    @contextmanager
    def yield_for_execution(self, context: dg.InitResourceContext):
        """Yields an HTTPX client configured for the weather API."""
        try:
            self._client = httpx.Client(base_url=self.base_url)
            yield self
        finally:
            self._client.close()

    def hourly_forecast(self, city: City, start_date: date, end_date: date) -> dict:
        params = ForecastParams(
            city=city,
            start_date=start_date,
            end_date=end_date,
            hourly=WeatherVars.default(),
        )

        response = self._client.get("forecast", params=params.to_query_params())
        response.raise_for_status()
        return response.json()

    def current_weather(self, city: City) -> dict:
        params = ForecastParams(city=city, current=WeatherVars.default())

        response = self._client.get("forecast", params=params.to_query_params())
        response.raise_for_status()
        return response.json()


@dg.definitions
def resources():
    return dg.Definitions(
        resources={
            "weather_api_client": WeatherApiClient(),
            "s3": S3Resource(
                aws_access_key_id=dg.EnvVar("AWS_ACCESS_KEY_ID"),
                aws_secret_access_key=dg.EnvVar("AWS_SECRET_ACCESS_KEY"),
                region_name=dg.EnvVar("AWS_REGION"),
            ),
        }
    )
