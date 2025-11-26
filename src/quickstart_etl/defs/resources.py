import httpx
from pydantic import PrivateAttr, HttpUrl
from contextlib import asynccontextmanager
import dagster as dg
from .models import City

class WeatherApiClient(dg.ConfigurableResource):
    """A simple client for the Open-Meteo weather API.

    See https://open-meteo.com/en/docs for more information.
    """

    base_url: str = "https://api.open-meteo.com/v1"
    _client: httpx.AsyncClient = PrivateAttr()  


    @asynccontextmanager
    async def yield_for_execution(self, context: dg.InitResourceContext): # pyright: ignore
        self._client = httpx.AsyncClient(base_url=str(self.base_url))
        try:
            yield self
        finally:
            await self._client.aclose()

    async def get_weather_data(self, city: City) -> dict:
        params = {
            "latitude": city.latitude,
            "longitude": city.longitude,
        }
        response = await self._client.get("forecast", params=params)
        response.raise_for_status()
        return response.json()



@dg.definitions
def resources():
    return dg.Definitions(
        resources={
            "weather_api_client": WeatherApiClient(),
        }
    )