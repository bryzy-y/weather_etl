from datetime import date
from enum import StrEnum
from typing import Self

from pydantic import BaseModel, model_validator

API_URL = "https://api.open-meteo.com/v1"


class City(BaseModel):
    name: str
    latitude: float
    longitude: float


CITIES = {
    "NYC": City(name="New York City", latitude=40.7128, longitude=-74.0060),
    "PHIL": City(name="Philadelphia", latitude=39.9526, longitude=-75.1652),
    "CHI": City(name="Chicago", latitude=41.8781, longitude=-87.6298),
    "DC": City(name="Washington DC", latitude=38.9072, longitude=-77.0369),
}


class WeatherVars(StrEnum):
    TEMPERATURE_2M = "temperature_2m"
    PRECIPITATION = "precipitation"
    WINDSPEED_10M = "windspeed_10m"
    SNOWFALL = "snowfall"
    RAIN = "rain"
    SHOWERS = "showers"
    SNOW_DEPTH = "snow_depth"
    VISIBILITY = "visibility"

    @classmethod
    def default(cls) -> list["WeatherVars"]:
        return [member for member in WeatherVars]


class ForecastParams(BaseModel):
    city: City
    start_date: date | None = None
    end_date: date | None = None
    temperature_unit: str = "celsius"
    windspeed_unit: str = "kmh"
    precipitation_unit: str = "mm"
    timezone: str = "auto"
    hourly: list["WeatherVars"] | None = None
    current: list["WeatherVars"] | None = None

    @model_validator(mode="after")
    def check_hourly_or_current(self) -> Self:
        if not self.hourly and not self.current:
            raise ValueError(
                "At least one of 'hourly' or 'current' must be provided."
            )
        return self

    def to_query_params(self) -> dict[str, str]:
        params: dict[str, str] = {
            "latitude": str(self.city.latitude),
            "longitude": str(self.city.longitude),
            "temperature_unit": self.temperature_unit,
            "windspeed_unit": self.windspeed_unit,
            "precipitation_unit": self.precipitation_unit,
            "timezone": self.timezone,
        }
        if self.start_date:
            params["start_date"] = self.start_date.isoformat()
        if self.end_date:
            params["end_date"] = self.end_date.isoformat()
        if self.hourly:
            params["hourly"] = ",".join([field.value for field in self.hourly])
        if self.current:
            params["current"] = ",".join([field.value for field in self.current])
        return params
