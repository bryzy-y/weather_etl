from pydantic import BaseModel

class City(BaseModel):
    name: str
    latitude: float
    longitude: float


NYC = City(name="New York City", latitude=40.7128, longitude=-74.0060)
LA = City(name="Los Angeles", latitude=34.0522, longitude=-118.2437)
PHIL = City(name="Philadelphia", latitude=39.9526, longitude=-75.1652)