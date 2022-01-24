import config
import datetime
from pyowm.weatherapi25.weather import Weather
from sqlalchemy.orm.session import Session
from typing import Union
from knowledge_base.kb import Station
from pyowm.owm import OWM

def open_weather() -> OWM:
    open_weather_map = OWM(config.OPEN_WEATHER_MAP_API_KEY)
    return open_weather_map

def get_weather_at_lat_long(owm: OWM,
                            date_and_time: datetime.datetime,
                            lat: float, long: float) -> Union[str, None]:
    forecast = owm.weather_manager().forecast_at_coords(lat, long, '3h')
    if forecast is None:
        return None

    try:
        weather_or_none = forecast.get_weather_at(date_and_time)
        if weather_or_none is None:
            return None
    except:
        return None

    weather: Weather = weather_or_none
    return weather.detailed_status

def get_weather_at_crs(db: Session, owm: OWM, 
                       date_and_time: datetime.datetime,
                       crs: str) -> Union[str, None]:
    result = db.query(Station.latitude, Station.longitude)\
        .filter(Station.crs_code == crs)\
        .first()
    if result is None:
        return None
    
    lat, long = result
    return get_weather_at_lat_long(
        owm, date_and_time, lat, long)

