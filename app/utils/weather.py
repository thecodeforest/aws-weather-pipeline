from typing import Tuple
from datetime import datetime, timedelta
import awswrangler as wr
from meteostat import Point, Hourly
import pandas as pd


def make_s3_weather_path(bucket: str, city: str, state: str, lat: float, lon: float, dt: str) -> str:
    city = city.lower().replace(" ", "-")
    state = state.lower().replace(" ", "-")
    path = f"s3://{bucket}/dt={dt}/{city}_{state}_{lat}_{lon}.csv"
    return path


def create_start_and_end_dts(bucket: str) -> Tuple[datetime, datetime]:
    keys = wr.s3.list_objects(f"s3://{bucket}/")
    # get the the last value in the list
    dts = sorted(set([key.split("/")[-2].replace("dt=", "") for key in keys]))
    last_load_dt = dts[-1]
    # convert last_complete_load_dt to datetime
    start = datetime.strptime(last_load_dt, "%Y-%m-%d").date()
    start = datetime(start.year, start.month, start.day, 0)
    end = (datetime.now() - timedelta(days=1)).date()
    end = datetime(end.year, end.month, end.day, 23)
    if start > end:
        end = datetime(start.year, start.month, start.day, 23)
    # ensure that end date is not less than start date
    return start, end


def collect_historical_weather_data(
    lat: float, lon: float, city: str, state: str, start: datetime, end: datetime
) -> pd.DataFrame:
    location = Point(lat, lon)
    print("collecting the hourly weather data below")
    data = Hourly(location, start, end)
    data = data.fetch()
    data = data.reset_index()
    data["lat"] = lat
    data["lon"] = lon
    data["city"] = city
    data["state"] = state
    data = data[["lat", "lon", "city", "state", "time", "temp", "rhum", "prcp", "wspd"]]
    return data
