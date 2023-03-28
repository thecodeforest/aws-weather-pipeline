from typing import Tuple
from datetime import datetime, timedelta
import awswrangler as wr
from meteostat import Point, Hourly
import pandas as pd
import sys
import os
from pathlib import Path

# get the path to the directory containing the current script
script_dir = Path(__file__).resolve().parent
# add the path to the directory containing the module to the Python path
sys.path.append(str(script_dir.parent))

from app.conf import cities  # noqa: E402

raw_data_bucket = 'raw-city-weather-data-1'


def make_s3_weather_path(bucket: str, city: str, state: str, lat: float, lon: float, dt: str) -> str:
    city = city.lower().replace(' ', '-')
    state = state.lower().replace(' ', '-')
    path = f"s3://{bucket}/dt={dt}/{city}_{state}_{lat}_{lon}.csv"
    return path


def create_start_and_end_dts(bucket: str) -> Tuple[datetime, datetime]:
    keys = wr.s3.list_objects(f"s3://{bucket}/")
    # get the 2nd to last value in the list
    dts = sorted(set([key.split("/")[-2].replace("dt=", "") for key in keys]))
    last_complete_load_dt = dts[-2]
    # convert last_complete_load_dt to datetime
    start = datetime.strptime(last_complete_load_dt, "%Y-%m-%d").date()
    start = datetime(start.year, start.month, start.day, 0)
    end = (datetime.now() - timedelta(days=1)).date()
    end = datetime(end.year, end.month, end.day, 23)
    return start, end


def collect_historical_weather_data(lat: float, lon: float, city: str, state: str, start: datetime, end: datetime) -> pd.DataFrame:
    location = Point(lat, lon)
    data = Hourly(location, start, end)
    data = data.fetch()
    data = data.reset_index()
    data['lat'] = lat
    data['lon'] = lon
    data['city'] = city
    data['state'] = state
    data = data[['lat', 'lon', 'city', 'state',
                 'time', 'temp', 'rhum', 'prcp', 'wspd']]
    return data


def main():
    start_dt, end_dt = create_start_and_end_dts(bucket=raw_data_bucket)
    for lat_lon, city_tuple in cities.items():
        lat = lat_lon[0]
        lon = lat_lon[1]
        city = city_tuple.city
        state = city_tuple.state
        city_weather_data = collect_historical_weather_data(lat=lat,
                                                            lon=lon,
                                                            city=city,
                                                            state=state,
                                                            start=start_dt,
                                                            end=end_dt
                                                            )
        for dt in city_weather_data['time'].dt.date.unique():
            daily_weather_data = city_weather_data[city_weather_data['time'].dt.date == dt]
            path = make_s3_weather_path(bucket=raw_data_bucket,
                                        city=city,
                                        state=state,
                                        lat=lat,
                                        lon=lon,
                                        dt=dt
                                        )
            print(dt, path, city, state)
            wr.s3.to_csv(daily_weather_data, path, index=False)
            break
        break


if __name__ == "__main__":
    main()
