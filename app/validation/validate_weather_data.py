import pandas as pd
import pandera as pa
from typing import Tuple
from pathlib import Path
from datetime import datetime
import sys

# get the path to the directory containing the current script
script_dir = Path(__file__).resolve().parent
# add the path to the directory containing the module to the Python path
sys.path.append(str(script_dir.parent))

from app.conf import cities  # noqa: E402


def validate_city_weather_data(df: pd.DataFrame, start_dt: datetime, end_dt: datetime) -> Tuple[bool, pd.DataFrame]:
    """Validate weather data using pandera schema."""
    schema = pa.DataFrameSchema(
        {  # check that lat and lon are floats
            "lat": pa.Column(pa.Float),
            "lon": pa.Column(pa.Float),
            "city": pa.Column(pa.String, checks=pa.Check.isin([city.city for city in cities.values()])),
            "state": pa.Column(
                pa.String,
                checks=pa.Check.isin([city.state for city in cities.values()]),
            ),
            "time": pa.Column(pa.DateTime, checks=pa.Check.in_range(start_dt, end_dt)),
            "temp": pa.Column(
                pa.Float,
            ),
            "rhum": pa.Column(pa.Float),
            "prcp": pa.Column(pa.Float),
            "wspd": pa.Column(pa.Float, checks=pa.Check.ge(0)),
        },
        coerce=True,
    )

    try:
        schema.validate(df)
        return True, None
    except pa.errors.SchemaError as err:
        return False, err.failure_cases
