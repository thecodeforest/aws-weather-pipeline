import os
import awswrangler as wr
from utils.weather import (
    make_s3_weather_path,
    create_start_and_end_dts,
    collect_historical_weather_data,
)
from utils.logger import Logger  # noqa: F401
from validation.validate_weather_data import validate_city_weather_data
from conf import cities
import boto3  # noqa: F401


try:
    environment = os.environ["AWS_LAMBDA_FUNCTION_NAME"]
    output_bucket = os.environ["OUTPUT_BUCKET"]
    glue_workflow_name = os.environ["GLUE_WORKFLOW_NAME"]
except KeyError:
    environment = "local"
    output_bucket = "raw-city-weather-data-1"
    glue_workflow_name = None


def weather_collector(event, context):
    logger = Logger(os.path.join("/tmp", "weather.log"))
    logger.log_info("Starting weather collector")
    logger.log_info(f"Environment: {environment}")
    print("starting weather collector")
    start_dt, end_dt = create_start_and_end_dts(bucket=output_bucket)
    for lat_lon, city_tuple in cities.items():
        lat = lat_lon[0]
        lon = lat_lon[1]
        city = city_tuple.city
        state = city_tuple.state
        print(f"Collecting weather data for {city} {state}")
        city_weather_data = collect_historical_weather_data(
            lat=lat, lon=lon, city=city, state=state, start=start_dt, end=end_dt
        )

        logger.log_info(f"Received {city_weather_data.shape[0]} rows of weather data for {city} {state}")
        has_passed, error_cases = validate_city_weather_data(city_weather_data, start_dt, end_dt)
        if not has_passed:
            logger.log_exception(ValueError("Received invalid weather data\n", error_cases))
            raise ValueError
        for dt in city_weather_data["time"].dt.date.unique():
            daily_weather_data = city_weather_data[city_weather_data["time"].dt.date == dt]
            if environment == "local":
                print(daily_weather_data.head())
                return None
            path = make_s3_weather_path(bucket=output_bucket, city=city, state=state, lat=lat, lon=lon, dt=dt)
            wr.s3.to_csv(daily_weather_data, path, index=False)
        logger.log_info(f"Finished weather collector for {city} {state}")
    logger.log_info("Finished weather collector")
    glue = boto3.client("glue")
    response = glue.start_workflow_run(Name=glue_workflow_name)
    workflow_run_id = response["RunId"]
    print(f"Started workflow run {workflow_run_id}")
    return {"statusCode": 200, "body": f"Started workflow run {workflow_run_id}"}
