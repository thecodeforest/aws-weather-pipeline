from awsglue.transforms import *
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import lower, concat, lit, max, date_format


def convert_celcius_to_fahrenheit(column):
    return (column * 1.8) + 32


sc = SparkContext.getOrCreate()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)

processed_bucket_name = "clean-city-weather-data-1"
region = "us-west-2"
db_name = "daily-weather-db"
table_name = "daily-weather-tableraw_city_weather_data_1"

# Read data into a DynamicFrame using the Data Catalog metadata
weather_dyf = glue_context.create_dynamic_frame.from_catalog(
    database=db_name, table_name=table_name
)


# convert to a spark dataframe
weather_df = weather_dyf.toDF()
# convert the temperature column from celcius to fahrenheit
weather_df = weather_df.withColumn(
    "temp_farenheit", convert_celcius_to_fahrenheit(weather_df.temp))

# convert the city column to lower-case
weather_df = weather_df.withColumn(
    "city", lower(weather_df.city)
)

# convert the state column to lower-case
weather_df = weather_df.withColumn(
    "state", lower(weather_df.state)
)
# concatenate the city and state to make ts-id
weather_df = weather_df.withColumn("ts_id", concat("city", lit("-"), "state"))
weather_df = weather_df.orderBy("ts_id", "time")

most_recent_load_dt = weather_df.agg(max(date_format(
    'time', 'yyyy-MM-dd')).alias('max_date')).collect()[0]['max_date']

# convert back to a DynamicFrame
weather_tmp = DynamicFrame.fromDF(weather_df,
                                  glue_context,
                                  "flat"
                                  )
# Rename, cast, and nest with apply_mapping
weather_tmp = weather_tmp.apply_mapping([('ts_id', 'string', 'ts_id', 'string'),
                                         ('time', 'string', 'time', 'string'),
                                         ('temp_farenheit', 'float',
                                          'temp_farenheit', 'float'),
                                         ]
                                        )
# repartition the data to 1 partition - this ensures we get a single file
weather_tmp = weather_tmp.repartition(1)

output_s3_path = f"s3://{processed_bucket_name}/dt={most_recent_load_dt}"

glue_context.write_dynamic_frame.from_options(
    frame=weather_tmp,
    connection_type='s3',
    connection_options={
        'path': output_s3_path,
    },
    format='csv',
    format_options={
        'separator': ","
    }
)
job.commit()