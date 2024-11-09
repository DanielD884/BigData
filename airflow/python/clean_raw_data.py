import argparse
import ast
from datetime import datetime
from math import atan2, cos, radians, sin, sqrt
from os import path
import pyspark
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, udf, unix_timestamp, hour, when
from pyspark.sql.types import FloatType, IntegerType

# Define the function to get the arguments from the command line
def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--yearmonth", required=True)
    return parser.parse_args()

# Define the function to calculate the haversine distance (shortest distance over the earth’s surface)
def haversine(lat1, lon1, lat2, lon2):
    R = 6371  
    phi1 = radians(lat1)
    phi2 = radians(lat2)
    delta_phi = radians(lat2 - lat1)
    delta_lambda = radians(lon2 - lon1)
    a = sin(delta_phi / 2) ** 2 + cos(phi1) * cos(phi2) * sin(delta_lambda / 2) ** 2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))
    return R * c

# Calculate the age from the birth year
def get_age(birth_year):
    try:
        age = datetime.now().year - int(birth_year)
    except (TypeError, ValueError):
        age = 0
    return age

# Get the timeslot based on the hour
def get_timeslot_helper(hour: int):
    if 0 <= hour < 6:
        return 1
    elif 6 <= hour < 12:
        return 2
    elif 12 <= hour < 18:
        return 3
    elif 18 <= hour <= 24:
        return 4
    return -1

# Get the generation based on the birth year
def get_generation(birth_year):
    try:
        year = int(birth_year)
        if year < 1956:
            return "Silent Generation"
        elif year < 1966:
            return "Baby Boomer"
        elif year < 1981:
            return "Generation X"
        elif year < 1996:
            return "Millennial"
        elif year < 2010:
            return "Generation Z"
        else:
            return "Generation Alpha"
    except (ValueError, TypeError):
        return "Unknown"

if __name__ == "__main__":
    args = get_args()
    year_months = ast.literal_eval(args.yearmonth)

    sc = pyspark.SparkContext()
    spark = SparkSession(sc)

    haversine_udf = udf(haversine, FloatType())
    get_age_udf = udf(get_age, IntegerType())
    get_generation_udf = udf(get_generation, IntegerType())

    for year_month in year_months:
        print("############ {} ############".format(year_month))

        raw_file = path.join("/user/hadoop/hubway_data/raw", year_month, "{}-hubway-tripdata.csv".format(year_month))
        final_file = path.join("/user/hadoop/hubway_data/final", year_month, "hubway-tripdata.parquet")

        df = (
            spark.read.format("csv")
            .options(header="true", delimiter=",", nullValue="null", inferschema="true")
            .load(raw_file)
        )
        df = (
            df.withColumnRenamed("tripduration", "trip_duration")
            .withColumnRenamed("starttime", "start_time")
            .withColumnRenamed("stoptime", "end_time")
            .withColumnRenamed("start station id", "start_station_id")
            .withColumnRenamed("start station name", "start_station_name")
            .withColumnRenamed("start station latitude", "start_station_latitude")
            .withColumnRenamed("start station longitude", "start_station_longitude")
            .withColumnRenamed("end station id", "end_station_id")
            .withColumnRenamed("end station name", "end_station_name")
            .withColumnRenamed("end station latitude", "end_station_latitude")
            .withColumnRenamed("end station longitude", "end_station_longitude")
            .withColumnRenamed("bikeid", "bike_id")
            .withColumnRenamed("usertype", "user_type")
            .withColumnRenamed("birth year", "birth_year")
            .dropna(how="any")
            .where((col("trip_duration") > 0) & (col("trip_duration") < 86400))
        )

        df = df.withColumn(
            "trip_duration_minutes",
            col("trip_duration") / 60
        )

        df = df.withColumn(
            "trip_distance_km",
            haversine_udf(
                col("start_station_latitude"),
                col("start_station_longitude"),
                col("end_station_latitude"),
                col("end_station_longitude")
            )
        )

        df = df.withColumn(
            "timeslot",
            when((hour("start_time") >= 0) & (hour("start_time") < 6), 1)
            .when((hour("start_time") >= 6) & (hour("start_time") < 12), 2)
            .when((hour("start_time") >= 12) & (hour("start_time") < 18), 3)
            .otherwise(4)
        )

        df = df.withColumn("generation", get_generation_udf(col("birth_year")))
        df = df.withColumn("age", get_age_udf(col("birth_year")))

        df = df.drop(
            "trip_duration",
            "start_time",
            "end_time",
            "start_station_latitude",
            "start_station_longitude",
            "end_station_latitude",
            "end_station_longitude",
            "user_type",
            "birth_year"
        )

        df.write.format("parquet").mode("overwrite").options(
            header="true", delimiter=",", nullValue="null", inferschema="true"
        ).save(final_file)