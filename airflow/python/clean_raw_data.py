import argparse
import ast
from datetime import datetime
from math import atan2, cos, radians, sin, sqrt
from os import path
import pyspark
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import FloatType, IntegerType


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--yearmonth", required=True, type=str)
    return parser.parse_args()

def determine_timeslot(hour: int) -> int:
    if 0 <= hour < 6:
        return 0
    elif 6 <= hour < 12:
        return 1
    elif 12 <= hour < 18:
        return 2
    elif 18 <= hour <= 24:
        return 3
    return -1

def is_within_timeslot(start_time: datetime, end_time: datetime, time_slot: int) -> int:
    start_slot = determine_timeslot(start_time.hour)
    end_slot = determine_timeslot(end_time.hour)

    if start_slot < 0 or end_slot < 0:
        return -1

    if start_slot == time_slot or end_slot == time_slot:
        return 1
    if start_slot < time_slot < end_slot:
        return 1
    if start_slot > end_slot and (start_slot > time_slot or end_slot < time_slot):
        return 1

    return 0

def calculate_age(birth_year):
    try:
        age = datetime.now().year - int(birth_year)
    except (TypeError, ValueError) as e:
        print(f"Error: {e}")
        age = 0

    return age


def determine_generation(birth_year):
    try:
        year = int(birth_year)

        if year < 1945:
            return 0
        elif year < 1964:
            return 1
        elif year < 1980:
            return 2
        elif year < 1996:
            return 3
        elif year < 2012:
            return 4
        else:
            return 5
    except (ValueError, TypeError):
        return -1


def haversine(s_lat, s_lon, e_lat, e_lon):
    R = 6373.0  

    try:
        lat_1, lon_1 = float(s_lat), float(s_lon)
        lat_2, lon_2 = float(e_lat), float(e_lon)

        delta_lat = radians(lat_2 - lat_1)
        delta_lon = radians(lon_2 - lon_1)

        radian_lat_1 = radians(lat_1)
        radian_lat_2 = radians(lat_2)

        a = sin(delta_lat / 2) ** 2 + cos(radian_lat_1) * cos(radian_lat_2) * sin(delta_lon / 2) ** 2
        c = 2 * atan2(sqrt(a), sqrt(1 - a))
        distance = round(R * c, 2)
    except (TypeError, ValueError) as e:
        print(f"Error: {e}")
        distance = 0.0

    return distance


if __name__ == "__main__":
    """
    Main Function
    """

    # Parse Command Line Args
    args = get_args()
    year_months = ast.literal_eval(args.yearmonth)

    print(year_months)

    # Initialize Spark Context
    sc = pyspark.SparkContext()
    spark = SparkSession(sc)
 
    # Register UDFs for Spark
    haversine_udf = udf(haversine, FloatType())
    age_udf = udf(calculate_age, IntegerType())
    generation_udf = udf(determine_generation, IntegerType())
    timeslot_0_udf = udf(lambda start_time, end_time: is_within_timeslot(start_time, end_time, 0), IntegerType())
    timeslot_1_udf = udf(lambda start_time, end_time: is_within_timeslot(start_time, end_time, 1), IntegerType())
    timeslot_2_udf = udf(lambda start_time, end_time: is_within_timeslot(start_time, end_time, 2), IntegerType())
    timeslot_3_udf = udf(lambda start_time, end_time: is_within_timeslot(start_time, end_time, 3), IntegerType())
    
    # Process Data
    for year_month in year_months:

        raw_file = path.join("/user/hadoop/hubway_data/raw", year_month, "{}-hubway-tripdata.csv".format(year_month))
        final_file = path.join("/user/hadoop/hubway_data/final", year_month, "hubway-tripdata.parquet")

        df = (
            spark.read.format("csv")
            .options(header="true", delimiter=",", nullValue="null", inferschema="true")
            .load(raw_file)
        )
        df = (
            (
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
            )
            .dropna(how="any")
            .where((col("trip_duration") > 0) & (col("trip_duration") < 86400))
        )

        # 
        df_station_distance: DataFrame = df.groupBy(
            "start_station_latitude",
            "start_station_longitude",
            "end_station_latitude",
            "end_station_longitude",
        ).count()
        df_station_distance = df_station_distance.withColumn(  
            "station_distance",
            haversine_udf(
                col("start_station_latitude"),
                col("start_station_longitude"),
                col("end_station_latitude"),
                col("end_station_longitude"),
            ),
        )
        df = df.join(
            df_station_distance.select(
                "start_station_latitude",
                "start_station_longitude",
                "end_station_latitude",
                "end_station_longitude",
                "station_distance",
            ),
            on=[
                "start_station_latitude",
                "start_station_longitude",
                "end_station_latitude",
                "end_station_longitude",
            ],
        )

        df = df.withColumn("generation", generation_udf(col("birth_year")))
        df = df.withColumn("age", age_udf(col("birth_year")))
        df = df.withColumn("timeslot_0", timeslot_0_udf(col("start_time"), col("end_time")))
        df = df.withColumn("timeslot_1", timeslot_1_udf(col("start_time"), col("end_time")))
        df = df.withColumn("timeslot_2", timeslot_2_udf(col("start_time"), col("end_time")))
        df = df.withColumn("timeslot_3", timeslot_3_udf(col("start_time"), col("end_time")))

        df = df.drop(
            "start_time",
            "end_time",
            "start_station_latitude",
            "start_station_longitude",
            "end_station_latitude",
            "end_station_longitude"
        )
        print(df.collect()[:5])

        # Write data to HDFS
        df.write.format("parquet").mode("overwrite").options(
            header="true", delimiter=",", nullValue="null", inferschema="true"
        ).save(final_file)