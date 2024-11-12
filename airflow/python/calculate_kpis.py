import argparse
import ast
from os import path

import pyspark
from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql.functions import col, desc, avg


def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument("--yearmonth", required=True, type=str)
    return parser.parse_args()

def calculate_average_kpis(df):
    avg_kpis = df.agg(
        avg("trip_duration").alias("avg_trip_duration"),
        avg("station_distance").alias("avg_station_distance")
    ).collect()[0]

    avg_trip_duration = avg_kpis["avg_trip_duration"]
    avg_station_distance = avg_kpis["avg_station_distance"]

    return (
        int(avg_trip_duration / 60) if avg_trip_duration is not None else 0,
        float(avg_station_distance) if avg_station_distance is not None else 0.0
    )


def calculate_gender_share(df):
    gender_df: DataFrame = df.groupBy(col("gender")).count().collect()
    
    gender_counts = {row['gender']: row['count'] for row in gender_df}
    
    gender_count_na = gender_counts.get(0, 0)
    gender_count_m = gender_counts.get(1, 0)
    gender_count_f = gender_counts.get(2, 0)
    
    gender_count = gender_count_na + gender_count_m + gender_count_f
    
    if gender_count > 0:
        m = float(gender_count_m / gender_count * 100)
        w = float(gender_count_f / gender_count * 100)
        na = float(gender_count_na / gender_count * 100)
        gender_share = str([("m", m), ("w", w), ("na", na)])
    else:
        gender_share = "[]"
    
    return gender_share


def calculate_top_10(df, column_name):
    return str(
        [(value, count) for value, count in df.groupBy(column_name).count().orderBy(desc("count")).limit(10).collect()]
    )


def calculate_time_slots_share(df):
    time_slots = [0, 1, 2, 3]
    time_slot_counts = {}

    for slot in time_slots:
        try:
            count = df.where(col(f"timeslot_{slot}") == 1).count()
        except IndexError:
            count = 0
        time_slot_counts[slot] = count

    total_count = sum(time_slot_counts.values())

    if total_count > 0:
        time_slots_share = [
            (slot, count / total_count * 100) for slot, count in time_slot_counts.items()
        ]
    else:
        time_slots_share = []

    return str(time_slots_share)


def process_year_month(spark, year_month):
    row = Row(
        "year_month",
        "avg_trip_duration",
        "avg_trip_distance",
        "gender_share",
        "generation_share",
        "top_used_bikes",
        "top_start_stations",
        "top_end_stations",
        "time_slots",
    )

    final_file = path.join("/user/hadoop/hubway_data/final", year_month, "hubway-tripdata.parquet")
    kpi_file = path.join("/user/hadoop/hubway_data/kpis", year_month, "kpis.parquet")

    df = (
        spark.read.format("parquet")
        .options(header="true", delimiter=",", nullValue="null", inferschema="true")
        .load(final_file)
    )

    avg_trip_duration, avg_trip_distance = calculate_average_kpis(df)
    gender_share = calculate_gender_share(df)
    generation_share = calculate_top_10(df, "generation")
    top_used_bikes = calculate_top_10(df, "bike_id")
    top_start_stations = calculate_top_10(df, "start_station_id")
    top_end_stations = calculate_top_10(df, "end_station_id")
    time_slots = calculate_time_slots_share(df)

    kpis_df = spark.createDataFrame(
        [
            row(
                year_month,
                avg_trip_duration,
                avg_trip_distance,
                gender_share,
                generation_share,
                top_used_bikes,
                top_start_stations,
                top_end_stations,
                time_slots,
            )
        ]
    )

    kpis_df.write.format("parquet").mode("overwrite").options(
        header="true", delimiter=",", nullValue="null", inferschema="true"
    ).save(kpi_file)


def main():
    args = parse_arguments()
    year_months = ast.literal_eval(args.yearmonth)
    sc = pyspark.SparkContext()
    spark = SparkSession(sc)

    for year_month in year_months:
        process_year_month(spark, year_month)


if __name__ == "__main__":
    main()