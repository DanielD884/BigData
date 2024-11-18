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


def calculate_gender_share(df, gender_type=None):
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
        
        if gender_type == "m":
            return m
        elif gender_type == "w":
            return w
        elif gender_type == "na":
            return na
        else:
            return 0.0
   

def calculate_top_n(df, column_name, rank, return_type="value"):
    top_n = df.groupBy(column_name).count().orderBy(desc("count")).limit(rank).collect()
    
    if len(top_n) >= rank:
        value, count = top_n[rank - 1][column_name], top_n[rank - 1]["count"]
        return value if return_type == "value" else count
    else:
        return None if return_type == "value" else 0


def calculate_time_slot_percentage(df, slot):
    try:
        total_count = df.count() 
        slot_count = df.where(col(f"timeslot_{slot}") == 1).count()
        percentage = (slot_count / total_count) * 100 if total_count > 0 else 0
    except IndexError:
        percentage = 0
    return round(percentage, 2)

def calculate_generation_percentage(df, generation_value):
    total_count = df.count()  
    generation_count = df.filter(col("generation") == generation_value).count()
    percentage = (generation_count / total_count) * 100 if total_count > 0 else 0
    return round(percentage, 2)

def format_year_month(year_month):
    return f"{year_month[4:6]}.{year_month[:4]}"


def process_year_month(spark, year_month):
    row = Row(
      "year_month",
      "avg_trip_duration",
      "avg_trip_distance",
      "gender_share_m",
      "gender_share_w",
      "gender_share_na",
      "silent_generation",
      "generation_x",
      "generation_y",
      "generation_z",
      "baby_boomer",
      "generation_alpha",
      "no_generation_data",
      "top_used_bikes_1_value",
      "top_used_bikes_1_count",
      "top_used_bikes_2_value",
      "top_used_bikes_2_count",
      "top_used_bikes_3_value",
      "top_used_bikes_3_count",
      "top_used_bikes_4_value",
      "top_used_bikes_4_count",
      "top_used_bikes_5_value",
      "top_used_bikes_5_count",
      "top_used_bikes_6_value",
      "top_used_bikes_6_count",
      "top_used_bikes_7_value",
      "top_used_bikes_7_count",
      "top_used_bikes_8_value",
      "top_used_bikes_8_count",
      "top_used_bikes_9_value",
      "top_used_bikes_9_count",
      "top_used_bikes_10_value",
      "top_used_bikes_10_count",
      "top_start_stations_1_value",
      "top_start_stations_1_count",
      "top_start_stations_2_value",
      "top_start_stations_2_count",
      "top_start_stations_3_value",
      "top_start_stations_3_count",
      "top_start_stations_4_value",
      "top_start_stations_4_count",
      "top_start_stations_5_value",
      "top_start_stations_5_count",
      "top_start_stations_6_value",
      "top_start_stations_6_count",
      "top_start_stations_7_value",
      "top_start_stations_7_count",
      "top_start_stations_8_value",
      "top_start_stations_8_count",
      "top_start_stations_9_value",
      "top_start_stations_9_count",
      "top_start_stations_10_value",
      "top_start_stations_10_count",
      "top_end_stations_1_value",
      "top_end_stations_1_count",
      "top_end_stations_2_value",
      "top_end_stations_2_count",
      "top_end_stations_3_value",
      "top_end_stations_3_count",
      "top_end_stations_4_value",
      "top_end_stations_4_count",
      "top_end_stations_5_value",
      "top_end_stations_5_count",
      "top_end_stations_6_value",
      "top_end_stations_6_count",
      "top_end_stations_7_value",
      "top_end_stations_7_count",
      "top_end_stations_8_value",
      "top_end_stations_8_count",
      "top_end_stations_9_value",
      "top_end_stations_9_count",
      "top_end_stations_10_value",
      "top_end_stations_10_count",
      "time_slots_0",
      "time_slots_1",
      "time_slots_2",
      "time_slots_3"
    )

    cleaned_file = path.join("/user/hadoop/hubway_data/final", year_month, "hubway-tripdata.parquet")
    kpi_file = path.join("/user/hadoop/hubway_data/kpis", year_month, "kpis.parquet")

    df = (
        spark.read.format("parquet")
        .options(header="true", delimiter=",", nullValue="null", inferschema="true")
        .load(cleaned_file)
    )

    avg_trip_duration, avg_trip_distance = calculate_average_kpis(df)

    # Calculate gender share
    gender_share_m = calculate_gender_share(df,"m")
    gender_share_w = calculate_gender_share(df,"w")
    gender_share_na = calculate_gender_share(df,"na")

    # Calculate top 10 used bikes
    top_used_bikes_1_value = calculate_top_n(df, "bike_id", 1, "value")
    top_used_bikes_1_count = calculate_top_n(df, "bike_id", 1, "count")
    top_used_bikes_2_value = calculate_top_n(df, "bike_id", 2, "value")
    top_used_bikes_2_count = calculate_top_n(df, "bike_id", 2, "count")
    top_used_bikes_3_value = calculate_top_n(df, "bike_id", 3, "value")
    top_used_bikes_3_count = calculate_top_n(df, "bike_id", 3, "count")
    top_used_bikes_4_value = calculate_top_n(df, "bike_id", 4, "value")
    top_used_bikes_4_count = calculate_top_n(df, "bike_id", 4, "count")
    top_used_bikes_5_value = calculate_top_n(df, "bike_id", 5, "value")
    top_used_bikes_5_count = calculate_top_n(df, "bike_id", 5, "count")
    top_used_bikes_6_value = calculate_top_n(df, "bike_id", 6, "value")
    top_used_bikes_6_count = calculate_top_n(df, "bike_id", 6, "count")
    top_used_bikes_7_value = calculate_top_n(df, "bike_id", 7, "value")
    top_used_bikes_7_count = calculate_top_n(df, "bike_id", 7, "count")
    top_used_bikes_8_value = calculate_top_n(df, "bike_id", 8, "value")
    top_used_bikes_8_count = calculate_top_n(df, "bike_id", 8, "count")
    top_used_bikes_9_value = calculate_top_n(df, "bike_id", 9, "value")
    top_used_bikes_9_count = calculate_top_n(df, "bike_id", 9, "count")
    top_used_bikes_10_value = calculate_top_n(df, "bike_id", 10, "value")
    top_used_bikes_10_count = calculate_top_n(df, "bike_id", 10, "count")

    # Calculate Top 10 Bike Stations
    top_start_stations_1_value = calculate_top_n(df, "start_station_name", 1, "value")
    top_start_stations_1_count = calculate_top_n(df, "start_station_name", 1, "count")
    top_start_stations_2_value = calculate_top_n(df, "start_station_name", 2, "value")
    top_start_stations_2_count = calculate_top_n(df, "start_station_name", 2, "count")
    top_start_stations_3_value = calculate_top_n(df, "start_station_name", 3, "value")
    top_start_stations_3_count = calculate_top_n(df, "start_station_name", 3, "count")
    top_start_stations_4_value = calculate_top_n(df, "start_station_name", 4, "value")
    top_start_stations_4_count = calculate_top_n(df, "start_station_name", 4, "count")
    top_start_stations_5_value = calculate_top_n(df, "start_station_name", 5, "value")
    top_start_stations_5_count = calculate_top_n(df, "start_station_name", 5, "count")
    top_start_stations_6_value = calculate_top_n(df, "start_station_name", 6, "value")
    top_start_stations_6_count = calculate_top_n(df, "start_station_name", 6, "count")
    top_start_stations_7_value = calculate_top_n(df, "start_station_name", 7, "value")
    top_start_stations_7_count = calculate_top_n(df, "start_station_name", 7, "count")
    top_start_stations_8_value = calculate_top_n(df, "start_station_name", 8, "value")
    top_start_stations_8_count = calculate_top_n(df, "start_station_name", 8, "count")
    top_start_stations_9_value = calculate_top_n(df, "start_station_name", 9, "value")
    top_start_stations_9_count = calculate_top_n(df, "start_station_name", 9, "count")
    top_start_stations_10_value = calculate_top_n(df, "start_station_name", 10, "value")
    top_start_stations_10_count = calculate_top_n(df, "start_station_name", 10, "count")

    # Calculate Top 10 End Bike Stations
    top_end_stations_1_value = calculate_top_n(df, "end_station_name", 1, "value")
    top_end_stations_1_count = calculate_top_n(df, "end_station_name", 1, "count")
    top_end_stations_2_value = calculate_top_n(df, "end_station_name", 2, "value")
    top_end_stations_2_count = calculate_top_n(df, "end_station_name", 2, "count")
    top_end_stations_3_value = calculate_top_n(df, "end_station_name", 3, "value")
    top_end_stations_3_count = calculate_top_n(df, "end_station_name", 3, "count")
    top_end_stations_4_value = calculate_top_n(df, "end_station_name", 4, "value")
    top_end_stations_4_count = calculate_top_n(df, "end_station_name", 4, "count")
    top_end_stations_5_value = calculate_top_n(df, "end_station_name", 5, "value")
    top_end_stations_5_count = calculate_top_n(df, "end_station_name", 5, "count")
    top_end_stations_6_value = calculate_top_n(df, "end_station_name", 6, "value")
    top_end_stations_6_count = calculate_top_n(df, "end_station_name", 6, "count")
    top_end_stations_7_value = calculate_top_n(df, "end_station_name", 7, "value")
    top_end_stations_7_count = calculate_top_n(df, "end_station_name", 7, "count")
    top_end_stations_8_value = calculate_top_n(df, "end_station_name", 8, "value")
    top_end_stations_8_count = calculate_top_n(df, "end_station_name", 8, "count")
    top_end_stations_9_value = calculate_top_n(df, "end_station_name", 9, "value")
    top_end_stations_9_count = calculate_top_n(df, "end_station_name", 9, "count")
    top_end_stations_10_value = calculate_top_n(df, "end_station_name", 10, "value")
    top_end_stations_10_count = calculate_top_n(df, "end_station_name", 10, "count")
    
    # Calculate Time Slots
    time_slots_0 = calculate_time_slot_percentage(df, 0)
    time_slots_1 = calculate_time_slot_percentage(df, 1)
    time_slots_2 = calculate_time_slot_percentage(df, 2)
    time_slots_3 = calculate_time_slot_percentage(df, 3)

    # Calculate Generation
    silent_generation = calculate_generation_percentage(df, 0)
    baby_boomer = calculate_generation_percentage(df, 1)
    generation_x = calculate_generation_percentage(df, 2)
    generation_y = calculate_generation_percentage(df, 3)
    generation_z = calculate_generation_percentage(df, 4)
    generation_alpha = calculate_generation_percentage(df, 5)
    no_generation_data = calculate_generation_percentage(df, -1)

    # Change Layout year_month
    year_month = format_year_month(year_month)


    kpis_df = spark.createDataFrame(
        [
            row(
              year_month,
              avg_trip_duration,
              avg_trip_distance,
              gender_share_m,
              gender_share_w,
              gender_share_na,
              silent_generation,
              generation_x,
              generation_y,
              generation_z,
              baby_boomer,
              generation_alpha,
              no_generation_data,
              top_used_bikes_1_value,
              top_used_bikes_1_count,
              top_used_bikes_2_value,
              top_used_bikes_2_count,
              top_used_bikes_3_value,
              top_used_bikes_3_count,
              top_used_bikes_4_value,
              top_used_bikes_4_count,
              top_used_bikes_5_value,
              top_used_bikes_5_count,
              top_used_bikes_6_value,
              top_used_bikes_6_count,
              top_used_bikes_7_value,
              top_used_bikes_7_count,
              top_used_bikes_8_value,
              top_used_bikes_8_count,
              top_used_bikes_9_value,
              top_used_bikes_9_count,
              top_used_bikes_10_value,
              top_used_bikes_10_count,
              top_start_stations_1_value,
              top_start_stations_1_count,
              top_start_stations_2_value,
              top_start_stations_2_count,
              top_start_stations_3_value,
              top_start_stations_3_count,
              top_start_stations_4_value,
              top_start_stations_4_count,
              top_start_stations_5_value,
              top_start_stations_5_count,
              top_start_stations_6_value,
              top_start_stations_6_count,
              top_start_stations_7_value,
              top_start_stations_7_count,
              top_start_stations_8_value,
              top_start_stations_8_count,
              top_start_stations_9_value,
              top_start_stations_9_count,
              top_start_stations_10_value,
              top_start_stations_10_count,
              top_end_stations_1_value,
              top_end_stations_1_count,
              top_end_stations_2_value,
              top_end_stations_2_count,
              top_end_stations_3_value,
              top_end_stations_3_count,
              top_end_stations_4_value,
              top_end_stations_4_count,
              top_end_stations_5_value,
              top_end_stations_5_count,
              top_end_stations_6_value,
              top_end_stations_6_count,
              top_end_stations_7_value,
              top_end_stations_7_count,
              top_end_stations_8_value,
              top_end_stations_8_count,
              top_end_stations_9_value,
              top_end_stations_9_count,
              top_end_stations_10_value,
              top_end_stations_10_count,
              time_slots_0,
              time_slots_1,
              time_slots_2,
              time_slots_3

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