import argparse
import ast
from functools import reduce
from os import path
import pyspark
from pyspark.sql import DataFrame, SparkSession


def get_args():
    parser = argparse.ArgumentParser(
        description="Combine KPI data from multiple year-month partitions stored in HDFS."
    )
    parser.add_argument("--yearmonth", help="Partition Year Month", required=True, type=str)
    return parser.parse_args()


def main():
    # Parse Command Line Args
    args = get_args()
    year_months = ast.literal_eval(args.yearmonth)

    print(year_months)

    # Initialize Spark Context
    sc = pyspark.SparkContext()
    spark = SparkSession(sc)

    data = []

    # Loop over all year months
    for year_month in year_months:
        print("############ {} ############".format(year_month))

        kpi_file = path.join("/user/hadoop/hubway_data/kpis", year_month, "kpis.parquet")

        try:
            # Load the current kpi file and append it to the data list
            df = spark.read.format("parquet").load(kpi_file)
            data.append(df)
        except Exception as e:
            print(f"Error loading {kpi_file}: {e}")

    if data:
        # Combine all dataframes into one
        kpi_data = reduce(DataFrame.union, data)

        # Write data to HDFS
        try:
            kpi_data.toPandas().to_excel("/home/airflow/output/combined-kpis.xlsx", index=False)
            print("Combined KPI data written to /home/airflow/output/combined-kpis.xlsx")
        except Exception as e:
            print(f"Error writing to Excel: {e}")
    else:
        print("No data to combine.")

    # Stop Spark Context
    spark.stop()


if __name__ == "__main__":
    main()