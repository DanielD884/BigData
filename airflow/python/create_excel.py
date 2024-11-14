import argparse
import ast
from functools import reduce
from os import path
import pandas as pd
from openpyxl import load_workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.chart import PieChart, Reference
import pyspark
from pyspark.sql import DataFrame, SparkSession


def get_args():
    parser = argparse.ArgumentParser(
        description="Some Basic Spark Job doing some stuff on hubway data stored within HDFS."
    )
    parser.add_argument("--yearmonth", help="Partion Year Month", required=True, type=str)

    return parser.parse_args()


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

    with pd.ExcelWriter("/home/airflow/output/combined-kpis.xlsx") as writer:
            # Loop over all year months
            for year_month in year_months:

                kpi_file = path.join("/user/hadoop/hubway_data/kpis", year_month, "kpis.parquet")

                # Load the current KPI file as a DataFrame
                df = (
                    spark.read.format("parquet")
                    .options(header="true", delimiter=",", nullValue="null", inferschema="true")
                    .load(kpi_file)
                )

                # Convert the Spark DataFrame to Pandas DataFrame and write to a specific sheet
                df.toPandas().to_excel(writer, sheet_name=year_month, index=False, header=True)
    
   # Lade die Excel-Datei mit openpyxl für das Layout und die Formatierung
    workbook = load_workbook("/home/airflow/output/combined-kpis.xlsx")
    
    for year_month in year_months:
        sheet = workbook[year_month]
        ws = workbook.active

        # Remove gridlines
        sheet.sheet_view.showGridLines = False

        # Define the thick border style
        thick_border = Border(left=Side(style='thick', color='000000'),
                            right=Side(style='thick', color='000000'),
                            top=Side(style='thick', color='000000'),
                            bottom=Side(style='thick', color='000000'))

        # Remove all existing borders in the range D5 to Y62
        for row in range(5, 63):
            for col in range(4, 26):  # D is the 4th column, Y is the 25th column
                cell = sheet.cell(row=row, column=col)
                cell.border = Border()

        # Apply the thick border around the range D5 to Y62
        for row in range(5, 63):
            # Left border
            sheet.cell(row=row, column=4).border = Border(left=thick_border.left)
            # Right border
            sheet.cell(row=row, column=25).border = Border(right=thick_border.right)

        for col in range(4, 26):
            # Top border
            sheet.cell(row=5, column=col).border = Border(top=thick_border.top)
            # Bottom border
            sheet.cell(row=62, column=col).border = Border(bottom=thick_border.bottom)

        # Set the corners
        sheet.cell(row=5, column=4).border = Border(top=thick_border.top, left=thick_border.left)
        sheet.cell(row=5, column=25).border = Border(top=thick_border.top, right=thick_border.right)
        sheet.cell(row=62, column=4).border = Border(bottom=thick_border.bottom, left=thick_border.left)
        sheet.cell(row=62, column=25).border = Border(bottom=thick_border.bottom, right=thick_border.right)

        # Set the text "hubway-data" in a merged cell
        sheet.merge_cells('D5:Y6')
        cell = sheet.cell(row=5, column=4)
        cell.value = "hubway-data"
        cell.font = Font(color="FFFFFF")  # White font
        cell.fill = PatternFill(start_color="808080", end_color="808080", fill_type="solid")  # Gray background
        cell.alignment = Alignment(horizontal="center", vertical="center")  # Center alignment

        # Merge cells E8 to H13 and set the text "February"
        sheet.merge_cells('E8:H13')
        cell = sheet.cell(row=8, column=5)
        cell.value = ws["A2"].value  
        cell.font = Font(color="000000")  # Black font
        cell.alignment = Alignment(horizontal="center", vertical="center")  # Center alignment

        # Apply thick border around the "February" box
        for row in range(8, 14):
            # Left border
            sheet.cell(row=row, column=5).border = Border(left=thick_border.left)
            # Right border
            sheet.cell(row=row, column=8).border = Border(right=thick_border.right)

        for col in range(5, 9):
            # Top border
            sheet.cell(row=8, column=col).border = Border(top=thick_border.top)
            # Bottom border
            sheet.cell(row=13, column=col).border = Border(bottom=thick_border.bottom)

        # Set the corners for the "February" box
        sheet.cell(row=8, column=5).border = Border(top=thick_border.top, left=thick_border.left)
        sheet.cell(row=8, column=8).border = Border(top=thick_border.top, right=thick_border.right)
        sheet.cell(row=13, column=5).border = Border(bottom=thick_border.bottom, left=thick_border.left)
        sheet.cell(row=13, column=8).border = Border(bottom=thick_border.bottom, right=thick_border.right)

                
        

        
    
    # Speichern der formatieren Excel-Datei
    workbook.save("/home/airflow/output/combined-kpis.xlsx")