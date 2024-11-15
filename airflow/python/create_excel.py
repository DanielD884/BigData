import argparse
import ast
from functools import reduce
from os import path
import pandas as pd
from openpyxl import load_workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.chart import PieChart, Reference, BarChart
from openpyxl.chart.label import DataLabelList
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
        cell.value = sheet["A2"].value  
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

        # Add the "Average Trip Duration" header
        sheet.merge_cells('E15:H18')
        cell = sheet.cell(row=15, column=5, value="Average Trip Duration")
        cell.font = Font(bold=True)
        cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)

        # Apply thick border around the "Average Trip Duration" box
        for row in range(15, 19):
            # Left border
            sheet.cell(row=row, column=5).border = Border(left=thick_border.left)
            # Right border
            sheet.cell(row=row, column=8).border = Border(right=thick_border.right)

        for col in range(5, 9):
            # Top border
            sheet.cell(row=15, column=col).border = Border(top=thick_border.top)
            # Bottom border
            sheet.cell(row=18, column=col).border = Border(bottom=thick_border.bottom)

        # Set the corners for the "Average Trip Duration" box
        sheet.cell(row=15, column=5).border = Border(top=thick_border.top, left=thick_border.left)
        sheet.cell(row=15, column=8).border = Border(top=thick_border.top, right=thick_border.right)
        sheet.cell(row=18, column=5).border = Border(bottom=thick_border.bottom, left=thick_border.left)
        sheet.cell(row=18, column=8).border = Border(bottom=thick_border.bottom, right=thick_border.right)

        # Add sample data for average trip duration
        average_trip_duration = sheet["B2"].value  
        sheet.merge_cells('E19:H24')
        cell = sheet.cell(row=19, column=5, value=average_trip_duration)
        cell.alignment = Alignment(horizontal="center", vertical="center")

        # Add the "Average Distance" header
        sheet.merge_cells('E26:H29')
        cell = sheet.cell(row=26, column=5, value="Average Distance")
        cell.font = Font(bold=True)
        cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)

        # Apply thick border around the "Average Distance" box
        for row in range(26, 30):
            # Left border
            sheet.cell(row=row, column=5).border = Border(left=thick_border.left)
            # Right border
            sheet.cell(row=row, column=8).border = Border(right=thick_border.right)

        for col in range(5, 9):
            # Top border
            sheet.cell(row=26, column=col).border = Border(top=thick_border.top)
            # Bottom border
            sheet.cell(row=29, column=col).border = Border(bottom=thick_border.bottom)

        # Set the corners for the "Average Distance" box
        sheet.cell(row=26, column=5).border = Border(top=thick_border.top, left=thick_border.left)
        sheet.cell(row=26, column=8).border = Border(top=thick_border.top, right=thick_border.right)
        sheet.cell(row=29, column=5).border = Border(bottom=thick_border.bottom, left=thick_border.left)
        sheet.cell(row=29, column=8).border = Border(bottom=thick_border.bottom, right=thick_border.right)

        # Add data for average distance
        average_distance = sheet["C2"].value
        sheet.merge_cells('E30:H35')
        cell = sheet.cell(row=30, column=5, value=average_distance)
        cell.alignment = Alignment(horizontal="center", vertical="center")


        # Add the first table from K15 to N37 with columns "Bike ID" and "Anzahl Nutzung"
        sheet.merge_cells('K11:N12')
        cell = sheet.cell(row=11, column=11, value="Bike ID and Anzahl Nutzung")
        cell.font = Font(bold=True)
        cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)

        sheet.merge_cells('K15:L16')
        sheet.merge_cells('M15:N16')
        cell = sheet.cell(row=15, column=11, value="Bike ID")
        cell.font = Font(bold=True)
        cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
        cell = sheet.cell(row=15, column=13, value="Anzahl Nutzung")
        cell.font = Font(bold=True)
        cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)

        # Fill the first table (Bike ID and Anzahl Nutzung) with "Platz" from N2 to Z2
        for i in range(10):
            # Platz-Spalte
            platz_cell = sheet.cell(row=17 + 2 * i, column=10, value=f"Platz {i + 1}")
            platz_cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
            sheet.merge_cells(start_row=17 + 2 * i, start_column=10, end_row=18 + 2 * i, end_column=10)

            # Bike ID and Anzahl Nutzung
            bike_id = sheet.cell(row=2, column=14 + 2 * i).value
            usage_count = sheet.cell(row=2, column=15 + 2 * i).value
            row_start = 17 + 2 * i
            row_end = row_start + 1
            sheet.merge_cells(start_row=row_start, start_column=11, end_row=row_end, end_column=12)
            sheet.merge_cells(start_row=row_start, start_column=13, end_row=row_end, end_column=14)
            sheet.cell(row=row_start, column=11, value=bike_id).alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
            sheet.cell(row=row_start, column=13, value=usage_count).alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)

        # Apply thick border around the first table
        for row in range(17, 37):
            for col in range(10, 15):
                cell = sheet.cell(row=row, column=col)
                cell.border = thick_border

        # Add the second table from Q15 to T37 with columns "Top 10 most start stations" and "Anzahl Nutzung"
        sheet.merge_cells('Q11:T12')
        cell = sheet.cell(row=11, column=17, value="Top 10 most start stations and Anzahl Nutzung")
        cell.font = Font(bold=True)
        cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)

        sheet.merge_cells('Q15:R16')
        sheet.merge_cells('S15:T16')
        cell = sheet.cell(row=15, column=17, value="Top 10 most start stations")
        cell.font = Font(bold=True)
        cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
        cell = sheet.cell(row=15, column=19, value="Anzahl Nutzung")
        cell.font = Font(bold=True)
        cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)

        # Fill the second table with "Platz" from AH2 to BA2
        for i in range(10):
            # Platz-Spalte
            platz_cell = sheet.cell(row=17 + 2 * i, column=16, value=f"Platz {i + 1}")
            platz_cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
            sheet.merge_cells(start_row=17 + 2 * i, start_column=16, end_row=18 + 2 * i, end_column=16)

            # Start Station and Anzahl Nutzung
            start_station = sheet.cell(row=2, column=34 + 2 * i).value
            usage_count = sheet.cell(row=2, column=35 + 2 * i).value
            row_start = 17 + 2 * i
            row_end = row_start + 1
            sheet.merge_cells(start_row=row_start, start_column=17, end_row=row_end, end_column=18)
            sheet.merge_cells(start_row=row_start, start_column=19, end_row=row_end, end_column=20)
            sheet.cell(row=row_start, column=17, value=start_station).alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
            sheet.cell(row=row_start, column=19, value=usage_count).alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)

        # Apply thick border around the second table
        for row in range(17, 37):
            for col in range(16, 21):
                cell = sheet.cell(row=row, column=col)
                cell.border = thick_border

        # Add the third table from W15 to Z37 with columns "Top 10 most end stations" and "Anzahl Nutzung"
        sheet.merge_cells('W11:Z12')
        cell = sheet.cell(row=11, column=23, value="Top 10 most end stations and Anzahl Nutzung")
        cell.font = Font(bold=True)
        cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)

        sheet.merge_cells('W15:X16')
        sheet.merge_cells('Y15:Z16')
        cell = sheet.cell(row=15, column=23, value="Top 10 most end stations")
        cell.font = Font(bold=True)
        cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
        cell = sheet.cell(row=15, column=25, value="Anzahl Nutzung")
        cell.font = Font(bold=True)
        cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)

        # Fill the third table from BB2 to BU2
        for i in range(10):
            # Platz-Spalte
            platz_cell = sheet.cell(row=17 + 2 * i, column=22, value=f"Platz {i + 1}")
            platz_cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
            sheet.merge_cells(start_row=17 + 2 * i, start_column=22, end_row=18 + 2 * i, end_column=22)

            # End Station and Anzahl Nutzung
            station_name = sheet.cell(row=2, column=54 + 2 * i).value
            usage_count = sheet.cell(row=2, column=55 + 2 * i).value
            row_start = 17 + 2 * i
            row_end = row_start + 1
            sheet.merge_cells(start_row=row_start, start_column=23, end_row=row_end, end_column=24)
            sheet.merge_cells(start_row=row_start, start_column=25, end_row=row_end, end_column=26)
            sheet.cell(row=row_start, column=23, value=station_name).alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
            sheet.cell(row=row_start, column=25, value=usage_count).alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)

        # Apply thick border around the third table
        for row in range(17, 37):
            for col in range(22, 27):
                cell = sheet.cell(row=row, column=col)
                cell.border = thick_border



        # Erstes Säulendiagramm
        bar_chart1 = BarChart()

        # Referenzen für Labels und Werte
        labels_ref = Reference(sheet, min_col=4, min_row=1, max_col=6, max_row=1)  # D1:F1 für Labels
        values_ref = Reference(sheet, min_col=4, min_row=2, max_col=6, max_row=2)  # D2:F2 für Werte

        # Daten und Kategorien hinzufügen
        bar_chart1.add_data(values_ref, titles_from_data=False)
        bar_chart1.set_categories(labels_ref)
        bar_chart1.title = "Usage Share by Gender"

        # Diagrammgröße einstellen
        bar_chart1.width = 10
        bar_chart1.height = 12

        # Diagramm in das Tabellenblatt einfügen
        sheet.add_chart(bar_chart1, "E38")

        # Zweites Säulendiagramm
        bar_chart2 = BarChart()

        # Referenzen für Labels und Werte
        labels_ref2 = Reference(sheet, min_col=7, min_row=1, max_col=13, max_row=1)  # G1:M1 für Labels
        values_ref2 = Reference(sheet, min_col=7, min_row=2, max_col=13, max_row=2)  # G2:M2 für Werte

        # Daten und Kategorien hinzufügen
        bar_chart2.add_data(values_ref2, titles_from_data=False)
        bar_chart2.set_categories(labels_ref2)
        bar_chart2.title = "Usage Share by Age"

        # Diagrammgröße einstellen
        bar_chart2.width = 10
        bar_chart2.height = 12

        # Diagramm in das Tabellenblatt einfügen
        sheet.add_chart(bar_chart2, "N38")

        # Drittes Säulendiagramm
        bar_chart3 = BarChart()

        # Referenzen für Labels und Werte
        labels_ref_bv_by = Reference(sheet, min_col=74, min_row=1, max_col=77, max_row=1)  # BV1:BY1 für Labels
        values_ref_bv_by = Reference(sheet, min_col=74, min_row=2, max_col=77, max_row=2)  # BV2:BY2 für Werte

        # Daten und Kategorien hinzufügen
        bar_chart3.add_data(values_ref_bv_by, titles_from_data=False)
        bar_chart3.set_categories(labels_ref_bv_by)
        bar_chart3.title = "Sample Bar Chart for BV to BY"

        # Diagrammgröße einstellen
        bar_chart3.width = 10
        bar_chart3.height = 12

        # Diagramm in das Tabellenblatt einfügen
        sheet.add_chart(bar_chart3, "H50")


    # Speichern der formatieren Excel-Datei
    workbook.save("/home/airflow/output/combined-kpis.xlsx")