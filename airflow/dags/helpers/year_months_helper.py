import os


# Function to get the year month list
def get_year_months():
    files = [file for file in os.listdir("/home/airflow/hubway_data") if file[:6].isdigit()]
    return [file[:6] for file in files]