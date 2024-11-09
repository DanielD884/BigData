from pathlib import Path
from typing import List

def get_year_months(directory: str = "/home/airflow/bike_data") -> List[str]:
    path = Path(directory)
    
    if not path.exists():
        raise FileNotFoundError(f"Directory {directory} does not exist.")
    
    year_months = sorted(file.name[:6] for file in path.iterdir() if file.is_file() and file.name[:6].isdigit())
    
    return year_months