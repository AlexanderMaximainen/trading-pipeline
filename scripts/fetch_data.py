from pathlib import Path
import pandas as pd
from datetime import datetime

RAW_DATA_PATH = Path("data/raw")

def read_csv_files():
    """Read all CSV files from data/raw and return one combined dataframe"""
    if not RAW_DATA_PATH.exists():
        print("Directory data/raw does not exist")
        return None
    
    csv_files = sorted(RAW_DATA_PATH.glob("*.csv"))

    if not csv_files:
        print("No CSV files found in data/raw")
        return None
    
    dataframes = []

    for file_path in csv_files:
        try:
            df = pd.read_csv(file_path, sep=";", encoding="utf-8-sig")

            df["source_file"] = file_path.name
            df["loaded_at"] = datetime.now()

            print(f"Read {file_path.name} - {len(df)} rows")
            dataframes.append(df)

        except Exception as e:
            print(f"Could not read {file_path.name}: {e}")
    
    if not dataframes:
        print("No valid CSV files could be read")
        return None

    df_all = pd.concat(dataframes, ignore_index=True)
    return df_all

if __name__ == "__main__":
    df = read_csv_files()

    if df is not None:
        print(df.head())
        print(f"\nTotal rows: {len(df)}")