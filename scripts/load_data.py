"""
Loads new CSV data into the bronze layer

- Checks which files are already loaded
- Only processes new files
- Validates columns before inserting
- Loads data into bronze.transactions

Returns number of rows loaded so the pipeline knows what to do next
"""

import pandas as pd
from pathlib import Path
from urllib.parse import quote_plus
from sqlalchemy import create_engine, text
from fetch_data import read_csv_files

PROJECT_ROOT = Path(__file__).resolve().parents[1]
RAW_DATA_PATH = PROJECT_ROOT / "data" / "raw"

SERVER = "MAXIMAINENPC"
DATABASE = "trading_pipeline"

def get_engine():
    """Create a database connection to SQL Server""" 

    odbc_connection_string = (
        "DRIVER={ODBC Driver 17 for SQL Server};"
        f"SERVER={SERVER};"
        f"DATABASE={DATABASE};"
        "Trusted_Connection=yes;"
    )

    connection_url = f"mssql+pyodbc:///?odbc_connect={quote_plus(odbc_connection_string)}"
    return create_engine(connection_url)

def get_already_loaded_files(engine):
    """Get list of files that are already in bronze to avoid loading them again"""

    query = text("SELECT DISTINCT source_file FROM bronze.transactions")

    try:
        with engine.connect() as connection:
            df_loaded = pd.read_sql(query, connection)
        return set(df_loaded["source_file"].dropna().tolist())
    except Exception as e:
        print(f"Could not read already loaded files from bronze.transactions: {e}")
        return set()


def prepare_dataframe(df: pd.DataFrame):
    """Make sure the dataframe has all expected columns before loading"""

    expected_columns = [
        "Datum",
        "Konto",
        "Typ av transaktion",
        "Värdepapper/beskrivning",
        "Antal",
        "Kurs",
        "Belopp",
        "Transaktionsvaluta",
        "Courtage",
        "Valutakurs",
        "Instrumentvaluta",
        "ISIN",
        "Resultat",
        "source_file",
        "loaded_at",
    ]

    missing_columns = [col for col in expected_columns if col not in df.columns]
    if missing_columns:
        raise ValueError(f"Data is missing the following columns: {missing_columns}")
    
    return df[expected_columns].copy()

def load_to_bronze(df: pd.DataFrame, engine):
    """Inserts a dataframe into the bronze.transactions table"""

    df.to_sql(
        name="transactions",
        con=engine,
        schema="bronze",
        if_exists="append",
        index=False,
    )

    print(f"Loaded {len(df)} rows into bronze.transactions")

def load_new_files_to_bronze():
    """
    Loads new CSV files into the bronze layer

    Only processes files that have not been loaded before

    Returns number of rows loaded
    """

    engine = get_engine()

    all_csv_files = sorted(RAW_DATA_PATH.glob("*.csv"))
    already_loaded_files = get_already_loaded_files(engine)

    # Find new files that haven't been loaded yet
    new_csv_files = [
        file_path for file_path in all_csv_files
        if file_path.name not in already_loaded_files
    ]

    if not new_csv_files:
        print("No new CSV files to load")
        return 0

    print(f"New files to load: {len(new_csv_files)}")

    # Read new files into a dataframe
    df = read_csv_files(new_csv_files)

    if df is None:
        print("No valid new CSV files found")
        return 0

    # Prepare and load data into bronze
    prepared_df = prepare_dataframe(df)
    load_to_bronze(prepared_df, engine)

    return len(prepared_df)

if __name__ == "__main__":
    rows_loaded = load_new_files_to_bronze()
    print(f"Rows loaded to bronze: {rows_loaded}")
