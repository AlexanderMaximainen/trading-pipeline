import pandas as pd
from pathlib import Path
from urllib.parse import quote_plus
from sqlalchemy import create_engine, text
from fetch_data import read_csv_files

RAW_DATA_PATH = Path("data/raw")

SERVER = "MAXIMAINENPC"
DATABASE = "trading_pipeline"

def get_engine():
    odbc_connection_string = (
        "DRIVER={ODBC Driver 17 for SQL Server};"
        f"SERVER={SERVER};"
        f"DATABASE={DATABASE};"
        "Trusted_Connection=yes;"
    )

    connection_url = f"mssql+pyodbc:///?odbc_connect={quote_plus(odbc_connection_string)}"
    return create_engine(connection_url)

def get_already_loaded_files(engine):
    query = text("SELECT DISTINCT source_file FROM bronze.transactions")

    try:
        with engine.connect() as connection:
            df_loaded = pd.read_sql(query, connection)
        return set(df_loaded["source_file"].dropna().tolist())
    except Exception:
        return set()


def prepare_dataframe(df: pd.DataFrame):
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

    df.to_sql(
        name="transactions",
        con=engine,
        schema="bronze",
        if_exists="append",
        index=False,
    )

    print(f"Loaded {len(df)} rows into bronze.transactions")

if __name__ == "__main__":
    engine = get_engine()

    all_csv_files = sorted(RAW_DATA_PATH.glob("*.csv"))
    already_loaded_files = get_already_loaded_files(engine)

    new_csv_files = [
        file_path for file_path in all_csv_files
        if file_path.name not in already_loaded_files
    ]

    if not new_csv_files:
        print("No new CSV files to load")
    else:
        print(f"New files to load: {len(new_csv_files)}")

        df = read_csv_files(new_csv_files)

        if df is None:
            print("No valid new CSV files found")
        else:
            prepared_df = prepare_dataframe(df)
            load_to_bronze(prepared_df, engine)
