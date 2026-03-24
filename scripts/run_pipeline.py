"""
Runs the full pipeline:

1. Loads new data into bronze
2. If new data was loaded, runs the silver layer

This makes sure everything runs in the right order.
"""

from sqlalchemy import text
from load_data import get_engine, load_new_files_to_bronze

def run_silver_pipeline():
    """Execute the SQL procedure that loads the silver layer"""

    engine = get_engine()

    with engine.begin() as connection:
        connection.execute(text("EXEC silver.usp_load_silver;"))

    print("Silver pipeline completed successfully")

def main():
    """Run the full pipeline: load new data to bronze and update silver layer"""
    
    try:
        print("Starting pipeline...")

        rows_loaded = load_new_files_to_bronze()

        # Only run silver if new data was loaded
        if rows_loaded == 0:
            print("No new bronze data loaded. Silver pipeline was not executed.")
            return

        print("Running silver master procedure...")
        run_silver_pipeline()

        print("Pipeline completed successfully")

    except Exception as e:
        print(f"Pipeline failed: {e}")
        raise

if __name__ == "__main__":
    main()