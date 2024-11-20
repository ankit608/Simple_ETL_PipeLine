import pandas as pd
import sqlite3

def extract_csv(file_path):
    try:
        data = pd.read_csv(file_path)
        print("Data successfully extracted!")
        return data
    except Exception as e:
        print(f"Error during extraction: {e}")
        return None


def transform_data(data):
    try:
        # Example transformations:
        # 1. Drop rows with missing values
        data = data.dropna()

        # 2. Convert column to proper format (e.g., date)
        if 'date' in data.columns:
            data['date'] = pd.to_datetime(data['date'], errors='coerce')

        # 3. Rename columns for consistency
        data.columns = data.columns.str.lower().str.replace(' ', '_')

        print("Data successfully transformed!")
        return data
    except Exception as e:
        print(f"Error during transformation: {e}")
        return None


def load_to_sqlite(data, db_name, table_name):
    try:
        conn = sqlite3.connect(db_name)
        data.to_sql(table_name, conn, if_exists='replace', index=False)
        conn.close()
        print(f"Data successfully loaded into {table_name} table of {db_name} database!")
    except Exception as e:
        print(f"Error during loading: {e}")


def etl_pipeline(csv_file_path, db_name, table_name):
    print("Starting ETL pipeline...")
    data = extract_csv(csv_file_path)
    if data is not None:
        data = transform_data(data)
        if data is not None:
            load_to_sqlite(data, db_name, table_name)
    print("ETL pipeline completed.")

# Example usage
csv_file_path = "example.csv"
db_name = "example.db"
table_name = "processed_data"

etl_pipeline(csv_file_path, db_name, table_name)
