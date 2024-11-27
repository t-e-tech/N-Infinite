import psycopg2
import pandas as pd
import json
import os

def load_db_config(config_file):
    """Load database configuration from a JSON file."""
    try:
        with open(config_file, 'r') as file:
            config = json.load(file)
            return config
    except FileNotFoundError:
        print(f"Configuration file {config_file} not found.")
        raise
    except json.JSONDecodeError as e:
        print(f"Error parsing JSON configuration file: {e}")
        raise

def extract_raw_data(query, connection):
    """Extract raw data using the provided SQL query."""
    try:
        df = pd.read_sql(query, connection)
        return df
    except Exception as e:
        print(f"Error extracting data: {e}")
        return pd.DataFrame()

def transform_data(raw_data):
    """Transform the raw data to calculate aggregations."""
    # 1. User transaction amount
    user_transaction = raw_data.groupby('user_id').agg(
        total_sales=('amount', 'sum'),
        average_sales=('amount', 'mean')
    ).reset_index()

    # 2. Daily transaction amount
    daily_transaction = raw_data.groupby('order_date').agg(
        total_sales=('amount', 'sum'),
        min_sales=('amount', 'min'),
        max_sales=('amount', 'max'),
        average_sales=('amount', 'mean'),
    ).reset_index()
    daily_transaction['vat'] = daily_transaction['total_sales'] * 0.07

    # 3. Product sales
    product_sales = raw_data.groupby('product_id').agg(
        number_of_transactions=('order_id', 'count'),
        total_sales=('amount', 'sum')
    ).reset_index()

    return user_transaction, daily_transaction, product_sales

def load_to_database(df, table_name, connection):
    """Load the transformed data into the database."""
    try:
        df.to_sql(table_name, connection, if_exists='replace', index=False)
        print(f"Data loaded successfully into {table_name}.")
    except Exception as e:
        print(f"Error loading data into {table_name}: {e}")

def etl_process(config_file):
    """Main ETL process."""
    try:
        # Load database configuration
        db_config = load_db_config(config_file)

        # Connect to the database
        conn = psycopg2.connect(**db_config)

        # Extract data
        query = "SELECT * FROM raw_transaction;"
        raw_data = extract_raw_data(query, conn)

        # Transform data
        user_transaction, daily_transaction, product_sales = transform_data(raw_data)

        # Load data
        load_to_database(user_transaction, "user_transaction_amount", conn)
        load_to_database(daily_transaction, "daily_transaction_amount", conn)
        load_to_database(product_sales, "product_sales", conn)

    except Exception as e:
        print(f"Error in ETL process: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    CONFIG_FILE = "./config/db_config.json"
    etl_process(CONFIG_FILE)
