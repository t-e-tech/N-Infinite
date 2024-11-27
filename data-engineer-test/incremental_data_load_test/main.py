import os
import psycopg2
import pandas as pd
import json

# Configuration file paths
db_config_file_path = './config/db_config.json'
csv_folder_path = '../ref/sales/'
mapping_file_path = './config/mapping.json'
log_file_path = '/processed_sales.log'

def load_config(file_path, config_type):
    """Load a JSON configuration file."""
    try:
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"{config_type} file not found at path: {file_path}")
        
        with open(file_path, 'r') as file:
            return json.load(file)

    except FileNotFoundError as fnf_error:
        print(fnf_error)
        raise
    except json.JSONDecodeError as json_error:
        print(f"Error decoding {config_type} file: {json_error}")
        raise
    except Exception as e:
        print(f"Unexpected error while loading {config_type}: {e}")
        raise

def get_processed_files(log_file_path):
    """Read a log file to get already processed CSV filenames."""
    if os.path.exists(log_file_path):
        with open(log_file_path, 'r') as log_file:
            return set(log_file.read().splitlines())
    return set()

def log_processed_file(log_file_path, file_name):
    """Log the processed file name into the log file."""
    with open(log_file_path, 'a') as log_file:
        log_file.write(file_name + '\n')

def transform_data(data, mapping):
    """Transform data based on the mapping rules."""
    transformed_data = pd.DataFrame()
    for column in mapping['columns']:
        source = column['source_column']
        target = column['target_column']
        transform = column['transform']
        data_type = column['data_type']

        if transform == "none":
            transformed_data[target] = data[source]
        else:
            # Example: Add transformation logic here
            raise NotImplementedError(f"Transform '{transform}' not implemented.")

        # Apply type casting
        if data_type == "INTEGER":
            transformed_data[target] = transformed_data[target].astype(int)
        elif data_type == "DATE":
            transformed_data[target] = pd.to_datetime(transformed_data[target]).dt.date
        elif data_type == "VARCHAR":
            transformed_data[target] = transformed_data[target].astype(str)

    return transformed_data

def load_csv_to_table(csv_file, db_config_file, mapping_file):
    """Load a CSV file into the target table as per the mapping."""
    try:
        # Load configurations
        db_config = load_config(db_config_file, "Database configuration")
        mapping = load_config(mapping_file, "Mapping configuration")

        # Load the CSV into a pandas DataFrame
        data = pd.read_csv(csv_file)

        # Transform the data using the mapping
        transformed_data = transform_data(data, mapping)

        # Establish a connection to the PostgreSQL database
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()

        # SQL query for inserting or updating rows dynamically
        target_table = mapping['table_name']
        columns = [col['target_column'] for col in mapping['columns']]
        insert_query = f"""
            INSERT INTO {target_table} ({', '.join(columns)})
            VALUES ({', '.join(['%s'] * len(columns))})
            ON CONFLICT (order_id)
            DO UPDATE SET {', '.join([f"{col} = EXCLUDED.{col}" for col in columns])};
        """
        
        # Insert or update rows into the database
        for index, row in transformed_data.iterrows():
            cursor.execute(insert_query, tuple(row))
        
        # Commit the transaction
        conn.commit()
        print(f"Data loaded successfully from {csv_file}!")

    except Exception as e:
        print(f"An error occurred while processing {csv_file}: {e}")

    finally:
        # Close the cursor and connection
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def process_daily_sales(csv_folder_path, db_config_file, mapping_file, log_file_path):
    """Process and load incremental daily sales data."""
    # Get list of already processed files
    processed_files = get_processed_files(log_file_path)

    # List CSV files in the directory
    csv_files = [f for f in os.listdir(csv_folder_path) if f.endswith('.csv')]

    # Filter out already processed files
    new_files = [f for f in csv_files if f not in processed_files]

    if not new_files:
        print("No new files to process.")
        return

    # Load each new CSV file into the target table
    for csv_file in new_files:
        csv_file_path = os.path.join(csv_folder_path, csv_file)
        load_csv_to_table(csv_file_path, db_config_file, mapping_file)
        log_processed_file(log_file_path, csv_file)

# Run the ETL process
process_daily_sales(csv_folder_path, db_config_file_path, mapping_file_path, log_file_path)
