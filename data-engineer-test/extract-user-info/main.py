import psycopg2
import pandas as pd
import json
import os

# Paths for the configuration files
db_config_file_path = './config/db_config.json'
mapping_file_path = './config/mapping.json'
csv_file_path = '../ref/user_info.csv'

def load_config(file_path, config_type):
    """
    Load a JSON configuration file.
    
    :param file_path: Path to the JSON file.
    :param config_type: Description of the configuration (e.g., "Database config").
    :return: Parsed JSON object.
    """
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

def apply_transformations(data, column_name, transform_type):
    """
    Apply transformations to the data based on the transform type.
    
    :param data: DataFrame containing the data.
    :param column_name: Name of the column to transform.
    :param transform_type: Type of transformation to apply (e.g., 'strip', 'capitalize', 'none').
    :return: Transformed column.
    """
    if transform_type == "none":
        return data[column_name]
    elif transform_type == "capitalize":
        return data[column_name].str.capitalize()
    elif transform_type == "strip":
        return data[column_name].str.strip()
    else:
        raise ValueError(f"Unknown transform type: {transform_type}")

def load_csv_to_table_with_mapping(csv_file, mapping_file, db_config_file):
    try:
        # Load database configuration
        db_config = load_config(db_config_file, "Database configuration")
        
        # Load mapping configuration
        mapping = load_config(mapping_file, "Mapping configuration")
        
        table_name = mapping['table_name']
        column_mappings = mapping['columns']
        
        # Extract source and target columns
        source_columns = [col['source_column'] for col in column_mappings]
        target_columns = [col['target_column'] for col in column_mappings]
        data_types = {col['source_column']: col['data_type'] for col in column_mappings}
        transforms = {col['source_column']: col['transform'] for col in column_mappings}
        
        # Load the CSV into a pandas DataFrame
        data = pd.read_csv(csv_file)
        
        # Apply transformations to the data
        for col in column_mappings:
            source_col = col['source_column']
            transform_type = col['transform']
            data[source_col] = apply_transformations(data, source_col, transform_type)
        
        # Select and reorder columns based on the source mapping
        data = data[source_columns]
        
        # Establish a connection to the PostgreSQL database
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()

        # Generate dynamic SQL for inserting rows
        cols = ', '.join(target_columns)
        values_placeholder = ', '.join(['%s'] * len(target_columns))

        # Iterate through the DataFrame and insert rows into the database
        for index, row in data.iterrows():
            cursor.execute(f"""
                INSERT INTO {table_name} ({cols})
                VALUES ({values_placeholder})
            """, tuple(row))

        # Commit the transaction
        conn.commit()
        print(f"Data loaded successfully into {table_name}!")

    except Exception as e:
        print("An error occurred:", e)

    finally:
        # Close the cursor and connection
        if cursor:
            cursor.close()
        if conn:
            conn.close()

# Call the function to load data
load_csv_to_table_with_mapping(csv_file_path, mapping_file_path, db_config_file_path)
