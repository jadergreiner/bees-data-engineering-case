# /opt/airflow/dags/transformation.py
# This script transforms raw data from the Bronze layer into a structured and
# clean format, saving the result in the Silver layer.
# The Silver layer is overwritten on each run to ensure it's the source of truth.

import pandas as pd
import os
import logging
import json
import shutil
import pyarrow as pa
import pyarrow.parquet as pq

# Define the paths for the Bronze and Silver layers
BRONZE_PATH = '/opt/airflow/data/bronze'
SILVER_PATH = '/opt/airflow/data/silver'

# Set up basic logging for the script
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Performs data cleaning and transformation.
    
    Args:
        df: The raw DataFrame to be transformed.
        
    Returns:
        pd.DataFrame: The transformed DataFrame.
    """
    logging.info("Starting data transformation...")
    
    # 1. Convert specific columns to a numeric type, handling errors
    for col in ['longitude', 'latitude']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # 2. Drop rows with missing values in key columns
    df.dropna(subset=['id', 'name'], inplace=True)
    
    # 3. Clean string columns (e.g., strip whitespace)
    # Garante que todas as colunas de string são tratadas como tal, incluindo 'state'
    for col in ['name', 'city', 'state', 'brewery_type']:
        if col in df.columns:
            # Force string type and strip whitespace
            df[col] = df[col].astype(str).str.strip()

    logging.info("Data transformation completed.")
    return df

def save_to_silver(df: pd.DataFrame, path: str):
    """
    Saves the transformed DataFrame to the Silver layer, overwriting previous data.
    This version partitions the data manually and explicitly sets the schema to avoid
    any type conflicts during reading.
    
    Args:
        df: The DataFrame to save.
        path: The target path to save the data.
    """
    logging.info(f"Saving transformed data to the Silver layer partitioned by 'state'...")
    
    try:
        # Check if the directory exists and remove it to ensure a clean overwrite
        if os.path.exists(path):
            logging.info(f"Removing old Silver layer data at {path}...")
            shutil.rmtree(path)
        
        # Get a list of unique states to create partitions
        unique_states = df['state'].unique()
        
        for state in unique_states:
            # Create a sub-directory for the state partition
            partition_path = os.path.join(path, f"state={state}")
            os.makedirs(partition_path, exist_ok=True)
            
            # Filter the DataFrame for the current state
            df_partition = df[df['state'] == state]
            
            # --- CORREÇÃO AQUI ---
            # Forçamos a conversão do DataFrame para uma Tabela PyArrow com o tipo de dado
            # de cada coluna definido explicitamente, evitando o "dictionary-encoding".
            schema = pa.Schema.from_pandas(df_partition, preserve_index=False)
            
            # Remove any dictionary types that might have been inferred
            new_fields = []
            for field in schema:
                if pa.types.is_dictionary(field.type):
                    new_fields.append(pa.field(field.name, pa.string()))
                else:
                    new_fields.append(field)
            
            # Create a new schema with all string fields
            new_schema = pa.schema(new_fields)
            
            # Convert the pandas DataFrame to a PyArrow Table using the new schema
            pa_table = pa.Table.from_pandas(df_partition, schema=new_schema, preserve_index=False)
            
            # Define the filename for the Parquet file
            file_name = f"breweries_{state}.parquet"
            file_path = os.path.join(partition_path, file_name)
            
            logging.info(f"Saving partition 'state={state}' to {file_path}...")
            
            # Save the partition to a Parquet file, explicitly setting compression
            # and a few other options for consistency
            pq.write_table(pa_table, file_path, compression='snappy')
            
        logging.info("Successfully saved partitioned data to the Silver layer.")
    except Exception as e:
        logging.error(f"Failed to save transformed data to Parquet: {e}")
        # Re-raise the exception to fail the Airflow task
        raise

def main() -> str:
    """
    Main function to orchestrate the transformation process.
    Reads from Bronze, transforms, and saves to Silver.
    
    Returns:
      str: The path to the Silver layer for the next task (via XCom).
    """
    logging.info("Starting transformation pipeline...")
    
    # Check if Bronze layer directory exists and has files
    if not os.path.exists(BRONZE_PATH) or not os.listdir(BRONZE_PATH):
        raise FileNotFoundError(f"Bronze layer directory not found or is empty at {BRONZE_PATH}.")

    try:
        # Find the latest JSON file in the Bronze layer
        files = [os.path.join(BRONZE_PATH, f) for f in os.listdir(BRONZE_PATH) if f.endswith('.json')]
        if not files:
            raise FileNotFoundError(f"No JSON files found in the Bronze layer at {BRONZE_PATH}.")
        
        # Sort files by creation/modification time to get the latest one
        latest_file = max(files, key=os.path.getmtime)
        
        # Read the raw JSON data
        df = pd.read_json(latest_file)
        
    except Exception as e:
        logging.error(f"Failed to read data from Bronze layer: {e}")
        raise ValueError(f"Error reading raw data. It might be corrupt or in the wrong format.")
        
    # Transform the data
    transformed_df = transform_data(df)
    
    # Save the transformed data
    save_to_silver(transformed_df, SILVER_PATH)
    
    logging.info("Transformation pipeline finished successfully.")
    
    return SILVER_PATH

if __name__ == '__main__':
    main()
