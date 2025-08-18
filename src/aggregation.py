# /opt/airflow/dags/aggregation.py
# This script reads the clean data from the Silver layer, performs aggregations,
# and saves the results in the Gold layer.

import pandas as pd
import os
import logging
import json
import pyarrow.parquet as pq

# Define the paths for the Silver and Gold layers
SILVER_LAYER_PATH = '/opt/airflow/data/silver'
GOLD_LAYER_PATH = '/opt/airflow/data/gold'

# Set up basic logging for the script
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def aggregate_data() -> pd.DataFrame:
    """
    Reads data from the Silver layer, aggregates it, and returns a DataFrame.
    This version reads each partitioned file manually to avoid 'type' issues.
    
    Returns:
        pd.DataFrame: A DataFrame with the aggregated data.
    """
    logging.info("Starting to read and aggregate data from the Silver layer...")
    
    # Check if the Silver layer exists and is not empty
    if not os.path.exists(SILVER_LAYER_PATH) or not os.listdir(SILVER_LAYER_PATH):
        raise FileNotFoundError(f"Silver layer directory not found or is empty at {SILVER_LAYER_PATH}.")
        
    all_dataframes = []
    
    try:
        # Iterate through the subdirectories (partitions) in the Silver layer
        for root, dirs, files in os.walk(SILVER_LAYER_PATH):
            for file in files:
                # Check if the file is a Parquet file
                if file.endswith('.parquet'):
                    file_path = os.path.join(root, file)
                    logging.info(f"Reading Parquet file: {file_path}...")
                    
                    # Read each Parquet file individually and append to the list
                    df_temp = pd.read_parquet(file_path, engine='pyarrow')
                    all_dataframes.append(df_temp)
        
        # Concatenate all DataFrames into a single one
        df = pd.concat(all_dataframes, ignore_index=True)
        
        logging.info("Successfully read and combined all data from the Silver layer.")
        logging.info(f"Total number of records to be aggregated: {len(df)}")
        
        # --- Aggregation Logic ---
        
        # Group by 'state' and 'city' to count the number of breweries in each
        logging.info("Aggregating data by state and city...")
        df_aggregated = df.groupby(['state', 'city']).size().reset_index(name='number_of_breweries')
        
        # Sort the results for better readability
        df_aggregated = df_aggregated.sort_values(by=['state', 'city']).reset_index(drop=True)
        
        logging.info("Aggregation completed.")
        return df_aggregated
        
    except Exception as e:
        logging.error(f"Error processing data from the Silver layer: {e}")
        # Re-raise the exception to fail the Airflow task
        raise

def save_to_gold(df: pd.DataFrame, path: str):
    """
    Saves the aggregated DataFrame to the Gold layer as a CSV file.
    The Gold layer is overwritten on each run.
    
    Args:
        df: The DataFrame to be saved.
        path: The target path for the Gold layer.
    """
    logging.info("Saving aggregated data to the Gold layer...")
    
    # Define the output file path with the new .csv extension
    output_file_path = os.path.join(path, 'aggregated_breweries.csv')
    
    # Ensure the directory exists
    os.makedirs(path, exist_ok=True)
    
    try:
        # Save the DataFrame to a CSV file, without the index
        df.to_csv(output_file_path, index=False)
        logging.info(f"Data successfully saved to {output_file_path}.")
    except Exception as e:
        logging.error(f"Failed to save aggregated data to CSV: {e}")
        raise

def main():
    """
    Main function to orchestrate the aggregation process.
    Reads from Silver, aggregates, and saves to Gold.
    """
    try:
        # Perform the aggregation
        df_aggregated = aggregate_data()
        
        # Save the aggregated data to the Gold layer
        save_to_gold(df_aggregated, GOLD_LAYER_PATH)
        
        logging.info("Aggregation pipeline finished successfully.")
        
    except Exception as e:
        logging.error(f"An error occurred during the pipeline execution: {e}")
        # This will cause the Airflow task to fail
        raise

if __name__ == '__main__':
    main()
