# /opt/airflow/src/data_validation.py
# This script performs data quality checks on the data in the Silver layer.

import pandas as pd
import os
import logging

# Configure basic logging to ensure consistent output
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# Define the path to the Silver layer
SILVER_LAYER_PATH = "/opt/airflow/data/silver"

def validate_dataframe(df: pd.DataFrame):
    """
    Performs data quality checks on the transformed brewery DataFrame.
    
    Args:
        df (pd.DataFrame): The DataFrame to validate.
        
    Raises:
        ValueError: If any data validation check fails.
    """
    logging.info("Starting data validation...")
    
    # Check 1: Ensure DataFrame is not empty
    if df.empty:
        raise ValueError("The DataFrame is empty. No data to validate.")

    # Check 2: The 'id' column should not be missing or have null values.
    # We'll use 'name' and 'state' as our unique key, but let's keep the user's check on 'id' if it exists.
    if 'id' in df.columns:
        if df['id'].isnull().any():
            raise ValueError("Validation failed: 'id' column contains null values.")
        if not df['id'].is_unique:
            raise ValueError("Validation failed: 'id' column contains duplicate values.")

    # Check 3: The 'name' column should be of type 'string' (object in pandas).
    if 'name' not in df.columns or df['name'].dtype != 'object':
        raise ValueError("Validation failed: 'name' column is missing or not of type string.")

    # Check 4: If the 'latitude' column exists, values must be within the valid range.
    if 'latitude' in df.columns:
        invalid_latitude = df[df['latitude'].notnull() & ~df['latitude'].between(-90, 90)]
        if not invalid_latitude.empty:
            raise ValueError("Validation failed: 'latitude' column contains values outside the valid range (-90 to 90).")

    # Check 5: If the 'longitude' column exists, values must be within the valid range.
    if 'longitude' in df.columns:
        invalid_longitude = df[df['longitude'].notnull() & ~df['longitude'].between(-180, 180)]
        if not invalid_longitude.empty:
            raise ValueError("Validation failed: 'longitude' column contains values outside the valid range (-180 to 180).")

    logging.info("All data validation checks passed successfully!")
    # No need to return True here, as the function will raise an exception on failure
    # and implicitly succeed on completion.

def main():
    """
    Main function to orchestrate the validation process.
    Reads the data from the Silver layer and performs validation.
    """
    logging.info("Starting data validation pipeline step.")
    
    # Read all partitioned files from the Silver layer
    try:
        df = pd.read_parquet(SILVER_LAYER_PATH)
    except Exception as e:
        logging.error(f"Error reading data from the Silver layer: {e}")
        # Return None to signal a failure to the DAG
        return None 
    
    # Call the core validation function
    validate_dataframe(df)
    
    # Return the path to the Silver layer for the next task in the DAG via XCom
    return SILVER_LAYER_PATH

if __name__ == "__main__":
    main()
