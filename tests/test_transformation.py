# tests/test_transformation.py
# This script contains unit tests for the transformation.py script.

import unittest
from unittest.mock import patch, mock_open
import os
import sys
import polars as pl
import logging
import shutil

# Add the parent directory to the system path to allow importing modules
# from the 'src' directory.
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Import the functions from the transformation script
from src import transformation

# Configure basic logging for the test script
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

class TestTransformationScript(unittest.TestCase):
    """
    Test suite for the transformation.py script.
    """

    def setUp(self):
        """
        Setup method to define mock data and configure test paths.
        """
        # Define mock raw data, including some missing or inconsistent values
        self.mock_raw_data = [
            {"id": "1", "name": "Brewery A", "brewery_type": "micro", "city": "Springfield", "state": "California", "country": "United States", "longitude": "1.1", "latitude": "2.2", "phone": "123"},
            {"id": "2", "name": "Brewery B", "brewery_type": "BREWPUB", "city": "Oakland", "state": "California", "country": "United States", "longitude": "3.3", "latitude": "4.4", "phone": "456"},
            {"id": "3", "name": "Brewery C", "brewery_type": None, "city": "New York", "state": "New York", "country": "United States", "longitude": "5.5", "latitude": None, "phone": "789"},
            {"id": "4", "name": "Brewery D", "brewery_type": "regional", "city": "Chicago", "state": "Illinois", "country": "United States", "longitude": "7.7", "latitude": "8.8", "phone": None}
        ]
        
        # Define the expected transformed data
        self.expected_transformed_data = pl.DataFrame({
            "brewery_id": ["1", "2", "3", "4"],
            "brewery_name": ["Brewery A", "Brewery B", "Brewery C", "Brewery D"],
            "brewery_type": ["micro", "brewpub", "unknown", "regional"],
            "city": ["Springfield", "Oakland", "New York", "Chicago"],
            "state": ["California", "California", "New York", "Illinois"],
            "country": ["United States", "United States", "United States", "United States"],
            "longitude": ["1.1", "3.3", "5.5", "7.7"],
            "latitude": ["2.2", "4.4", None, "8.8"],
            "phone": ["123", "456", "789", None]
        })

    def test_load_data_from_bronze_success(self):
        """
        Test if load_data_from_bronze successfully loads data.
        """
        logging.info("Running test_load_data_from_bronze_success...")
        # Patch the read_json function and os.path.exists
        with patch('polars.read_json', return_value=pl.DataFrame(self.mock_raw_data)), \
             patch('os.path.exists', return_value=True):
            df = transformation.load_data_from_bronze("test_file.json")
            self.assertFalse(df.is_empty())
            self.assertEqual(len(df), len(self.mock_raw_data))

    def test_load_data_from_bronze_file_not_found(self):
        """
        Test if load_data_from_bronze returns an empty DataFrame if the file is not found.
        """
        logging.info("Running test_load_data_from_bronze_file_not_found...")
        # Patch os.path.exists to simulate a file not found scenario
        with patch('os.path.exists', return_value=False):
            df = transformation.load_data_from_bronze("non_existent_file.json")
            self.assertTrue(df.is_empty())

    def test_transform_data(self):
        """
        Test if transform_data correctly cleans and normalizes the data.
        """
        logging.info("Running test_transform_data...")
        # Convert mock data to a Polars DataFrame for the test
        input_df = pl.DataFrame(self.mock_raw_data)
        transformed_df = transformation.transform_data(input_df)
        
        # Assertions to check for correct transformations
        self.assertFalse(transformed_df.is_empty())
        self.assertEqual(transformed_df.columns, self.expected_transformed_data.columns)
        
        # Check if "brewery_type" is correctly lowercased and nulls are filled
        self.assertTrue(all(col in ["micro", "brewpub", "unknown", "regional"] for col in transformed_df["brewery_type"]))
        
        # Check if all data matches the expected DataFrame
        self.assertTrue(transformed_df.equals(self.expected_transformed_data))

    def test_save_data_to_silver(self):
        """
        Test if save_data_to_silver correctly saves and partitions the data.
        """
        logging.info("Running test_save_data_to_silver...")
        
        # Mock the Polars write_parquet method
        with patch.object(pl.DataFrame, 'write_parquet') as mock_write_parquet, \
             patch('os.makedirs'):
            transformation.save_data_to_silver(self.expected_transformed_data)
            
            # Assertions to check if write_parquet was called with the correct arguments
            mock_write_parquet.assert_called_once()
            call_args, call_kwargs = mock_write_parquet.call_args
            
            # Check if the file path is correct
            self.assertTrue(call_kwargs['file'].endswith("breweries.parquet"))
            
            # Check if the partitioning column is correct
            self.assertEqual(call_kwargs['partition_by'], "state")
            
    def test_main_orchestration(self):
        """
        Test the main function to ensure it orchestrates the pipeline correctly.
        """
        logging.info("Running test_main_orchestration...")
        # Patch the functions that `main` calls to ensure it works correctly
        with patch('src.transformation.load_data_from_bronze', return_value=pl.DataFrame(self.mock_raw_data)) as mock_load, \
             patch('src.transformation.transform_data', return_value=self.expected_transformed_data) as mock_transform, \
             patch('src.transformation.save_data_to_silver') as mock_save:
            
            transformation.main("test_file.json")
            
            # Assertions to verify that each function was called exactly once
            mock_load.assert_called_once()
            mock_transform.assert_called_once_with(pl.DataFrame(self.mock_raw_data))
            mock_save.assert_called_once_with(self.expected_transformed_data)

if __name__ == "__main__":
    unittest.main()