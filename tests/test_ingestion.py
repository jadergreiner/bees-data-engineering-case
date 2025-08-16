# tests/test_ingestion.py
# This script contains unit tests for the ingestion.py script.

import unittest
from unittest.mock import patch, mock_open
import json
import os
import sys
import requests  # <-- CORREÇÃO: Importa a biblioteca requests

# Add the parent directory to the system path to allow importing modules
# from the 'src' directory.
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Import the functions from the ingestion script
from src import ingestion

class TestIngestionScript(unittest.TestCase):
    """
    Test suite for the ingestion.py script.
    """

    def setUp(self):
        """
        Setup method to define mock data and cleanup paths.
        """
        # Define mock API response data
        self.mock_breweries_data = [
            {"id": "1", "name": "Brewery A", "state": "California"},
            {"id": "2", "name": "Brewery B", "state": "New York"}
        ]
        
        # Define the target path for the Bronze layer
        self.bronze_path = "/opt/airflow/data/bronze"

    @patch('requests.get')
    def test_fetch_breweries_data_success(self, mock_get):
        """
        Test that fetch_breweries_data successfully fetches data from the API.
        """
        # Configure the mock object to return a successful response
        mock_response = unittest.mock.Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = self.mock_breweries_data
        mock_get.return_value = mock_response
        
        # Call the function with test data
        data = ingestion.fetch_breweries_data(page=1, per_page=2)
        
        # Assertions
        self.assertEqual(data, self.mock_breweries_data)
        mock_get.assert_called_once_with(
            ingestion.API_BASE_URL,
            params={"page": 1, "per_page": 2}
        )

    @patch('requests.get')
    def test_fetch_breweries_data_failure(self, mock_get):
        """
        Test that fetch_breweries_data handles a failed API request (e.g., 404).
        """
        # Configure the mock object to raise an HTTPError
        mock_response = unittest.mock.Mock()
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError
        mock_get.return_value = mock_response
        
        # Call the function and check the return value
        data = ingestion.fetch_breweries_data(page=1, per_page=2)
        
        # Assertion
        self.assertEqual(data, [])

    @patch('os.makedirs')
    @patch('builtins.open', new_callable=mock_open)
    @patch('os.path.exists', return_value=False)
    def test_save_data_to_bronze_creation(self, mock_exists, mock_file, mock_makedirs):
        """
        Test that save_data_to_bronze creates the directory and saves the data.
        """
        file_name = "test_file.json"
        ingestion.save_data_to_bronze(self.mock_breweries_data, file_name)
        
        # Assertions
        mock_exists.assert_called_once_with(self.bronze_path)
        mock_makedirs.assert_called_once_with(self.bronze_path)
        
        # CORREÇÃO: Asserção para o conteúdo do arquivo
        handle = mock_file()
        written_content = "".join([call[0][0] for call in handle.write.call_args_list])
        expected_content = json.dumps(self.mock_breweries_data, indent=4)
        self.assertEqual(written_content, expected_content)

    @patch('os.makedirs')
    @patch('builtins.open', new_callable=mock_open)
    @patch('os.path.exists', return_value=True)
    def test_save_data_to_bronze_without_creation(self, mock_exists, mock_file, mock_makedirs):
        """
        Test that save_data_to_bronze works without creating the directory.
        """
        file_name = "test_file.json"
        ingestion.save_data_to_bronze(self.mock_breweries_data, file_name)
        
        # Assertion: makedirs should NOT be called
        mock_makedirs.assert_not_called()
        
    @patch('src.ingestion.save_data_to_bronze')
    @patch('src.ingestion.fetch_breweries_data')
    def test_main_script(self, mock_fetch, mock_save):
        """
        Test the main orchestration logic of the script.
        """
        # Configure fetch_breweries_data to return data and then an empty list
        mock_fetch.side_effect = [
            self.mock_breweries_data,  # First page
            self.mock_breweries_data,  # Second page
            []                         # End of data
        ]
        
        ingestion.main()
        
        # Assertions
        self.assertEqual(mock_fetch.call_count, 3)
        mock_save.assert_called_once()
        # You can add more detailed assertions about the data passed to mock_save

if __name__ == '__main__':
    unittest.main()
