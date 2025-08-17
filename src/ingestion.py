# src/ingestion.py
# This script extracts data from the Open Brewery DB API and saves it
# to the Bronze layer of the data lake.

import requests
import json
import os
import logging
from datetime import datetime

# Configure basic logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# API endpoint and data path configuration
API_BASE_URL = "https://api.openbrewerydb.org/v1/breweries"
BRONZE_LAYER_PATH = "/opt/airflow/data/bronze"

def fetch_breweries_data(page: int, per_page: int) -> list:
    """
    Fetches a single page of brewery data from the API.

    Args:
        page (int): The page number to fetch.
        per_page (int): The number of results per page.

    Returns:
        list: A list of brewery dictionaries.
    """
    params = {
        "page": page,
        "per_page": per_page
    }
    try:
        logging.info(f"Fetching data from page {page}...")
        response = requests.get(API_BASE_URL, params=params)
        response.raise_for_status()  # Raises an HTTPError for bad responses
        return response.json()
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching data from the API: {e}")
        return []

def save_data_to_bronze(data: list, file_name: str):
    """
    Saves the list of dictionaries to a JSON file in the Bronze layer.

    Args:
        data (list): The list of dictionaries to save.
        file_name (str): The name of the output file.
    """
    if not os.path.exists(BRONZE_LAYER_PATH):
        os.makedirs(BRONZE_LAYER_PATH)
        logging.info(f"Created directory: {BRONZE_LAYER_PATH}")

    file_path = os.path.join(BRONZE_LAYER_PATH, file_name)
    try:
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4)
        logging.info(f"Successfully saved data to {file_path}")
    except IOError as e:
        logging.error(f"Error saving data to file: {e}")

def main():
    """
    Main function to orchestrate the data ingestion process.
    It handles pagination to fetch all available data.
    """
    all_breweries_data = []
    page = 1
    per_page = 50  # Max value for per_page is 200, but 50 is a good practice for stability

    while True:
        data = fetch_breweries_data(page, per_page)
        if not data:
            logging.info("End of data reached or an error occurred. Stopping ingestion.")
            break
        
        all_breweries_data.extend(data)
        logging.info(f"Fetched {len(data)} breweries from page {page}. Total fetched: {len(all_breweries_data)}")
        page += 1

    if all_breweries_data:
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        output_file_name = f"breweries_raw_{timestamp}.json"
        save_data_to_bronze(all_breweries_data, output_file_name)
        logging.info(f"Total breweries ingested: {len(all_breweries_data)}")
    else:
        logging.warning("No data was ingested. Check the API or the script.")

if __name__ == "__main__":
    main()