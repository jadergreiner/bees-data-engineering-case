# Dockerfile
# This Dockerfile extends the official Airflow image to install
# necessary Python libraries for our data pipeline.

# Use the official Airflow image as the base
FROM apache/airflow:2.8.1

# Install pandas and requests
# pandas is used for data manipulation and transformation.
# requests is used for making HTTP requests to the API.
RUN pip install --no-cache-dir \
    pandas \
    requests