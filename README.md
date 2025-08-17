# BEES Data Engineering Case

## 📄 Project Description

This repository contains a complete solution for the BEES Data Engineering technical case. The project demonstrates the creation of an end-to-end data pipeline, from ingesting data from an external API to persisting it in a data lake, following the **Medallion Architecture** (Bronze, Silver, and Gold).

The pipeline is designed to be **scalable, robust, and observable**, covering aspects such as:

* **Data Ingestion:** Consuming data from the Open Brewery DB API with pagination handling.

* **Orchestration:** Managing the workflow using **Apache Airflow**.

* **Data Architecture:** Implementing a data lake with three layers to ensure data quality and organization.

* **Containerization:** Using **Docker** to modularize and simplify the execution environment.

* **Data Quality:** Automated validation and testing.

* **Monitoring and Alerts:** Strategies to ensure the health of the pipeline.

---

## 🎯 Case Objectives

The main goal is to build a solution that meets the following requirements:

1.  **API:** Extract data from the [Open Brewery DB API](https://www.openbrewerydb.org/).

2.  **Orchestration:** Orchestrate the data pipeline with a tool like Airflow.

3.  **Language:** Use Python for data processing and transformation.

4.  **Data Lake:** Implement the Medallion architecture with Bronze, Silver, and Gold layers.

5.  **Transformations:**
    * **Bronze:** Persist raw data in JSON format.
    * **Silver:** Transform data to Parquet format, partitioning by `state`.
    * **Gold:** Create an aggregated layer with the count of breweries per type and per state.

6.  **Containerization:** Use Docker and Docker Compose for environment isolation.

7.  **Repository:** Maintain the project in a public GitHub repository with clear documentation.

---

## 🛠️ Technologies Used

* **Python:** Main programming language.

* **Apache Airflow:** For pipeline orchestration.

* **Pandas:** For data manipulation and transformation.

* **Docker & Docker Compose:** For containerizing the development and production environment.

* **Git:** For version control.

---

## 🚀 How to Run the Project

Follow the steps below to set up and run the application on your machine.

### Prerequisites

Ensure you have [Docker](https://docs.docker.com/get-docker/) and [Docker Compose](https://docs.docker.com/compose/install/) installed.

### 1. Clone the Repository

```bash
git clone [https://github.com/YOUR_USERNAME/bees-data-engineering-case.git](https://github.com/YOUR_USERNAME/bees-data-engineering-case.git)
cd bees-data-engineering-case
```

### 2. Configure the Environment

No additional configuration is necessary. The `docker-compose.yml` will automatically set up the Airflow environment and dependencies.

### 3. Run the Pipeline

Start the Docker environment with the following command:

```bash
docker-compose up -d --build
```

Wait a few minutes for all services to start (Airflow Webserver, Scheduler).

Access the Airflow UI in your browser:
[http://localhost:8080](http://localhost:8080)

* **Default User:** `airflow`
* **Default Password:** `airflow`

In the Airflow UI, you will find the DAG (Directed Acyclic Graph) named `breweries_pipeline`. Enable it and trigger it manually to start the flow.

### 4. Project Structure

* `dags/`: Contains the `breweries_pipeline.py` file with the DAG definition.
* `src/`: Contains the Python code for the extraction and transformation functions.
* `data/`: Output folder for the data lake layers (Bronze, Silver, and Gold). **This folder will be created automatically after the pipeline runs.**
* `docker-compose.yml`: File to orchestrate the Docker containers (Airflow Webserver and Scheduler).

---

## 🧠 Design and Architectural Decisions

### Why Airflow?

**Apache Airflow** was chosen as the orchestrator for its ability to manage complex workflows programmatically. It allows for defining tasks, schedules, dependencies, and, crucially, implementing robust **retries and error handling**, ensuring pipeline resilience.

### Why the Medallion Architecture?

The Medallion architecture is a recommended practice for data lakes because:

* **Bronze:** Guarantees the immutability of raw data, serving as a reliable source for reprocessing.
* **Silver:** Standardizes and cleans the data, making it ready for consumption and analysis, with the added benefit of partitioning for query optimization.
* **Gold:** Provides aggregated data ready for analytical use or for feeding dashboards, without the need to re-process large volumes repeatedly.

### Partitioning Strategy

The Silver layer is partitioned by the `state` field. This decision was made to optimize analytical queries, as the final aggregation requires a grouping by location. Partitioning allows query engines to skip irrelevant data, improving performance.

### Validation and Tests

The pipeline includes validations to ensure data quality. Test functions were created to verify the structure of the extracted data and its integrity after transformation.

### Monitoring and Alerts

For production monitoring, the following would be implemented:

* **UI of Airflow:** The Airflow interface provides a visual status of tasks (success, failure, running).
* **Email/Slack Alerts:** Configure callbacks in Airflow to send notifications via email or Slack in case of a task failure.
* **Data Quality Checks:** Tools like **Great Expectations** could be integrated to validate data quality at each data lake layer and fail the pipeline if integrity is compromised.

---

## 🤝 Contact

If you have any questions about the solution, feel free to get in touch.
