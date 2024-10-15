# IFCO Data Engineering Test

## Description
This project uses Apache Spark for data processing, allowing users to efficiently perform data analysis using PySpark. The environment is managed using Docker and Poetry to ensure a clean and reproducible installation. A medallion architecture is used and pytest is used for unit testing.

The main project file is `run.py`, which acts as the **entry point** of the project. When executed, this file is responsible for **calling all the necessary functions** to perform data transformations and analysis using Spark.

Additionally, the project is set up to run unit tests using Pytest to ensure that all functions and components in the code are working correctly.To run in test mode, you must run the container with the MODE environment variable set to test

## Installation

### Requirements

- Have Docker installed.

### Installation Steps

1. Clone the repository:
   ```bash
   git clone https://github.com/diazknel/IFCO.git
   cd IFCO
2. Build the Docker container:   
   ```bash
   docker compose build     
3. Executer the container
   ```bash
   docker run -e MODE=app ifco_v2-pyspark-app 
4. Test Mode all Unit Test
    ```bash
    docker run -e MODE=test ifco_v2-pyspark-app
5. Test Mode specfic Unit Test
   ```bash
   docker run -e MODE=test -e TEST_FILE="tests/test_bronze.py" ifco_v2-pyspark-app
