# Data Pipeline for Ingesting Sensor Data to Database

## Description
This project establishes a data pipeline to generate sample data and load it into a PostgreSQL database. The pipeline is orchestrated with Apache Airflow and containerized using Docker for streamlined deployment and management.

## Installation

### Prerequisites
- Docker installed on your machine. [Install Docker](https://docs.docker.com/get-docker/) if you don't have it.
- A working directory prepared for Airflow.

### Steps
1. **Set Up Working Directory**: 
    - Create the following files in your working directory:
      - `Dockerfile`
      - `docker-compose.yml`
      - `requirements.txt`
    - Your working directory should be organized as follows:

    ```
    work directory
    ├─ docker-compose.yml
    ├─ Dockerfile
    └─ requirements.txt
    ```

2. **Build Custom Airflow Image**:
    ```sh
    docker build --tag extending_airflow:latest .
    ```

3. **Start Docker Containers**:
    ```sh
    docker-compose up -d
    ```

4. **Set Up PostgreSQL Database**:
    - After Docker starts, PostgreSQL will be running. The default credentials for PostgreSQL are:
      - **Server Name**: `localhost`
      - **Username**: `airflow`
      - **Password**: `airflow`
    - Access the PostgreSQL database and create the necessary table using the following SQL DDL files:
    ```
    \extract-user-info\user_info.sql
    \incremental_data_load_test\raw_transaction.sql
    \complex_data_transformation_test\daily_transaction_amount.sql
    \complex_data_transformation_test\product_sales.sql
    \complex_data_transformation_test\top_sellers_chiangmai.sql
    \complex_data_transformation_test\user_location_gender_trasaction.sql
    \complex_data_transformation_test\user_transaction_amount.sql
    ```