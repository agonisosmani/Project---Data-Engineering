# Realtime Data Streaming | End-to-End Data Engineering Project


## Introduction

In this project, we will design a real-time data backend for a data-intensive streaming application using modern data engineering practices. The system will ingest, process, and aggregate large volumes of time-referenced data, ensuring scalability, reliability, and maintainability through a microservices-based architecture.

The key components include Apache Kafka for real-time data ingestion and streaming, Apache Zookeeper for distributed synchronization, and Apache Spark for efficient data processing. We’ll use Apache Airflow for pipeline orchestration, ensuring smooth data flow between components, and Cassandra for scalable storage of processed data. PostgreSQL will handle intermediate data storage during the pipeline's ingestion phase.

All services will be containerized using Docker to ensure consistency, easy deployment, and scalability across environments. 

We will simulate the real-time data flow using the randomuser.me API, which provides timestamped data. The system will process and aggregate this data to demonstrate real-time streaming capabilities. This architecture is designed to be modular, scalable, and resilient, ensuring future expandability and adaptation to various data sources and volumes. 

## App Architecture

![App Architecture](https://github.com/agonisosmani/Project---Data-Engineering/blob/main/App%20Architecture.PNG)

The project is designed with the following components:

- **Data Source**: We use `randomuser.me` API to generate random user data for our pipeline.
- **Apache Airflow**: Responsible for orchestrating the pipeline and storing fetched data in a PostgreSQL database.
- **Apache Kafka and Zookeeper**: Used for streaming data from PostgreSQL to the processing engine.
- **Control Center and Schema Registry**: Helps in monitoring and schema management of our Kafka streams.
- **Apache Spark**: For data processing with its master and worker nodes.
- **Cassandra**: Where the processed data will be stored.


## Technologies

- Apache Airflow
- Python
- Apache Kafka
- Apache Zookeeper
- Apache Spark
- Cassandra
- PostgreSQL
- Docker

## Getting Started

1. Clone the repository:
    ```bash
    git clone https://github.com/agonisosmani/Project---Data-Engineering
    ```

2. Navigate to the project directory:
    ```bash
    cd e2e-data-engineering
    ```

3. Run Docker Compose to spin up the services:
    ```bash
    docker-compose up
    ```


