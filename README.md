# Project DE - Recruitment ETL Pipeline

This project implements a robust ETL (Extract, Transform, Load) pipeline using PySpark to process recruitment-related event data.

## Purpose
The main goal of this project is to aggregate raw event data (clicks, conversions, qualifications) from a distributed NoSQL database (Cassandra) and merge it with relational business data (Jobs/Companies) from MySQL to provide actionable insights.

## Architecture & Data Flow
The pipeline follows these steps:
1.  **Extract**: Retrieves new event data from **Apache Cassandra** based on the last processed timestamp.
2.  **Transform**: 
    - Processes raw events to calculate metrics for:
        - Clicks
        - Conversions
        - Qualified Applications
        - Unqualified Applications
    - Groups data by hour, job, and publisher.
3.  **Merge**: Joins the aggregated metrics with company/job metadata retrieved from **MySQL**.
4.  **Load**: Synchronizes the processed data into a final `events` table in **MySQL** for reporting and analysis.

## Tech Stack
-   **Language**: Python
-   **Processing Engine**: Apache Spark (PySpark)
-   **Databases**: 
    - Apache Cassandra (Source for high-volume event data)
    - MySQL (Source for metadata and destination for processed results)
-   **Environment**: Docker & Docker Compose for orchestration.

---

## Future Vision & Target Architecture (Roadmap)

To scale this project into a production-grade data platform, the following target architecture is planned:

### 1. Key Components & Infrastructure
The target deployment uses separate EC2 instances to avoid resource and port conflicts.

| Service | Port | EC2 Instance | Notes |
| :--- | :--- | :--- | :--- |
| **MySQL** | 3306 | Database Node | Dimension tables + ETL results |
| **Cassandra** | 9042 | Database Node | 3-node cluster for raw events |
| **Spark Master UI**| 8080 | Spark Node | Web UI for monitoring |
| **Airflow Web UI** | 8080 | Airflow Node | DAG management |

> [!NOTE]
> Spark UI and Airflow UI both use port 8080 by default, but stay conflict-free by running on separate EC2 instances.

### 2. Proposed Directory Structure
The project will be refactored into a modular structure:
```text
dataengineering/
├── code/
│   ├── pipeline/               # Core ETL modules (config, data_io, process)
│   ├── scripts/                # Utility scripts (data_generator.py)
│   └── dags/                   # Airflow DAGs (etl.py, gen-data.py)
├── docker/                     # Dedicated Docker configs per service
├── queries/                    # Schema definitions (MySQL/Cassandra)
└── data/                       # Sample seed data (CSV)
```

### 3. Key Enhancements
-   **Orchestration**: Implement **Apache Airflow** for scheduling (Hourly) and Watermark management.
-   **Cloud Integration (AWS)**: Migrate to **Amazon S3** (Data Lake), **AWS Glue/EMR** (Processing), and **Amazon RDS** (Storage).
-   **High Availability**: Scalable 3-node Cassandra cluster (Seed, Node-2, Node-3).

---
*Developed as part of the Data Engineering recruitment project.*
