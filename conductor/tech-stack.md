# Tech Stack - Open Data Lakehouse Demo

## Languages
- **Python:** Used for data processing (PySpark), the backend web application (Flask), and scripting.
- **TypeScript:** Primary language for the frontend dashboard (Next.js).

## Frameworks & Libraries
- **Flask:** Lightweight Python web framework for the backend.
- **Next.js:** React framework for building the interactive dashboard.
- **Tailwind CSS:** Utility-first CSS framework for styling.
- **PySpark:** Python API for Apache Spark, used for large-scale data processing.

## Data Storage & Management
- **Apache Iceberg:** Open table format for huge analytic datasets, providing transactional consistency and schema evolution.
- **Apache Parquet:** Columnar storage format optimized for analytical queries.
- **Google Cloud Storage (GCS):** Scalable object storage for the data lake.
- **BigQuery:** Serverless, highly scalable data warehouse, integrated with the Iceberg catalog.

## Messaging & Processing
- **Apache Kafka:** Distributed event streaming platform for real-time data ingestion.
- **Apache Spark / Dataproc:** Managed Spark and Hadoop service for processing and transforming data.

## Infrastructure
- **Terraform:** Infrastructure as Code (IaC) for provisioning and managing Google Cloud resources.
