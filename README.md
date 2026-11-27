# Open Data Lakehouse Demo

## General Description

This project demonstrates an open data lakehouse architecture using various open-source technologies. The goal is to
showcase how to build a scalable and flexible data platform that combines the best aspects of data lakes and data
warehouses, enabling advanced analytics, machine learning, and business intelligence, combining familiar and portable
open source technologies together with cloud native capabilities and level of management.

The architecture typically includes:

- **Data Ingestion**: Tools for bringing data from various sources into the lakehouse.
- **Data Storage**: A data lake for raw and refined data, often using formats like Parquet or ORC.
- **Metadata Management**: A catalog to manage schemas and table definitions.
- **Query Engines**: Engines for querying data directly in the data lake.
- **Data Transformation**: Tools for processing and transforming data.
- **Visualization/BI**: Tools for reporting and dashboard.

## Architecture, Governance & Lineage

This demo showcases Google Cloud's Open Data Lakehouse stack built on **Apache Iceberg**:

- **BigQuery Managed Iceberg Tables (`Lakehouse for Apache Iceberg`)**: Managed storage lifecycle, clustering, and metadata export (`EXPORT TABLE METADATA`) for `bus_stations`, `ridership`, `bus_rides`, and `bus_rides_features`.
- **Lakehouse Runtime Catalog (`Apache Iceberg REST Catalog`) with Credential Vending**: Provisioned via Terraform (`google_biglake_iceberg_catalog`) with `CREDENTIAL_MODE_VENDED_CREDENTIALS`. Only the catalog's managed service account holds bucket-scoped `roles/storage.objectUser`; engines receive short-lived, table-scoped storage tokens without broad bucket IAM.
- **Dynamic 4-Part `P.C.N.T` Querying**: BigQuery queries Spark-managed `bus_lines` directly via `` `<PROJECT_ID>.<REST_CATALOG_BUCKET>.rest_namespace.bus_lines` `` exposed through a live BigQuery View (`ridership_lakehouse.bus_lines`), eliminating stale `metadata.json` external table pointers.
- **Knowledge Catalog Governance & Auto Data Quality**:
  - Terraform provisions a **Knowledge Catalog Business Glossary** (`ridership-lakehouse-glossary`) with canonical transit terms (`Demand Metric`, `Bus Capacity`, `Left Behind Passengers`).
  - Notebooks Part 2 and Part 3 idempotently create and run **Auto Data Quality scans** (`bus-rides-dq-scan` and `bus-rides-features-dq-scan`), exporting results to `ridership_lakehouse`.
- **End-to-End OpenLineage**:
  - Enabled natively on **Serverless for Apache Spark** interactive Spark Connect sessions (`spark.dataproc.lineage.enabled = "true"`) in Notebooks Parts 1–2, alongside automatic BigQuery lineage in Part 3.
  - *Limitation:* Serverless for Apache Spark automated OpenLineage capture supports batch and interactive sessions only — it does **not** cover Spark Structured Streaming (`assets/code/pyspark-job.py`).
- **BigQuery AI & ML Forecasting**: Compares `ARIMA_PLUS_XREG` (`BigQuery ML`) against zero-shot `AI.FORECAST` powered by **TimesFM**.

## Prerequisites

Before you begin, ensure you have the following installed:

- Terraform (`>= 1.5.7`; uses `hashicorp/google` `>= 7.37.0, < 8.0.0`)
- Google Cloud CLI (`gcloud` / `gcloud storage`)
- IAM permission `serviceusage.services.use` on your target `project_id` (required for User ADC `user_project_override = true` with the Lakehouse Runtime Catalog API)

## Deployment

1. **Make sure your `gcloud` CLI and Application Default Credentials are authenticated:**
   ```bash
   gcloud auth login
   gcloud auth application-default login
   ```
2. **Initialize Terraform:**
   ```bash
   cd terraform
   terraform init
   ```
3. **Create a `terraform.tfvars` file:**
   ```bash
   cp terraform.tfvars.example terraform.tfvars
   ```
4. **Edit `terraform.tfvars` with your `project_id` and `region`.**
5. **Run Terraform:**
   ```bash
   terraform apply
   ```
6. **Note the Terraform output variables** (including `cloud_run_url`, `iceberg_rest_catalog_id`, `iceberg_rest_catalog_sa`, and `knowledge_catalog_glossary_id`).

## Loading the notebooks

The first parts of this demo are step-by-step Jupyter notebooks designed to run in Google Cloud's **BigQuery Studio Notebooks** (with **Spark Connect** on **Serverless for Apache Spark**):

1. Open your browser and go to the [Google Cloud Console](https://console.cloud.google.com/).
2. Make sure you select the project where Terraform deployed the assets.
3. Navigate to **BigQuery Studio** → **Notebooks** (or search for `BigQuery Studio`).
4. Click **Upload** / **Import notebooks** and select **From Cloud Storage**.
5. Load all 4 Jupyter notebooks from the `<YOUR_PROJECT>-ridership-lakehouse` bucket under the `notebooks/` folder.
6. Open and run the notebooks in order. **Note:** Part 0 (`lakehouse_part0_data_generation`) is optional when using the pre-staged data copied by Terraform.

## Running the real-time analysis

Terraform also deploys a web app to Cloud Run to showcase near-real-time processing and alerting. From the Terraform outputs, note the `cloud_run_url` output variable, open it in a browser, and follow the on-screen instructions. **Note:** This part depends on running at least Notebooks Part 1 and Part 2 first.

## Project structure

In this repo, you will find the following folders:

- `assets`: Code and notebook assets
    - `code`: Contains the PySpark streaming job and supporting files
        - `ivySettings.xml`: Definition of Maven repos so the Spark job can pull external dependencies (`confluent-kafka` / Managed Kafka auth handler). In production, pre-bundle dependencies rather than pulling at runtime.
        - `pyspark-job.py`: The PySpark Structured Streaming job on Serverless for Apache Spark, listening to Kafka `bus-updates`, writing live state to BigQuery `bus_state`, and emitting alerts to `capacity-alerts`.
        - `run-pyspark.sh`: Shell script for manually submitting the streaming job.
    - `notebooks`: Contains all notebooks used in this demo, designed to run in **BigQuery Studio Notebooks** (pre-configured with `!` shell commands and Spark Connect). We use [jupytext](https://jupytext.readthedocs.io/en/latest/) so each `.ipynb` file has a paired `.py` percent script for clean Git diffs.
- `webapp`: The Flask web app displaying real-time bus capacity data, with two background workers triggered by UI controls (protected by `ADMIN_TOKEN` bearer auth on state-changing POST endpoints):
    - `kafka_service`: Queries the BigQuery `bus_rides` table for 10 days of historical data from the same calendar window in 2024, shifts timestamps forward to simulate live traffic, and publishes messages to the Kafka `bus-updates` topic.
    - `pyspark_service`: Starts, monitors, and cancels the Serverless for Apache Spark streaming batch job (`pyspark-streaming-job`).

## Development

In the `assets` and `webapp` folders, use `uv` to install the virtual environments for each sub-project.

When editing notebooks, notebook outputs must be cleared and contents kept synchronized with their matching `.py` percent files. Two tools are installed in the `dev` dependency group:

1. [jupytext](https://jupytext.readthedocs.io/en/latest/) for syncing content between `.ipynb` and `.py` files.
2. [nbstripout](https://github.com/kynan/nbstripout) for stripping outputs from `.ipynb` files.

Run both commands from the `assets` directory before opening a PR:

```bash
uv run jupytext --sync notebooks/*.py --to ipynb
uv run nbstripout notebooks/*.ipynb
```

### GitHub Actions

A GitHub Actions workflow checks two invariants on every PR to `main` or `workshop`:

1. **Notebook/script sync** via `jupytext`:
   ```bash
   uv run jupytext --test notebooks/*.ipynb --to py:percent
   ```
2. **Stripped outputs** via `nbstripout`:
   ```bash
   uv run nbstripout --verify notebooks/*.ipynb
   ```

These checks **verify** the files and do not modify PR commits automatically.