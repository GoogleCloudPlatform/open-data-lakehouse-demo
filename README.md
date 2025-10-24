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

## Prerequisites

Before you begin, ensure you have the following installed:

- Terraform
- gsutil

## Deployment

This project might include Terraform configurations for deploying to cloud providers like AWS.

1. **Make sure your gsutil/gcloud is authenticated :**
   Ensure your gcloud utility is configured with appropriate credentials and a default region.
   ```bash
   gcloud auth
   ```
2. **Initialize Terraform:**
   ```bash
   cd terraform
   terraform init

3. **Create a `terraform.tfvars` file**
   ```bash
   cp terraform.tfvars.example terraform.tfvars

4. **Edit the `terraofmr.tfvars` file to hold your variables**

5. **Run terraform**
   ```bash
   terraform apply
   ```
6. **Note the terraform output variables**
   We will need them later.

## Loading the notebooks

The first parts of this demo are based on step by step guide in a Jupyter Notebooks, best run on Google Clouds
**Colab Enterprise** product.

1. Open your browser and go to the [Google Clouds Console](https://console.cloud.google.com/).
2. Make sure you select the right project where the terraform setup deployed the assets.
3. In the search bar, search for `Colab Enterprise`
4. Click "Import notebooks"
5. Select the "From Cloud Storage" option
6. Load all 4 Jupyter notebooks from the `<YOUR_PROJECT>-lakehouse-ridership` bucket, under the `notebooks`
   folder.
7. Open the notebooks in order - and follow the instructions. **Note:** the first notebook (part 0) is optional.

## Running the real-time analysis

We also deployed a web app to the Cloud run service, to showcase near realtime processing and alerting. From the
terraform outputs, note the "cloud_run_url" output variable. Open the link in a browser, and follow the on-screen
instructions. **Note** that this part has a dependency on running at least the notebooks part 1 and part 2.

## Project structure

In this repo, you will find the following folders:

- `assets`: Code and notebooks assets
    - `code`: Contains the pyspark job file and supporting files
        - `ivySettings.xml`: definition of maven repos, so the spark job can pull some external dependencies, namely the
          `confluant-kafka` dependency. In a production environment, consider pre-bundling your dependencies, so
          external dependencies are NOT pulled at runtime.
        - `pyspark-job.py`: The pyspark streaming job, listening to kafka events, and outputting buses state to BQ, as
          well as alert messages to another kafka topic.
        - `run-pyspark.sh`: shell script for testing to run the job.
    - `notebooks`: Contains all notebooks that are used in this demo, and are meant to be run in colab enterprise or
      BigQuery studio. There is blocker to run them locally, just make sure some plugins are pre-configured on those
      environments (e.g. bash operator `!`, sql magic `%sql` etc.). We are
      using [jupytext](https://jupytext.readthedocs.io/en/latest/) package for development,
      so for
      each `.ipynb` file, there is a matching `.py` file, to make git tracking and comparison easier.
- `webapp`: The webapp (flask) to display real-time data. The app has 2 important background workers that are activated
  when the corresponding buttons are pressed in the web interface.
    - `kafka_service`: When started, this service will query BigQuery `bus_rides` table, for 5 days worth of data from "
      today in 2024", change the results to look like new messages from now, and push to kafka `bus-updates` topic.
    - `pyspark_service`: When started, this service will start the pyspark streaming job, and monitor its status.

## Development

In the `assets` and the `webapp` folders, use `uv` to install the virtual environments.

For the `assets` environment, pay attention to the dev dependencies, and install the pre-hooks before commiting:

```bash
cd assets 
uv sync
uv run pre-commit install
```

The jupytext and black pre-commit hooks will now be triggered whenever you commit a change to the notebooks, and convert
those changes to the paired `.py` files.
This will make code reviews a much more friendly experience.

When working the notebooks, pay attention that the notebooks outputs have to be cleared manually. The jupytext
pre-commit hook clashes with other potential hooks that would clear the outputs, as they change the notebooks, which
causes a loop between hooks.
