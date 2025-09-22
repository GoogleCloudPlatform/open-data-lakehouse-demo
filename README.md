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
- **Visualization/BI**: Tools for reporting and dashboarding.

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
6. Load all 4 Jupyter notebooks from the `<YOUR_PROJECT>-lakehouse-ridership` bucket, under the `notebooks_and_code`
   folder.
7. Open the notebooks in order - and follow the instructions. **Note:** the first notebook (part 0) is optional.

## Running the real-time analysis

We also deployed a web app to the Cloud run service, to showcase near realtime processing and alerting. From the
terraform outputs, note the "cloud_run_url" output variable. Open the link in a browser, and follow the on screen
instructions. **Note** that this part has a dependency on running at least the noteboks part 1 and part 2.
