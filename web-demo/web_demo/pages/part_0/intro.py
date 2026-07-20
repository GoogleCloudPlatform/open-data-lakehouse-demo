import streamlit as st

st.markdown("""
# Ridership Open Lakehouse Demo (Part 0): Generating our datasets

This notebook will demonstrate a strategy to implement an open lakehouse on GCP, using Apache Iceberg,
as an open source standard for managing data, while still leveraging GCP native capabilities. This demo will use
BigQuery Manged Iceberg Tables, Managed Apache Kafka and Apache Kafka Connect to ingest streaming data, Vertex AI for Generative AI queries on top of the data and Dataplex to govern tables.

This notebook will generate fake data and anonymized real-world data.

the real-world data used in this notebook is from [MTA daily ridership data](https://data.ny.gov/Transportation/MTA-Daily-Ridership-Data-2020-2025/vxuj-8kew/data_preview).

Rest of the data is being randomly generated inside the notebook.

## MTA Data - PLEASE READ!

This part is tricky - this is the raw data from the MTA subways of new york.
The MTA website and API are slow... very slow. Checking the [hourly ridership data](https://data.ny.gov/Transportation/MTA-Subway-Hourly-Ridership-2020-2024/wujg-7c2s/about_data), we have about 110 million records to get.

If you created the demo, using our terraform scripts, you should be able to access the MTA raw CSV under the bucket `<YOUR_PROJECT_ID>-ridership-lakehouse`, as the terraform script copies over the CSV from a publicly available bucket `gs://data-lakehouse-demo-data-assets/mta-raw/`.

If you haven't created this demo using our terraform scripts, the easiest way to get a hold of the data would be to run the following `gsutil` command:

```bash
gcloud storage rsync --recursive gs://data-lakehouse-demo-data-assets/  gs://<YOUR_BUCKET_NAME>/
```

Downloading the CSV manually is the more efficient option, but still very slow, so you would have to send the request, and keep your machine and browser awake for a few hours (yes, hours, was about 2 hours in my case) before the CSV starts downloading.

for programmatic download using the API, the situation might be worse. the API is prone to timeouts. The default records limit per request is 1,000, and the maximum is 50,000, which means we have to do chunking of API calls, but the latency is increasing expo. when increasing the limit.

I've written the function to download the data and write each request to be appended to a file, but this ran for 4 hours, and got around 8% of the data, before I gave up.

The next cell has the function to download the data using the API, but the call to the function is commented out, since it is very slow to run.

the cell after that allows you to fill in the path to GCS, so, whichever method you want to get the data, just make sure, that the variable `MTA_RAW_CSV` points to a valid and accessible path on GCS that holds the MTA hourly ridership data.

happy thoughts!
""")