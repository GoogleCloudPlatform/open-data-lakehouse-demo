#!/bin/bash
set -e # Exit immediately if a command exits with a non-zero status.

# Check if gcloud exists as a command in the path.
if ! command -v gcloud &> /dev/null
then
    echo "gcloud could not be found. Please install Google Cloud SDK and ensure gcloud is in your PATH."
    exit 1
fi

echo "Syncing gs://data-lakehouse-demo-data-assets/ to gs://$BUCKET_NAME/"
gcloud storage rsync --recursive gs://data-lakehouse-demo-data-assets/ gs://$BUCKET_NAME/