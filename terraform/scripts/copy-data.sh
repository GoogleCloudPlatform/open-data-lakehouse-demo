#!/bin/bash
set -e # Exit immediately if a command exits with a non-zero status.

# Check if gcloud exists as a command in the path.
if ! command -v gcloud &> /dev/null
then
    echo "gcloud could not be found. Please install Google Cloud SDK and ensure gcloud is in your PATH."
    exit 1
fi

echo "Copying gs://data-lakehouse-demo-data-assets/ to gs://$BUCKET_NAME/"
# Use `cp --recursive --no-clobber` instead of `rsync` because the public source bucket
# grants object-level read/list (roles/storage.objectViewer) without storage.buckets.get.
gcloud storage cp --recursive --no-clobber "gs://data-lakehouse-demo-data-assets/*" "gs://$BUCKET_NAME/"