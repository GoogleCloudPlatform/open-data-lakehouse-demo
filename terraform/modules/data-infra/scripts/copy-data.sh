#!/bin/bash
set -e # Exit immediately if a command exits with a non-zero status.

# Check if gsutil exists as a command in the path.
if ! command -v gsutil &> /dev/null
then
    echo "gsutil could not be found. Please install Google Cloud SDK and ensure gsutil is in your PATH."
    exit 1
fi

echo "Syncing gs://data-lakehouse-demo-data-assets/ to gs://$BUCKET_NAME/"
gsutil -m rsync -r gs://data-lakehouse-demo-data-assets/  gs://$BUCKET_NAME/