#!/bin/bash
set -e # Exit immediately if a command exits with a non-zero status.

# Check if gcloud exists as a command in the path.
if ! command -v gcloud &> /dev/null
then
    echo "gcloud could not be found. Please install Google Cloud SDK and ensure gcloud is in your PATH."
    exit 1
fi
# Allow environment variables to be overridden by arguments
while [[ "$#" -gt 0 ]]; do
    case $1 in
        --catalog-name) CATALOG_NAME="$2"; shift ;;
        --project-id) PROJECT_ID="$2"; shift ;;
        *) echo "Unknown parameter passed: $1"; exit 1 ;;
    esac
    shift
done

# Check if catalog exists
if gcloud alpha biglake iceberg catalogs describe $CATALOG_NAME --project $PROJECT_ID &> /dev/null; then
    echo "Catalog $CATALOG_NAME already exists. Skipping creation."
else
    gcloud alpha biglake iceberg catalogs create $CATALOG_NAME --catalog-type gcs-bucket --credential-mode end-user --project $PROJECT_ID
fi