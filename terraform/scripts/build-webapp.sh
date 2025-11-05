#!/bin/bash
set -e # Exit immediately if a command exits with a non-zero status.

# Check if gcloud exists as a command in the path.
if ! command -v gcloud &> /dev/null
then
    echo "gcloud could not be found. Please install Google Cloud SDK and ensure gcloud is in your PATH."
    exit 1
fi

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
TF_DIR=$(dirname "$SCRIPT_DIR")
ROOT_DIR=$(dirname "$TF_DIR")
WEBAPP_DIR="${ROOT_DIR}/webapp"

gcloud builds submit ${WEBAPP_DIR} \
      --tag ${TAG} \
      --project ${PROJECT_ID} \
      --region ${REGION} \
      --default-buckets-behavior regional-user-owned-bucket \
      --service-account "${SERVICE_ACCOUNT}"
