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
        --tag) TAG="$2"; shift ;;
        --project-id) PROJECT_ID="$2"; shift ;;
        --region) REGION="$2"; shift ;;
        --service-account) SERVICE_ACCOUNT="$2"; shift ;;
        --webapp-dir) WEBAPP_DIR="$2"; shift ;;
        *) echo "Unknown parameter passed: $1"; exit 1 ;;
    esac
    shift
done

# Check if required variables are set (either from env or args)
if [ -z "$TAG" ] || [ -z "$PROJECT_ID" ] || [ -z "$REGION" ] || [ -z "$SERVICE_ACCOUNT" ] || [ -z "$WEBAPP_DIR" ]; then
    echo "Error: One or more required variables (TAG, PROJECT_ID, REGION, SERVICE_ACCOUNT, WEBAPP_DIR) are not set. Use env vars or flags (--tag, --project-id, --region, --service-account, --webapp-dir)."
    exit 1
fi

echo "WEBAPP_DIR=${WEBAPP_DIR}"
echo "Building buses dashboard image... (TAG=${TAG}; PROJECT_ID=${PROJECT_ID}; REGION=${REGION}; SERVICE_ACCOUNT=${SERVICE_ACCOUNT})"
gcloud builds submit ${WEBAPP_DIR} \
      --tag ${TAG} \
      --project ${PROJECT_ID} \
      --region ${REGION} \
      --default-buckets-behavior regional-user-owned-bucket \
      --service-account  "projects/${PROJECT_ID}/serviceAccounts/${SERVICE_ACCOUNT}"
