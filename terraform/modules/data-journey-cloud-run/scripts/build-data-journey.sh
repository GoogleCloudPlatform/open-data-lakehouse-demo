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
        --data-journey-dir) DATA_JOURNEY_DIR="$2"; shift ;;
        *) echo "Unknown parameter passed: $1"; exit 1 ;;
    esac
    shift
done

# Check if required variables are set (either from env or args)
if [ -z "$TAG" ] || [ -z "$PROJECT_ID" ] || [ -z "$REGION" ] || [ -z "$SERVICE_ACCOUNT" ] || [ -z "$DATA_JOURNEY_DIR" ]; then
    echo "Error: One or more required variables (TAG, PROJECT_ID, REGION, SERVICE_ACCOUNT, DATA_JOURNEY_DIR) are not set. Use env vars or flags (--tag, --project-id, --region, --service-account, --data-journey-dir)."
    exit 1
fi

echo "DATA_JOURNEY_DIR=${DATA_JOURNEY_DIR}"
echo "Building webapp image... (TAG=${TAG}; PROJECT_ID=${PROJECT_ID}; REGION=${REGION}; SERVICE_ACCOUNT=${SERVICE_ACCOUNT})"

# Set the project
gcloud config set project ${PROJECT_ID}

gcloud builds submit ${DATA_JOURNEY_DIR} \
      --tag ${TAG} \
      --project ${PROJECT_ID} \
      --region ${REGION} \
      --billing-project ${PROJECT_ID} \
      --default-buckets-behavior regional-user-owned-bucket \
      --service-account  "projects/${PROJECT_ID}/serviceAccounts/${SERVICE_ACCOUNT}"
