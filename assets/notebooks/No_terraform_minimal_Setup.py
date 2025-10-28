# %% [markdown]
# # Setup notebook - read before you run!!
# This notebooks is a short-hand replacement for the `terraform` scripts in the repo. Running this ntoebook will setup the bare minimum resources on your project, in order to run notebooks 0-3.
#
# If you already ran or are running the `terrafom` scripts - **DO NOT RUN THIS NOTEBOOOK!!!**
#
# The runtime for this notebook requires a VPC already been created with at least 1 subnet and enabled for `Private Google Access` as well as access to the internet.
#
# After creating the runtime, just run all cells in this notebook.

# %%
# PROJECT_ID = !gcloud config get-value project
PROJECT_ID = PROJECT_ID[0]
LOCATION = "us-central1"  # @param {"type":"string","placeholder":"GCP Region"}


BQ_DATASET = "ridership_lakehouse"
BQ_CONNECTION_NAME = "cloud-resources-connection"

GENERAL_BUCKET_NAME = f"{PROJECT_ID}-ridership-lakehouse"
BQ_CATALOG_BUCKET_NAME = f"{PROJECT_ID}-iceberg-bq-catalog"
REST_CATALOG_BUCKET_NAME = f"{PROJECT_ID}-iceberg-rest-catalog"

BQ_CATALOG_PREFIX = "bq_namespace"
REST_CATALOG_PREFIX = "rest_namespace"


assert PROJECT_ID is not None and len(PROJECT_ID) > 0

regions = [
    "us-central1",
    "europe-west1",
    "us-west1",
    "asia-east1",
    "us-east1",
    "asia-northeast1",
    "asia-southeast1",
    "us-east4",
    "australia-southeast1",
    "europe-west2",
    "europe-west3",
    "southamerica-east1",
    "asia-south1",
    "northamerica-northeast1",
    "europe-west4",
    "europe-north1",
    "us-west2",
    "asia-east2",
    "europe-west6",
    "asia-northeast2",
    "asia-northeast3",
    "us-west3",
    "us-west4",
    "asia-southeast2",
    "europe-central2",
    "northamerica-northeast2",
    "asia-south2",
    "australia-southeast2",
    "southamerica-west1",
    "europe-west8",
    "europe-west9",
    "us-east5",
    "europe-southwest1",
    "us-south1",
    "me-west1",
    "europe-west12",
    "me-central1",
    "europe-west10",
    "africa-south1",
    "northamerica-south1",
    "europe-north2",
]
assert LOCATION in regions

# %%
from google.cloud import bigquery, storage

bigquery_client = bigquery.Client()
storage_client = storage.Client()


# %%

for b in [GENERAL_BUCKET_NAME, BQ_CATALOG_BUCKET_NAME, REST_CATALOG_BUCKET_NAME]:
    bucket = storage_client.bucket(b)
    if not bucket.exists():
        bucket = storage_client.create_bucket(b, location=LOCATION)

# %%
dataset_ref = bigquery.Dataset(f"{PROJECT_ID}.{BQ_DATASET}")
dataset_ref.location = LOCATION
dataset = bigquery_client.create_dataset(dataset_ref, exists_ok=True)


# %%
# !gsutil -m rsync -r gs://data-lakehouse-demo-data-assets/  gs://{GENERAL_BUCKET_NAME}/

# %%
# !bq mk \
# --connection \
# --location={LOCATION} \
# --project_id={PROJECT_ID} \
# --connection_type=CLOUD_RESOURCE \
# {BQ_CONNECTION_NAME}

# %%
import json
import time

# connection_details_json_str = !bq show --format json --connection {PROJECT_ID}.{LOCATION}.{BQ_CONNECTION_NAME}
connection_details_dict = json.loads(connection_details_json_str[0])
CONNECTION_SA_ID = connection_details_dict["cloudResource"]["serviceAccountId"]
if not CONNECTION_SA_ID:
    # it's possible that this command failed, when ran immediately after the previous command
    # this is due to the time it takes the API to be consistent due to async actions on GCP
    # we will wait 10 seconds, and try again
    # if this still fails, we'll throw an exception
    time.sleep(10)
    # connection_details_json_str = !bq show --format json --connection {PROJECT_ID}.{REGION}.multimodal
    connection_details_dict = json.loads(connection_details_json_str[0])
    CONNECTION_SA_ID = connection_details_dict["cloudResource"]["serviceAccountId"]
if not CONNECTION_SA_ID:
    raise ValueError("No Service Account detected for BQ Connection")

# Sleeping (again) - for the same reason, before the next cell uses this variable.
time.sleep(10)
CONNECTION_SA_ID


# %%
# !gcloud projects add-iam-policy-binding {PROJECT_ID} \
#   --member='serviceAccount:{CONNECTION_SA_ID}' \
#   --role='roles/aiplatform.user' --condition=None \
#   --no-user-output-enabled


# %%
# !gcloud storage buckets add-iam-policy-binding 'gs://{GENERAL_BUCKET_NAME}' \
#     --member='serviceAccount:{CONNECTION_SA_ID}' \
#     --role=roles/storage.objectUser --condition=None \
#     --no-user-output-enabled

# !gcloud storage buckets add-iam-policy-binding 'gs://{BQ_CATALOG_BUCKET_NAME}' \
#     --member='serviceAccount:{CONNECTION_SA_ID}' \
#     --role=roles/storage.objectUser --condition=None \
#     --no-user-output-enabled

# !gcloud storage buckets add-iam-policy-binding 'gs://{REST_CATALOG_BUCKET_NAME}' \
#     --member='serviceAccount:{CONNECTION_SA_ID}' \
#     --role=roles/storage.objectUser --condition=None \
#     --no-user-output-enabled


# %%
# !gcloud services enable dataproc.googleapis.com biglake.googleapis.com --project=$PROJECT_ID

# %%
# PROJECT_NUMBER = !gcloud projects describe $PROJECT_ID --format="value(projectNumber)"
PROJECT_NUMBER = PROJECT_NUMBER[0]
PROJECT_NUMBER

# %%
DEFAULT_COMPUTE_SA = f"{PROJECT_NUMBER}-compute@developer.gserviceaccount.com"
DEFAULT_COMPUTE_SA

# %%
# !gcloud projects add-iam-policy-binding {PROJECT_ID} \
#   --member='serviceAccount:{DEFAULT_COMPUTE_SA}' \
#   --role='roles/dataproc.worker' --condition=None \
#   --no-user-output-enabled

# %%
# cidr_range = !gcloud compute networks subnets list --filter="region={LOCATION}" --format="value(ipCidrRange)"
cidr_range = cidr_range[0]
cidr_range

# %%
# !gcloud compute firewall-rules create allow-internal-ingress --network lakehouse-demo-vpc --allow all --source-ranges={cidr_range}
