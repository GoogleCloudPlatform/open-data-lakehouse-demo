import marimo

__generated_with = "0.22.0"
app = marimo.App(width="medium")

with app.setup:
    # Copyright 2025 Google LLC
    #
    # Licensed under the Apache License, Version 2.0 (the "License");
    # you may not use this file except in compliance with the License.
    # You may obtain a copy of the License at
    #
    #     https://www.apache.org/licenses/LICENSE-2.0
    #
    # Unless required by applicable law or agreed to in writing, software
    # distributed under the License is distributed on an "AS IS" BASIS,
    # WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    # See the License for the specific language governing permissions and
    # limitations under the License.
    pass


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # Helper notebook

    This notebook contains helper functions and global variables that are read from environment variables. Those environment variables should be created by the terraform setup, and contain information about the rest of the setup (BQ Datasets, connections, service accounts etc.).

    There are `LakehouseBigQueryClient` and `LakehouseStorageClient` that inherit from the `bigquery.Client` and `storage.Client` in respect, and extend them with some helper methods to simplify usage down the line.
    """)
    return


@app.cell
def _():
    import marimo as mo
    import subprocess
    import os
    import time
    import sys
    from contextlib import contextmanager
    from typing import Optional, List, Union, Any
    import pandas as pd
    from google.api_core.client_info import ClientInfo
    from google.cloud import bigquery, storage

    USER_AGENT = "cloud-solutions/data-to-ai-nb-v3"

    # From terraform or .env file
    # The project_id and location are required. All others have default based on conventions
    PROJECT_ID = os.environ["PROJECT_ID"]
    LOCATION = os.environ["LOCATION"]
    GENERAL_BUCKET_NAME = os.environ.get("GENERAL_BUCKET_NAME", f"{PROJECT_ID}-ridership-lakehouse")
    STAGING_BQ_DATASET = os.environ.get("STAGING_BQ_DATASET", "ridership_lakehouse_staging")
    MAIN_BQ_DATASET = os.environ.get("MAIN_BQ_DATASET", "ridership_lakehouse")
    FULL_BQ_CONNECTION_NAME = os.environ.get("BQ_CONNECTION_NAME", "cloud-resources-connection")
    BQ_CATALOG_BUCKET_NAME = os.environ.get("BQ_CATALOG_BUCKET_NAME", f"{PROJECT_ID}-iceberg-bq-catalog")
    REST_CATALOG_BUCKET_NAME = os.environ.get("REST_CATALOG_BUCKET_NAME", f"{PROJECT_ID}-iceberg-rest-catalog")
    BQ_CONNECTION_NAME = FULL_BQ_CONNECTION_NAME.split("/")[-1]
    SUBNETWORK_ID = os.environ.get("SUBNETWORK_ID", f"projects/{PROJECT_ID}/regions/{LOCATION}/subnetworks/{LOCATION}-open-lakehouse-subnet")
    SPARK_SERVICE_ACCOUNT = os.environ.get(
        "SPARK_SERVICE_ACCOUNT",
        f"backend-service-account@{PROJECT_ID}.iam.gserviceaccount.com"
    )


    BQ_CATALOG_PREFIX = "bq_namespace"
    REST_CATALOG_PREFIX = "rest_namespace"
    return (
        ClientInfo,
        LOCATION,
        PROJECT_ID,
        USER_AGENT,
        bigquery,
        contextmanager,
        mo,
        pd,
        storage,
        subprocess,
        sys,
        time,
    )


@app.cell
def _lakehousebigqueryclient(
    ClientInfo,
    LOCATION,
    PROJECT_ID,
    USER_AGENT,
    bigquery,
):
    class LakehouseBigQueryClient(bigquery.Client):
        def __init__(self):
            super().__init__(
                project=PROJECT_ID,
                location=LOCATION,
                client_info=ClientInfo(user_agent=USER_AGENT),
            )

        def select_top_rows(self, dataset_name: str, table_name: str, num_rows: int = 10):
            query = f"""
            SELECT *
            FROM `{self.project}.{dataset_name}.{table_name}`
            LIMIT {num_rows}
            """
            return self.query(query).to_dataframe()

    return


@app.cell
def _(ClientInfo, PROJECT_ID, USER_AGENT, pd, storage):
    class LakehouseStorageClient(storage.Client):
        def __init__(self):
            super().__init__(
                project=PROJECT_ID, 
                client_info=ClientInfo(user_agent=USER_AGENT),
            )

        def display_blobs_with_prefix(self, bucket_name: str, prefix: str, top=20):
            blobs = [
                [b.name, b.size, b.content_type, b.updated]
                for b in self.list_blobs(
                    bucket_name,
                    prefix=prefix,
                )
            ]
            df = pd.DataFrame(blobs, columns=["Name", "Size", "Content Type", "Updated"])
            return df.head(top)


        def delete_blobs_with_prefix(self, bucket_name: str, prefix: str):
            blobs = self.list_blobs(bucket_name, prefix=prefix)
            for blob in blobs:
                blob.delete()

    return


@app.cell
def _(subprocess, sys):
    def sh(command: str, stream: bool = False) -> str:
        """
        Executes a shell command, similar to `!command` in Jupyter.

        Args:
            command: The shell command to execute.
            stream: If True, prints output to stdout as it becomes available.
                    If False, captures and returns it (useful for assigning to variables).

        Returns:
            The stdout output as a string.
        """
        if stream:
            # Run and stream output directly to stdout (visible in Marimo cell output)
            process = subprocess.Popen(
                command,
                shell=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            output = []
            if process.stdout:
                for line in process.stdout:
                    print(line, end="")
                    output.append(line)
            if process.stderr:
                for line in process.stderr:
                    print(line, end="", file=sys.stderr)

            process.wait()
            return "".join(output)
        else:
            # Capture mode (run quietly)
            result = subprocess.run(
                command, shell=True, capture_output=True, text=True
            )
            if result.stderr:
                print(result.stderr, file=sys.stderr)
            return result.stdout.splitlines()

    return


@app.cell
def _(contextmanager, time):
    @contextmanager
    def timer(label: str = "Execution"):
        """
        Context manager to time a block of code, similar to `%%time`.

        Usage:
            with timer("Data Processing"):
                heavy_computation()
        """
        start = time.perf_counter()
        try:
            yield
        finally:
            end = time.perf_counter()
            elapsed = end - start
            if elapsed < 1:
                print(f"⏱️  {label}: {elapsed * 1000:.2f} ms")
            else:
                print(f"⏱️  {label}: {elapsed:.4f} s")

    return


if __name__ == "__main__":
    app.run()
