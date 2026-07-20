import time
from pathlib import Path

import streamlit as st

from web_demo.components.code import display_and_run_function
from web_demo.utils.clients import get_storage_client
from web_demo.utils.config import load_config
from web_demo.utils.general import bytes_to_human_readable

config = load_config()
st.markdown("""
If you want to download the MTA data on your own, use the following code:
""")


def programmatically_download_mta_data():
    # This function will use the api to download the data into a csv locally
    # Note that the final size of the CSV would be around 17GB
    # also note that the API is slow and prone to timeout errors
    # this is why it has a back-off mechanism where we start with the
    # maximum records allowed and back off whenever we have a timeout error
    # as mentioned above, a much easier approach would be to go to the website
    # and download the CSV manually (although that takes some time as well)
    # then upload the CSV to GCS
    # This method is here for convenience and is not currently being called.

    import requests
    from csv import DictWriter
    from sodapy import Socrata

    FILENAME = "mta_data.csv"

    fieldnames = [
        "transit_timestamp",
        "transit_mode",
        "station_complex_id",
        "station_complex",
        "borough",
        "payment_method",
        "fare_class_category",
        "ridership",
        "transfers",
        "latitude",
        "longitude",
        "georeference",
        ":@computed_region_kjdx_g34t",
        ":@computed_region_yamh_8v7k",
        ":@computed_region_wbg7_3whc",
    ]

    # this method was planned to be able to resume from where it last stopped
    # we have the option to read an existing CSV that we started downloading
    # if you want to ignore the existing file, you can delete it manually or just flip this flag
    FORCE_CLEAR_DATA = False

    # I got this number after downloading the full file and looking at it.
    # Since this should be a static dataset, the number shouldn't change over time
    TOTAL_NUMBER_OF_RECORDS = 110_696_370
    STEP = 50_000


    client = Socrata("data.ny.gov", None)

    # is there is current file already exists
    existing_file = os.path.exists(FILENAME)

    if not existing_file or FORCE_CLEAR_DATA:
        # if we no current file exists, or flag to ignore it is raised
        rows_got = 0
        with open(FILENAME, "w") as mta_fp:
            mta_writer = DictWriter(mta_fp, fieldnames=fieldnames)
            # write headers
            mta_writer.writeheader()
    else:
        with open(FILENAME, "r") as f:
            # read how many records we have already (minus 1 for headers)
            rows_got = sum((1 for _ in f)) - 1
        print(
            f"""Starting from existing data. already got {rows_got:,}
        records ({round(rows_got / TOTAL_NUMBER_OF_RECORDS * 100, 2)}%)"""
        )
    current_step = STEP
    # while the number of rows we got is smaller than the total number of rows expected
    while rows_got < TOTAL_NUMBER_OF_RECORDS:
        try:
            # get more data
            results = client.get("wujg-7c2s", limit=current_step, offset=rows_got)
        except requests.exceptions.ReadTimeout:
            # in case of a timeout, ask for less data
            current_step = current_step - 1000
            print(f"Got timeout, adjusting limit to {current_step}")
        else:
            # when we get data, append it to the file.
            with open(FILENAME, "a") as mta_fp:
                mta_writer = DictWriter(mta_fp, fieldnames=fieldnames)
                mta_writer.writerows(results)
            rows_got = rows_got + len(results)
            print(
                f"Got {rows_got:,} rows so far ({round(rows_got / TOTAL_NUMBER_OF_RECORDS * 100, 2)}%)"
            )

    # upload the local CSV to GCS
    storage_client = get_storage_client()
    bucket = storage_client.bucket(config.GENERAL_BUCKET_NAME)
    blob = bucket.blob(config.raw_mta_csv_path_in_gcs)
    blob.upload_from_filename(config.raw_mta_csv_path_in_gcs)


display_and_run_function(programmatically_download_mta_data, title="programmatically_download_mta_data",
                         expanded=False)

storage_client = get_storage_client()

def verify_we_have_mta_raw():
    st.toast("Verifying we have the raw MTA data", icon="⏳")
    bucket = storage_client.bucket(config.gcs_bucket_name)
    _mta_raw_csv_blob = bucket.get_blob(config.raw_mta_csv_path_in_gcs)
    if _mta_raw_csv_blob.exists():
        st.toast("Verified we have the raw MTA data", icon="✅")
        return True
    st.toast("Failed to verify we have the raw MTA data", icon="❌")
    return False
st.markdown(f"__Make sure to validate using the button below that the CSV is present in the GCS bucket "
            f"{config.gcs_bucket_name} under the path f{config.raw_mta_csv_path_in_gcs} before continuing__")

st.button("Verify we have the raw MTA data", on_click=verify_we_have_mta_raw)

st.page_link(Path("pages/part_0/step_2.py"), label="Next")
