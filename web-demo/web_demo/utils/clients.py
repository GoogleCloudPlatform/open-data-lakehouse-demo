import streamlit as st
from web_demo.utils.config import load_config
import google.cloud.bigquery as bigquery
from google.cloud import storage

config = load_config()

@st.cache_resource
def get_bq_client() -> bigquery.Client:
    return bigquery.Client(project=config.project_id)

@st.cache_resource
def get_storage_client() -> storage.Client:
    return storage.Client(project=config.project_id)

