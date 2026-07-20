import datetime
from importlib import metadata
from io import StringIO
from typing import Any, Callable, Optional
from typing import Iterable

import google.cloud.storage
import streamlit as st
from google.cloud.storage import Blob
from pandas import DataFrame, read_csv
from streamlit_arborist import tree_view

from web_demo.utils.clients import get_storage_client
from web_demo.utils.general import bytes_to_human_readable

client = get_storage_client()

def preview_pdf(blob: Blob) -> None:
    data = blob.download_as_bytes()
    import pdf2image
    pages = pdf2image.convert_from_bytes(data)
    for page in pages:
        st.image(page)

def preview_csv(blob: Blob) -> None:
    data = blob.download_as_bytes().decode("utf-8")
    df = read_csv(StringIO(data))
    st.dataframe(df)

RENDERER: dict[str, Callable[[Blob], Optional[Any]]] = {
    "text": lambda blob: st.code(blob.download_as_text()),
    "json": lambda blob: st.json(blob.download_as_text()),
    "csv": preview_csv,
    "image": lambda blob: st.image(blob.download_as_bytes()),
    "audio": lambda blob: st.audio(blob.download_as_bytes()),
    "video": lambda blob: st.video(blob.download_as_bytes()),
    "pdf": preview_pdf,
}

def build_path_tree(paths: list[str]) -> list[dict]:
    root_nodes = []
    # Hash map to provide O(1) lookup for existing nodes by their full path
    nodes = {}

    for path in paths:
        # Remove any leading/trailing slashes and split into components
        parts = path.strip("/").split("/")
        current_path = ""

        for i, part in enumerate(parts):
            is_last = (i == len(parts) - 1)
            next_path = f"{current_path}/{part}" if current_path else part

            if next_path not in nodes:
                # 1. Create the new node
                node = {"id": next_path, "name": part}

                # If it's not the last component, it acts as a directory
                if not is_last:
                    node["children"] = []

                # 2. Attach it to its parent or to the root list
                if current_path:
                    # Ensure parent has a 'children' list (handles out-of-order paths)
                    if "children" not in nodes[current_path]:
                        nodes[current_path]["children"] = []
                    nodes[current_path]["children"].append(node)
                else:
                    root_nodes.append(node)

                # 3. Register in our O(1) lookup map
                nodes[next_path] = node
            else:
                # If the node already exists but we are now traversing through it
                # to a deeper child, ensure it has a 'children' list initialized
                if not is_last and "children" not in nodes[next_path]:
                    nodes[next_path]["children"] = []

            current_path = next_path

    return root_nodes

@st.cache_data(ttl=datetime.timedelta(minutes=5))
def _list_bucket(selected_bucket: str, path=""):
    path_data = []
    blobs = client.list_blobs(
        selected_bucket,
        prefix=path,
    ) # type: Iterable[google.cloud.storage.Blob]
    blobs_names = [b.name for b in blobs]
    return build_path_tree(blobs_names)


def get_type_of_preview(blob):
    content_type = blob.content_type.split(";")[0]
    if content_type:
        match content_type:
            case "application/json": return "json"
            case "text/plain": return "text"
            case "text/csv": return "csv"
            case "image/jpeg": return "image"
            case "image/jpg": return "image"
            case "image/png": return "image"
            case "audio/mpeg": return "audio"
            case "video/mp4": return "video"
            case "application/pdf": return "pdf"
    suffix = blob.name.split(".")[-1]
    if suffix:
        match suffix:
            case "json": return "json"
            case "txt": return "text"
            case "csv": return "csv"
            case "jpg": return "image"
            case "jpeg": return "image"
            case "png": return "image"
            case "mp3": return "audio"
            case "mp4": return "video"
            case "pdf": return "pdf"
            case _: return None
    return None


def gcs_tree_view():
    buckets = [b.name for b in client.list_buckets()]
    selected_bucket = st.selectbox("Select a bucket", options=buckets)
    if selected_bucket:
        with st.spinner("Fetching bucket"):
            bucket = client.get_bucket(selected_bucket)
            data = _list_bucket(selected_bucket)
        st.write(f"Bucket {bucket.name} in location {bucket.location}")
        selected_blob = tree_view(
            data,
            icons={'open': '📂', 'closed': '📁', 'leaf': '📄'},
            open_by_default=False,
            selection=None,
            select_internal_nodes=False,
            search_term='',
            height=300,
        )
        if selected_blob:
            with st.spinner("Fetching blob"):
                blob = bucket.get_blob(selected_blob["id"])
            if not blob.exists():
                st.write(f"Blob {blob.name} does not exist")
            else:
                st.subheader(blob.name.split("/")[-1])
                blob_metadata = {
                    "Size": [bytes_to_human_readable(blob.size)],
                    "Content Type": [blob.content_type or "N/A"],
                }
                st.table(blob_metadata)
                type_of_file = get_type_of_preview(blob)
                if blob.size > 50_000_000: # roughly 50MB
                    st.write("Blob size is too large to preview")
                elif type_of_file and type_of_file in RENDERER:
                    renderer = RENDERER.get(type_of_file)
                    try_to_preview = st.button("Try to preview", type="primary")
                    if try_to_preview:
                        with st.spinner("Previewing blob"):
                            with st.container(border=True, height=500):
                                renderer(blob)
                else:
                    st.write("Unable to preview file type")




