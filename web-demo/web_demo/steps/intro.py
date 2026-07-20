from pathlib import Path

import streamlit as st

from web_demo.utils.clients import get_storage_client, get_bq_client
from web_demo.utils.general import get_source_code

# Set page configuration for a premium analytics experience
st.set_page_config(
    page_title="Intro",
    page_icon="🔬",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.header("Introduction")
st.write("This is the introduction step. We show interactive code execution in this walkthough.")
st.write("We use streamlit (as st) so in code samples, you will see references to st.")
st.write("We also have some utility functions that we will use in the walkthrough. They are documented below.")

steps_path = Path(__file__).parent
base_path = steps_path.parent
utils_path = base_path / "utils"
for file in utils_path.glob("*.py"):
    with open(file, "r") as f:
        content = f.read()
        if content:
            with st.expander(str(file.relative_to(base_path)), False):
                st.code(content)

st.write("Do pay attention to these 2 functions:")
st.code(get_source_code(get_storage_client))
st.code(get_source_code(get_bq_client))
st.write("These functions that return the native google cloud client are tied to the configuration in the sidebar, "
         "and cached for reuse.")
st.write("Each step, will show some code, and a button to execute it.")
st.write("Before starting, check the configuration on the left sidebar, verify and save, so we will use the right configuration.")
st.write("You can also click on the step name in the sidebar to jump to that step.")