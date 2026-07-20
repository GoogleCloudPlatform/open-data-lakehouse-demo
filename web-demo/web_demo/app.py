import os

import streamlit as st

from sections.sidebar_config import sidebar_config
from sections.sidebar_top import sidebar_top
from utils.config import load_config
from utils.steps import Step

# Disable mTLS client certificate provider to avoid status code -11 error
os.environ["GOOGLE_API_USE_CLIENT_CERTIFICATE"] = "false"

# Set page configuration for a premium analytics experience
st.set_page_config(
    page_title="Base example for Google Cloud Demo",
    page_icon="🔬",
    layout="wide",
    initial_sidebar_state="expanded",

)
st.markdown("""
    <style>
        
        .stAppDeployButton {display:none;}
        
    </style>
""", unsafe_allow_html=True)
config = load_config()

# Sidebar
sidebar_top()
sidebar_config()
pages = {
    "Main": [
        st.Page("pages/main.py", title="Main"),
    ],
    "Part 0": [
        st.Page("pages/part_0/intro.py", title="Data Preparation Intro", url_path="part_0_intro"),
        st.Page("pages/part_0/step_1.py", title="Data Prep - step 1", url_path="part_0_step_1"),
        st.Page("pages/part_0/step_2.py", title="Data Prep - step 2", url_path="part_0_step_2"),
    ]
}
nav = st.navigation(pages)
nav.run()

