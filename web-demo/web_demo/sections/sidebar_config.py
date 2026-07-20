import streamlit as st
from dataclasses import asdict
from web_demo.utils.config import load_config, AppConfig, save_config

config = load_config()
dict_config = asdict(config)
def sidebar_config():
    # st.json(dict_config)

    with st.sidebar.expander("⚙️ Configuration", expanded=False):
        for key, value in dict_config.items():
            dict_config[key] = st.text_input(key, value)
        verify_and_save_button = st.button("Save Config")
        if verify_and_save_button:
            new_config = AppConfig(**dict_config)
            save_config(new_config)
