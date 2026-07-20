import streamlit as st
from pathlib import Path

path = Path(__file__).parent / "main.md"
with open(path) as f:
    md = f.read()

st.markdown(md)