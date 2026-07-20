from typing import Callable, Optional

import streamlit as st
from web_demo.utils.general import get_source_code


def display_and_run_function(function: Callable, title: Optional[str] = None, expanded=True):
    code = get_source_code(function)
    _title = (title + " Code") if title else "Code"
    _run_title = ("Run " + title) if title else "Run Code"
    with st.expander(_title, expanded=expanded):
        st.code(code, language="python")

    run_code = st.button(_run_title)
    if run_code:
        function()