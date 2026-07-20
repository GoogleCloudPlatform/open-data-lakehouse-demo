from typing import Optional

import streamlit as st

from web_demo.utils.clients import get_bq_client


def run_bq(
        query: str,
        query_name: Optional[str] = None,
        cache_key: Optional[str] = None,
        use_expander=True,
        default_show: bool = True,
        completely_hide: bool = False
):
    # Print query
    _query_name = query_name or "Query"
    if not completely_hide:
        st.write(_query_name)
        if not use_expander:
            st.code(query)
        else:
            with st.expander(_query_name, expanded=default_show):
                st.code(query)

    result = None
    col1, col2 = st.columns([1, 1])
    with col1:
        run_query = st.button(f"Run {_query_name}", type="primary", icon="🚀")
    with col2:
        clear_query = st.button(f"Clear {_query_name} results", type="secondary", icon="🗑️")
    if run_query:
        client = get_bq_client()
        query_job = client.query(query)
        result = query_job.result().to_dataframe()
        if cache_key:
            st.session_state[cache_key] = result
    if cache_key and cache_key in st.session_state:
        result = st.session_state.get(cache_key)
    if clear_query:
        result = None
        if cache_key:
            st.session_state[cache_key] = None
    if result is not None:
        st.dataframe(result)
