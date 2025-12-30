import streamlit as st
from snowflake.snowpark import Session
from snowflake.snowpark.context import get_active_session


def get_session():
    """
    1. Tries to get the active session (works if running inside Snowflake).
    2. If that fails, looks for local credentials in .streamlit/secrets.toml.
    """
    try:
        # Option A: Are we inside Snowflake?
        session = get_active_session()
        return session
    except Exception:
        # Option B: We are local, let's login manually
        if "snowflake" in st.secrets: #snowflake is in the first line of the .toml file
            try:
                # Create a session using the details from secrets.toml
                session = Session.builder.configs(st.secrets["snowflake"]).create()
                return session
            except Exception as e:
                st.error(f"Local login failed: {e}")
                return None
        else:
            st.error("No active session and no local secrets found.")
            return None