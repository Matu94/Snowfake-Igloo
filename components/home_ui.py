
import streamlit as st
from utils.snowflake_connector import get_session

def home():
    st.write("### Welcome!")

    # Connection Check
    session = get_session()
    if session:
        st.success(f"Connected to Snowflake! Current Role: {session.get_current_role()}")
        st.info(f"Current Warehouse: {session.get_current_warehouse()}, Current Database: {session.get_current_database()}")
        database = session.get_current_database()
    else:
        st.warning("No active Snowflake session found.")
    
    return None