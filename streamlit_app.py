import streamlit as st
from utils.snowflake_connector import get_session
from utils.data_provider import get_data_provider
from models.table import Table  
from models.view import View
from models.dynamic_table import DynamicTable



st.title("Snowflake ETL Builder") 

# Sidebar Navigation
st.sidebar.title("Navigation")
layers = ["Home", "Bronze", "Silver", "Gold"]
selected_layer = st.sidebar.radio("Select Layer", layers)


# Main Page Logic
if selected_layer == "Home":
    st.write("### Welcome to the ETL Builder")
    
    # Connection Check
    #session = get_session()
    #if session:
    #    st.success(f"Connected to Snowflake! Current Role: {session.get_current_role()}")
    #    st.info(f"Current Warehouse: {session.get_current_warehouse()}")
    #else:
    #    st.warning("No active Snowflake session found.")


elif selected_layer == "Bronze":
    st.header("Bronze Layer - Raw Data")
    

    st.write("#### Preview of a Standard Bronze Table:")
    
    # Instantiate a fake table
    bronze_table = Table(
        schema="DB.BRONZE", 
        name="LANDING_USERS", 
        columns="id INT, json_data VARIANT, load_date TIMESTAMP"
    )
    
    # Display the DDL using Streamlit's code block
    st.code(bronze_table.create_ddl(), language='sql')


elif selected_layer == "Silver":
    st.header("Silver Layer - Builder")
    

    #Get Data Provider
    provider = get_data_provider()

    st.subheader("1. Select Source")
    #pretend looking at the Bronze schema
    source_tables = provider.get_tables("BRONZE_DB.RAW") 
    selected_source = st.selectbox("Choose a Source Table", source_tables)

    # Get columns for that source
    columns = provider.get_columns(selected_source)
    st.write(f"**Source Columns from {selected_source}:**")
    st.json(columns) # Quick way to visualize the data

    st.divider() #Add a visual line

    st.subheader("2. Define Target")
    obj_type = st.selectbox("Type", ["Table", "View", "Dynamic Table"])


    if obj_type == "Dynamic Table":
        # Create a form so the app doesn't reload on every keystroke
        with st.form("dt_form"):
            col1, col2 = st.columns(2) # Make it look nice with 2 columns
            
            with col1:
                name_input = st.text_input("Table Name")
                schema_input = st.text_input("Schema", value="SILVER_DB.CLEAN")
                wh_input = st.selectbox("Warehouse", ["COMPUTE_WH", "ETL_WH"])
            
            with col2:
                lag_input = st.text_input("Target Lag", value="1 minute")
            
            # Text area for the SQL Logic
            sql_input = st.text_area("Select Statement (Logic)", height=150)
            
            # The Submit Button
            submitted = st.form_submit_button("Generate DDL")
            
            if submitted:
                # Instantiate a fake dynamictable
                # use the variables from the form (name_input, etc.) 
                new_dt = DynamicTable(
                    name=name_input,
                    schema=schema_input,
                    columns=sql_input,      
                    warehouse=wh_input,     
                    target_lag=lag_input    
                )
                
                st.success("Object generated successfully!")
                st.code(new_dt.create_ddl(), language='sql')


elif selected_layer == "Gold":
    st.header("Gold Layer - Aggregated Facts")
    st.write("This section is under construction.")