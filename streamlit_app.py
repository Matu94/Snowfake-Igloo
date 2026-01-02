import streamlit as st
import pandas as pd
from utils.snowflake_connector import get_session
from utils.data_provider import get_data_provider
from models.table import Table  
from models.view import View
from models.dynamic_table import DynamicTable


#   !!!!!!!!    Page Config     !!!!!!!!
st.set_page_config(page_title="Snowflake Builder", layout="wide")
st.title("❄️ Snowflake Object Builder")

st.sidebar.title("Menu")
page = st.sidebar.radio("Go to", ["Home", "Create New Object", "Modify Existing"])




# ==========================================
# PAGE 1: HOME (Dashboard)
# ==========================================
if page == "Home":
    st.write("### Welcome to the ETL Builder")
    
    # Connection Check
    #session = get_session()
    #if session:
    #    st.success(f"Connected to Snowflake! Current Role: {session.get_current_role()}")
    #    st.info(f"Current Warehouse: {session.get_current_warehouse()}")
    #else:
    #    st.warning("No active Snowflake session found.")



# ==========================================
# PAGE 2: CREATE NEW OBJECT 
# ==========================================
elif page == "Create New Object":

    st.header("🆕 Create Object from Scratch")
    st.caption("Define your object structure manually by adding columns.")

    provider = get_data_provider()

    # 1. Object Settings
    with st.container():
        col1, col2, col3 = st.columns(3)
        with col1:
            obj_type = st.selectbox("Select the new object type:", ["Table", "Dynamic Table", "View"])
            target_schema = st.selectbox("Target Schema", provider.get_schemas())
        with col2:
            target_name = st.text_input("Object Name", placeholder="e.g. USERS_RAW")
            if obj_type in ("Dynamic Table", "View"):
                editor_source_object = st.selectbox("Select source object:", provider.get_tables(target_schema))
        with col3:
            if obj_type == "Dynamic Table":
                target_lag = st.text_input("Target lag:", placeholder="e.g. 10 min")
                warehouse = st.text_input("Warehouse:", placeholder="e.g. WH_ADHOC")

    st.divider()

    # 2. Column Definition Editor
    st.subheader("Define Columns")
    
    default_data = pd.DataFrame(
        [{"Column Name": "ID", "Transformation Rule": "", "Data Type": "NUMBER", "Nullable": True}],
    )
    # Default structure for the editor - if view or dt, set the source columns with types
    if obj_type in ("Dynamic Table", "View"):
        for i in range(2):
            default_data = default_data.add(pd.DataFrame( 
                [{"Column Name": provider.get_columns(editor_source_object)[i][0], 
                  "Transformation Rule": "", 
                  "Data Type": provider.get_columns(editor_source_object)[i][1], 
                  "Nullable": True}])
                 #{"Column Name": provider.get_columns(editor_source_object)[2][0], 
                 #"Transformation Rule": "", 
                 #"Data Type": provider.get_columns(editor_source_object)[2][1], 
                 #"Nullable": True}]

        )
    else:
        default_data = pd.DataFrame(
            [{"Column Name": "ID", "Transformation Rule": "", "Data Type": "NUMBER", "Nullable": True}],
        )
    

    # Types available in Snowflake
    sf_types = ["NUMBER", "VARCHAR", "BOOLEAN", "TIMESTAMP", "DATE", "VARIANT", "FLOAT"]

    # The Editor allows adding/deleting rows (num_rows="dynamic")
    editor_result = st.data_editor(
        default_data,
        num_rows="dynamic", 
        column_config={
            "Column Name": st.column_config.TextColumn("Column Name", required=True),
            "Transformation Rule": st.column_config.TextColumn("Transformation Logic (SQL)", help="e.g. UPPER(col) or CAST(col as int)"),
            "Data Type": st.column_config.SelectboxColumn("Data Type", options=sf_types, required=True),
            "Nullable": st.column_config.CheckboxColumn("Allow Nulls?"),
        },
        use_container_width=True,
        key="create_editor"
    )

    #Based on editor, create the syntax ready DDL
    col_definitions = []
    for index, row in editor_result.iterrows():
        if row["Column Name"]: # Skip empty rows
            # Logic: NAME TYPE [NOT NULL]
            rule = f"{row['Transformation Rule']} AS {row['Column Name']}" if row['Transformation Rule'] else row['Column Name']
            col_str = f"{rule} {row['Data Type']}"
            if not row["Nullable"]:
                col_str += " NOT NULL"
            col_definitions.append(col_str)
    
    # Join them
    cols_sql = ",\n".join(col_definitions)

    #Generate DDL based on editor_result
    #3 Object display
    if obj_type == 'View':

        result = View(
            schema = target_schema, 
            name = target_name, 
            columns=cols_sql,
            source_object = editor_source_object)

        st.code(result.create_ddl(), language='sql')

    if obj_type == 'Table':

        result = Table(
            schema = target_schema, 
            name = target_name, 
            columns=cols_sql)

        st.code(result.create_ddl(), language='sql')


    #if obj_type == 'Dynamic Table':
    #
    #    result = DynamicTable(
    #        schema = target_schema, 
    #        name = target_name, 
    #        columns=cols_sql)
    #
    #    st.code(result.create_ddl(), language='sql')





# ==========================================
# PAGE 3: MODIFY EXISTING 
# ==========================================
elif page == "Modify Existing":
    st.header("Silver Layer - Builder")
    provider = get_data_provider()

    #1: Source Selection 
    st.subheader("1. Select Source")
    source_tables = provider.get_tables("BRONZE_DB.RAW") 
    selected_source = st.selectbox("Choose a Source Table", source_tables)

    #2: Column Mapping
    st.subheader("2. Define Transformations")
    
    # Get raw columns: [("ID", "NUMBER"), ("NAME", "VARCHAR")...]
    raw_columns = provider.get_columns(selected_source)
    
    # Convert to DataFrame for the Editor
    # add empty columns for 'Rename To' and 'Transformation Logic'
    df_cols = pd.DataFrame(raw_columns, columns=["Source Column", "Data Type"])
    df_cols["Target Column Name"] = df_cols["Source Column"] # Default to same name
    df_cols["Transformation"] = "" # Empty by default
    df_cols["Include"] = True # Checkbox to keep/drop column

    # DISPLAY THE EDITOR
    st.write("Edit the columns below. Uncheck 'Include' to drop a column.")
    edited_df = st.data_editor(
        df_cols,
        column_config={
            "Include": st.column_config.CheckboxColumn("Keep?", help="Select to include in target"),
            "Source Column": st.column_config.TextColumn("Source", disabled=True), # Read-only
            "Data Type": st.column_config.TextColumn("Type", disabled=True),       # Read-only
            "Target Column Name": st.column_config.TextColumn("Target Name"),
            "Transformation": st.column_config.TextColumn("SQL Logic (Optional)", help="e.g. CAST(x AS INT) or UPPER(x)"),
        },
        hide_index=True,
        use_container_width=True
    )

    # 3: SQL Generation Logic ---
    st.divider()
    st.subheader("3. Preview SQL")
    
    if st.button("Preview Projection SQL"):
        #need to loop through the edited rows and build the SELECT list
        select_parts = []
        
        for index, row in edited_df.iterrows():
            if row["Include"]:
                src = row["Source Column"]
                tgt = row["Target Column Name"]
                logic = row["Transformation"]
                
                # Logic: If there is a transformation, use it. Otherwise use source column.
                
                if logic:
                    col_sql = f"{logic} AS {tgt}"
                elif src != tgt:
                    col_sql = f"{src} AS {tgt}"
                else:
                    col_sql = src 
                
                select_parts.append(col_sql)
        
        # Join them with commas
        final_select = "SELECT \n    " + ",\n    ".join(select_parts) + f"\nFROM {selected_source}"
        
        st.code(final_select, language="sql")


    st.divider() #Add a visual line

    st.subheader("4. Review & Configure")

    #use a distinct button to "Lock in and save" the transformation
    if st.button("Generate & Lock SQL"):
        # 1. Re-calculate the SQL (same logic as before)
        select_parts = []
        for index, row in edited_df.iterrows():
            if row["Include"]:
                src = row["Source Column"]
                tgt = row["Target Column Name"]
                logic = row["Transformation"]
                
                if logic:
                    col_sql = f"{logic} AS {tgt}"
                elif src != tgt:
                    col_sql = f"{src} AS {tgt}"
                else:
                    col_sql = src
                select_parts.append(col_sql)
        
        final_select = "SELECT \n    " + ",\n    ".join(select_parts) + f"\nFROM {selected_source}"
        
        #2. SAVE IT TO THE BACKPACK (Session State!!)
        st.session_state['generated_sql'] = final_select
        st.success("SQL Generated! Scroll down to configure deployment.")

    # The Intelligent Form 
    # Only show this form if we have generated SQL in the "backpack"
    if 'generated_sql' in st.session_state:
        
        # Show the SQL being used
        st.info("Using the following logic:")
        st.code(st.session_state['generated_sql'], language="sql")

        st.subheader("4. Deploy Object")
        
        #wrap the creation in a form
        with st.form("deployment_form"):
            col1, col2 = st.columns(2)
            with col1:
                obj_type = st.selectbox("Object Type", ["Dynamic Table", "View", "Table"])
                tgt_schema = st.text_input("Target Schema", value="SILVER_DB.CLEAN")
                tgt_name = st.text_input("Target Name", value=f"CLEAN_{selected_source}")
            
            with col2:
                wh = st.selectbox("Warehouse", ["COMPUTE_WH", "ETL_WH"])
                lag = st.text_input("Target Lag", value="1 minute")

            # The Grand Finale Button
            deploy_clicked = st.form_submit_button("Deploy to Snowflake")

            if deploy_clicked:
                # 1. Instantiate the correct Object based on dropdown
                if obj_type == "Dynamic Table":
                    new_obj = DynamicTable(tgt_name, tgt_schema, st.session_state['generated_sql'], wh, lag)
                elif obj_type == "View":
                    new_obj = View(tgt_name, tgt_schema, st.session_state['generated_sql'])
                else:
                    # Table logic might be different (CTAS), but let's assume CTAS for now
                    new_obj = Table(tgt_name, tgt_schema, st.session_state['generated_sql'])

                # 2. Get the Final DDL
                final_ddl = new_obj.create_ddl()

                # 3. Simulate Execution
                st.toast("Validating Syntax...", icon="🔄")
                
                # real app run: session.sql(final_ddl).collect()
                st.success(f"Successfully created {obj_type}: {tgt_name}")
                st.code(final_ddl, language="sql")
                
                # Optional: Clear state to start over
                # del st.session_state['generated_sql']


elif page == "Gold":
    st.header("Gold Layer - Aggregated Facts")
    st.write("This section is under construction.")