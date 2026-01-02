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
page = st.sidebar.radio("Go to", ["Home", "Create New Object", "Modify Existing", "Sandbox"])

session = get_session()
database = session.get_current_database()



# ==========================================
# PAGE 1: HOME (Dashboard)
# ==========================================
if page == "Home":
    st.write("### Welcome to the ETL Builder")
    
    # Connection Check
    session = get_session()
    if session:
        st.success(f"Connected to Snowflake! Current Role: {session.get_current_role()}")
        st.info(f"Current Warehouse: {session.get_current_warehouse()}, Current Database: {session.get_current_database()}")
        database = session.get_current_database()
    else:
        st.warning("No active Snowflake session found.")



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
    st.header("Modif existing")
    #provider = get_data_provider()

    
# ==========================================
# PAGE 4: Sandbox
# ==========================================
elif page == "Sandbox":
    st.header("Sandbox")
    st.write("This section is my playground")

    provider = get_data_provider()
    st.code(provider.get_schemas(database))