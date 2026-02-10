import streamlit as st
import pandas as pd
from utils.snowflake_connector import get_session
from utils.data_provider import get_data_provider
from components.table_editor import create_table, modify_table
from components.view_editor import create_view, modify_view
from components.dynamictable_editor import create_dynamic_table, modify_dynamic_table
from components.deploy_ui import display_deploy_button



session = get_session()
database = session.get_current_database()
provider = get_data_provider()

def create_object():
    st.markdown("### Create new object")
    st.markdown("Configure your new Snowflake object below.")

    #TARGET CONFIGURATION
    with st.container(border=True):
        st.markdown("#### 1. Target Definition")
        
        c1, c2, c3 = st.columns([1, 1, 2]) #Uneven columns for better spacing
        
        with c1:
            obj_type = st.selectbox("Object Type", ["Table", "View", "Dynamic Table"])
        
        with c2:
            target_schema = st.selectbox("Target Schema", provider.get_schemas(database))
        
        with c3:
            target_name = st.text_input("Object Name", placeholder="e.g. CLEAN_USERS_T")

    source_tables = []
    joins = []
    #SOURCE CONFIGURATION 
    if obj_type in ("Dynamic Table", "View"):
        with st.container(border=True):
            st.markdown("#### 2. Source Data")
            
            if "builder_source_tables" not in st.session_state:
                st.session_state.builder_source_tables = []
            
            # Add Source Table UI
            c1, c2, c3, c4 = st.columns([3, 3, 2, 2])
            with c1:
                src_schema = st.selectbox("Source Schema", provider.get_schemas(database), key="new_src_schema")
            with c2:
                src_table = st.selectbox("Source Table", provider.get_tables(src_schema), key="new_src_table")
            with c3:
                # Default alias: T + count
                next_alias = f"T{len(st.session_state.builder_source_tables) + 1}"
                src_alias = st.text_input("Alias", value=next_alias, key="new_src_alias")
            with c4:
                st.write("") # Spacer
                if st.button("Add Source"):
                    # Check duplication
                    if not any(t['alias'] == src_alias for t in st.session_state.builder_source_tables):
                        st.session_state.builder_source_tables.append({
                            'schema': src_schema, 
                            'table': src_table, 
                            'alias': src_alias
                        })
                    else:
                        st.error("Alias must be unique")

            # Display Selected Sources
            if st.session_state.builder_source_tables:
                st.markdown("**Selected Sources:**")
                st.dataframe(pd.DataFrame(st.session_state.builder_source_tables))
                
                if st.button("Clear Sources"):
                     st.session_state.builder_source_tables = []
            
            source_tables = st.session_state.builder_source_tables

            # Joins Configuration
            if len(source_tables) > 1:
                st.markdown("#### Joins")
                join_types = ["LEFT JOIN", "INNER JOIN", "RIGHT JOIN", "FULL OUTER JOIN"]
                # available_aliases should exclude the first table (base table) to avoid self-joins on the same alias
                available_aliases = [t['alias'] for t in source_tables if t['alias'] != source_tables[0]['alias']]
                
                # Initial Data
                join_df = pd.DataFrame(columns=["join_type", "right_alias", "on_condition"])

                join_editor = st.data_editor(
                    join_df,
                    num_rows="dynamic",
                    column_config={
                        "join_type": st.column_config.SelectboxColumn("Type", options=join_types, required=True),
                        "right_alias": st.column_config.SelectboxColumn("Right Table", options=available_aliases, required=True),
                        "on_condition": st.column_config.TextColumn("ON Condition", required=True, width="large")
                    },
                    key="join_editor"
                )
                
                joins = join_editor.to_dict('records')
            
            


    #ADVANCED SETTINGS
    if obj_type == "Dynamic Table":
        with st.container(border=True):
            st.markdown("#### Dynamic Table Settings")
            
            c1, c2 = st.columns(2)
            with c1:
                warehouse = st.selectbox("Warehouse", ["COMPUTE_WH"], help="WH used for the refresh") #TODO: Get the WHs
            with c2:
                target_lag = st.text_input("Refresh Lag", value="1 minute", help="e.g. '1 minute', '1 hour'")

    st.divider()



    #EDITORS:
    st.subheader(f"Design {obj_type} Columns")
    
    final_ddl = None # Initialize variable

    if obj_type == 'Table':
        # Assumes create_table handles the UI for column editing
        final_ddl = create_table(target_schema, target_name)

    elif obj_type == 'View':
        if source_tables:
            final_ddl = create_view(source_tables, joins, target_schema, target_name)
        else:
             st.info("Please select source tables.")

    elif obj_type == 'Dynamic Table':
        if source_tables:
            final_ddl = create_dynamic_table(source_tables, joins, target_schema, target_name, warehouse, target_lag)
        else:
             st.info("Please select source tables.")



    # PREVIEW & DEPLOY ---
    if final_ddl:
        st.divider()
        st.markdown("#### Review & Deploy")
        
        st.code(final_ddl, language='sql')
        commitmsg = st.text_input("Commit message", value="Commit msg")
        #Deployment Button
        display_deploy_button(final_ddl,target_schema,obj_type,target_name,commitmsg)

    
    return None




def modify_object():
    st.markdown("### Modify an existing object")
    st.markdown("Configure your Snowflake object below.")

    #TARGET CONFIGURATION
    with st.container(border=True):
        st.markdown("#### 1. Target Definition")
        
        c1, c2, c3 = st.columns([1, 1, 2]) #Uneven columns for better spacing
        
        with c1:
            obj_type = st.selectbox("Object Type", ["Table", "View", "Dynamic Table"])
        
        with c2:
            selected_schema = st.selectbox("Select Schema", provider.get_schemas(database))
        
        with c3:
            if obj_type == "View":
                object_name = st.selectbox("Select Object", provider.get_views(selected_schema))
            elif obj_type == "Table":
                object_name = st.selectbox("Select Object", provider.get_tables(selected_schema, 'normal'))
            elif obj_type == "Dynamic Table":
                object_name = st.selectbox("Select Object", provider.get_tables(selected_schema, 'dynamic'))

    
    
    #EDITORS:
    st.subheader(f"Design {obj_type} Columns")
    
    final_ddl = None # Initialize variable

    if obj_type == 'Table':
        
        final_ddl = modify_table(selected_schema, object_name)

    if obj_type == 'View':
        
        final_ddl = modify_view(selected_schema, object_name)

    if obj_type == 'Dynamic Table':
        final_ddl = modify_dynamic_table(selected_schema, object_name)


    



    # PREVIEW & DEPLOY ---
    if final_ddl:
        st.divider()
        st.markdown("#### Review & Deploy")
        
        st.code(final_ddl, language='sql')
        
        commitmsg = st.text_input("Commit message", value="Commit msg")
        #Deployment Button
        display_deploy_button(final_ddl,selected_schema,obj_type,object_name,commitmsg)
    
    return None