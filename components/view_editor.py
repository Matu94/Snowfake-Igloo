#Base Types 
import streamlit as st
import pandas as pd
from models.view import View  
from utils.data_provider import get_data_provider

#Base Types 
sf_types = ["NUMBER", "VARCHAR", "BOOLEAN", "TIMESTAMP", "DATE", "VARIANT", "FLOAT"]
#Base df
#Prepare Data & Dynamic Options for existing datatypes from get_columns()
provider = get_data_provider()




def create_view(source_tables, joins, target_schema, target_name):
     
    #1. Create dynamic col_type options (both standard and already existing)
    #need this because i gave the coice to select the base types, but already existing can have more precies ones like NUMBER(38,0)
    #Fetch ALL columns at once
    rows_list = []

    # Iterate through all source tables
    for tbl in source_tables:
        schema = tbl['schema']
        table = tbl['table']
        alias = tbl['alias']
        
        # We assume source is a Table for now, based on builders_ui.py behavior
        # In a generic implementation we might need to know if it's a View
        source_cols = provider.get_columns(schema, table, 'Table') 
        
        for col_name, col_type, nullable in source_cols:
            rows_list.append({
                "src_col_nm": f"{alias}.{col_name}", # Display with Alias
                "new_col_nm": col_name,
                "transformation": "",
                "data_type": col_type 
            })

            #Add this specific/more precise type to list if it's not there
            if col_type not in sf_types:
                sf_types.append(col_type)

    #2. Create the DataFrame based on existing and base objects
    default_data = pd.DataFrame(rows_list)  


    #3. Create the Editor  
    #Now 'options' includes both standard types and the specific ones fromsource
    editor_result = st.data_editor(
        default_data,
        num_rows="dynamic",
        column_config={
            "src_col_nm": st.column_config.TextColumn("Source Column", required=True, disabled = True),
            "new_col_nm": st.column_config.TextColumn("New Column Name", required=True),
            "transformation": st.column_config.TextColumn("Transformation", help = "eg. 'LEFT()'"),
            "data_type": st.column_config.SelectboxColumn(
                "Data Type", 
                options=sorted(list(set(sf_types))), #set removes duplicates, list converts it back to liust, sorted ofc sort it...
                required=True #This tells the data editor that this specific cell cannot be empty
            )
        },
        use_container_width=True,
        key="view_create_editor"     #unique ID badge for this 'widget' 
    )   


    #4. Generate DDL   
    col_definitions = []
    col_names_only = [] #For view DDL

    for index, row in editor_result.iterrows(): #need index to have string as a result, not tuple
        if row["src_col_nm"]: 
            
            rule = row['transformation'] if row['transformation'] else row['src_col_nm']    #Check that we have rule or not. if not, use the original column name

            # If user explicitly wrote a transformation rule, trust them. 
            # If not, use the src_col_nm which now includes the alias (e.g. "T1.ID")
            
            if rule != row["new_col_nm"]:  
                col_str = f"{rule}::{row['data_type']} AS {row['new_col_nm']}"
            else:
                col_str = f"{row['src_col_nm']}::{row['data_type']}"
            col_definitions.append(col_str) 
            col_names_only.append(row["new_col_nm"])   
    cols_sql = ",\n\t".join(col_definitions)          
    cols_names_str = ",\n\t".join(col_names_only)       

    # Construct the FROM clause
    # source_tables[0] is the base
    base_tbl = source_tables[0]
    from_clause = f"{base_tbl['schema']}.{base_tbl['table']} {base_tbl['alias']}"
    
    # Append joins
    for join in joins:
        # join = {'join_type': 'LEFT JOIN', 'right_alias': 'T2', 'on_condition': 'T1.ID = T2.ID'}
        # We need to find the right table definition to get schema/table name from alias
        right_tbl_def = next((t for t in source_tables if t['alias'] == join['right_alias']), None)
        
        if right_tbl_def:
            from_clause += f"\n{join['join_type']} {right_tbl_def['schema']}.{right_tbl_def['table']} {right_tbl_def['alias']} ON {join['on_condition']}"

    #5. Object display  
    result = View(
        schema = target_schema, 
        name = target_name, 
        columns=cols_sql,
        col_names=cols_names_str,
        source_object = from_clause) # Pass the full FROM/JOIN clause as source_object
    
    
    
    return result.create_ddl()



def modify_view(selected_schema, selected_object_name, source_tables, joins):
    
    #1. Create dynamic col_type options (both standard and already existing)
    #need this because i gave the coice to select the base types, but already existing can have more precies ones like NUMBER(38,0)
    #Fetch ALL columns at once
    rows_list = []
    
    # We want to show ALL source columns (Source-Driven) 
    # BUT we want to pre-fill with existing transformations if they exist.
    
    # If source_tables is empty (e.g. initial load failed or cleared), maybe fallback to old behavior?
    # But builders_ui tries to populate it.
    
    if not source_tables:
        # Fallback to old simple mode if no sources defined (shouldn't happen if parsing works)
        # Or just show empty and let user add sources
        source_cols_from_view = provider.get_columns(selected_schema, selected_object_name, 'View')
        for col_name, col_type, nullable in source_cols_from_view:
             rows_list.append({
                "src_col_nm": col_name,
                "new_col_nm": col_name,
                "transformation": provider.get_transform_by_alias(selected_schema,selected_object_name,'View',col_name),
                "data_type": col_type #This can be 'NUMBER(38,0)', wich is not part of the base types
            })

        #Add this specific/more precise type to list if it's not there
             if col_type not in sf_types:
                sf_types.append(col_type)
    else:
        # Source-Driven Logic
        # 1. Get all potential source columns
        # 2. Get existing view columns + transformations
        
        # Get existing transformations map: alias -> {transformation, type}
        existing_transforms = provider.get_transform(selected_schema, selected_object_name, 'View')
        # Convert to a dict for easy lookup by Alias (new_col_nm)
        # But wait, we need to map Source Column -> Target Column.
        # Use simple name matching? 
        # If DDL is "SELECT T1.ID AS USER_ID ...", we have "USER_ID" and transform "T1.ID".
        # We need to find the row where src_col_nm is "T1.ID" and set new_col_nm to "USER_ID".
        
        # Let's build a map of Source Expression -> Target Details
        # existing_transform_map = { tf['transformation'].upper(): tf for tf in existing_transforms } 
        # This is hard because transformation can be anything.
        
        # Simpler approach for version 1:
        # List all source columns.
        # If a source column matches an existing target column NAME (and isn't transformed), pre-fill it.
        # If the view has complex transformations, we might miss them if we only iterate source columns.
        
        # Better approach:
        # 1. Iterate all source columns.
        # 2. Check if this source column is used in the existing view logic? Hard to parse.
        
        # Let's stick to the Plan: List all source columns.
        # For each source column, check if it exists in the view output (by name matching if simple, or by checking the definition).
        
        # Actually, let's just LIST all source columns as available. 
        # If the user already defined the view, we try to match by Column Name? 
        # No, "modify" means "re-define". 
        # IF we want to preserve existing work, we must check if "T1.ID" was mapped to "ID" or "USER_ID".
        # `get_transform` gives us the list of output columns and their source expressions.
        
        current_view_defs = provider.get_transform(selected_schema, selected_object_name, 'View')
        # current_view_defs = [{'alias': 'ID', 'type': 'NUMBER', 'transformation': 'T1.ID'}, ...]
        
        # Create a lookup: Source Expression -> Target Alias
        # Because we iterate over Source Columns (e.g. T1.ID), we check if 'T1.ID' is used as a transformation source.
        src_to_target = {}
        for cvd in current_view_defs:
            # Transformation is currently just the string before ::TYPE
            # e.g. "T1.ID" or "LEFT(T1.NAME, 2)"
            clean_transform = cvd['transformation'].strip()
            src_to_target[clean_transform] = cvd
            
        for tbl in source_tables:
            schema = tbl['schema']
            table = tbl['table']
            alias = tbl['alias']
            
            source_cols = provider.get_columns(schema, table, 'Table') 
            
            for col_name, col_type, nullable in source_cols:
                src_col_full = f"{alias}.{col_name}"
                
                # Check if this exact column is used as a source
                match = src_to_target.get(src_col_full)
                
                if match:
                    new_name = match['alias']
                    transform = "" # It's a direct map
                    d_type = match['type']
                else:
                    # New or unused column
                    new_name = col_name 
                    transform = "" # Default to empty
                    d_type = col_type

                rows_list.append({
                    "src_col_nm": src_col_full,
                    "new_col_nm": new_name,
                    "transformation": transform,
                    "data_type": d_type 
                })

                if d_type not in sf_types:
                    sf_types.append(d_type)

    
    #2. Create the DataFrame based on existing and base objects
    default_data = pd.DataFrame(rows_list)  


    #3. Create the Editor  
    #Now 'options' includes both standard types and the specific ones fromsource
    editor_result = st.data_editor(
        default_data,
        num_rows="dynamic",
        column_config={
            "src_col_nm": st.column_config.TextColumn("Source Column", required=True, disabled = True),
            "new_col_nm": st.column_config.TextColumn("New Column Name", required=True),
            "transformation": st.column_config.TextColumn("Transformation", help = "eg. 'LEFT()'"),
            "data_type": st.column_config.SelectboxColumn(
                "Data Type", 
                options=sorted(list(set(sf_types))), #set removes duplicates, list converts it back to liust, sorted ofc sort it...
                required=True #This tells the data editor that this specific cell cannot be empty
            )
        },
        use_container_width=True,
        key="view_modify_editor"     #unique ID badge for this 'widget' 
    )   


    #4. Generate DDL   
    col_definitions = []
    col_names_only = [] #For view DDL

    for index, row in editor_result.iterrows(): #need index to have string as a result, not tuple
        if row["src_col_nm"]: 
            
            rule = row['transformation'] if row['transformation'] else row['src_col_nm']    #Check that we have rule or not. if not, use the original column name

            if rule != row["new_col_nm"]:  #If we have rule that means we need to build the string in a different way, we need alias anyways with this method
                col_str = f"{rule}::{row['data_type']} AS {row['new_col_nm']}"
            else:
                col_str = f"{row['src_col_nm']}::{row['data_type']}"
            col_definitions.append(col_str) 
            col_names_only.append(row["new_col_nm"])   
    cols_sql = ",\n\t".join(col_definitions)          
    cols_names_str = ",\n\t".join(col_names_only)      

    # Construct the FROM clause from the PASSED source_tables and joins (Edited version)
    source_object = ""
    if source_tables:
        base_tbl = source_tables[0]
        from_clause = f"{base_tbl['schema']}.{base_tbl['table']} {base_tbl['alias']}"
        
        for join in joins:
            right_tbl_def = next((t for t in source_tables if t['alias'] == join['right_alias']), None)
            if right_tbl_def:
                from_clause += f"\n{join['join_type']} {right_tbl_def['schema']}.{right_tbl_def['table']} {right_tbl_def['alias']} ON {join['on_condition']}"
        source_object = from_clause
    else:
        # Fallback if no sources passed? 
        source_schema_name, source_obj_name = provider.get_source(selected_schema,selected_object_name,'View')
        source_object = f"{source_schema_name}.{source_obj_name}"


    #5. Object display  
    result = View(
        schema = selected_schema, 
        name = selected_object_name, 
        columns=cols_sql,
        col_names=cols_names_str,
        source_object = source_object)
    
    
    return result.create_ddl()