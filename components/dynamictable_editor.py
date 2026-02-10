#Base Types 
import streamlit as st
import pandas as pd
from models.dynamic_table import DynamicTable  
from utils.data_provider import get_data_provider

#Base Types 
sf_types = ["NUMBER", "VARCHAR", "BOOLEAN", "TIMESTAMP", "DATE", "VARIANT", "FLOAT"]
#Base df
#Prepare Data & Dynamic Options for existing datatypes from get_columns()
provider = get_data_provider()



def create_dynamic_table(source_tables, joins, target_schema, target_name, warehouse, target_lag):
    
    #1. Create dynamic col_type options (both standard and already existing)
    #need this because i gave the coice to select the base types, but already existing can have more precies ones like NUMBER(38,0)
    #Fetch ALL columns at once
    rows_list = []
        
    # Iterate through all source tables
    for tbl in source_tables:
        schema = tbl['schema']
        table = tbl['table']
        alias = tbl['alias']
        
        # We assume source is a Table for now
        source_cols = provider.get_columns(schema, table, 'Dynamic Table') 
        
        for col_name, col_type, nullable in source_cols:
            rows_list.append({
                "src_col_nm": f"{alias}.{col_name}",
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
            "src_col_nm": st.column_config.TextColumn("Source Column", required=True, disabled=True),
            "new_col_nm": st.column_config.TextColumn("New Column Name", required=True),
            "transformation": st.column_config.TextColumn("Transformation", help = "eg. 'LEFT()'"),
            "data_type": st.column_config.SelectboxColumn(
                "Data Type", 
                options=sorted(list(set(sf_types))), #set removes duplicates, list converts it back to liust, sorted ofc sort it...
                required=True #This tells the data editor that this specific cell cannot be empty
            )
        },
        use_container_width=True,
        key="dynamictable_create_editor"     #unique ID badge for this 'widget' 
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
    cols_sql = ",\n\t".join(col_definitions)          #Result: "ID NUMBER, NAME VARCHAR"
    cols_names_str = ",\n\t".join(col_names_only)      #Result: "ID, NAME"   



    #5. Object display  
    # Construct the FROM clause
    base_tbl = source_tables[0]
    from_clause = f"{base_tbl['schema']}.{base_tbl['table']} {base_tbl['alias']}"
    
    # Append joins
    for join in joins:
        right_tbl_def = next((t for t in source_tables if t['alias'] == join['right_alias']), None)
        if right_tbl_def:
            from_clause += f"\n{join['join_type']} {right_tbl_def['schema']}.{right_tbl_def['table']} {right_tbl_def['alias']} ON {join['on_condition']}"

    result = DynamicTable(
        schema = target_schema, 
        name = target_name, 
        columns=cols_sql,
        col_names=cols_names_str,
        source_object=from_clause,
        warehouse=warehouse,
        target_lag=target_lag)
    
    
    return result.create_ddl()

def modify_dynamic_table(selected_schema, selected_object_name, source_tables, joins):
    
    # 1. Create dynamic col_type options
    rows_list = []
    #Build the rows from source 
    #rows_list is a list, and the result of get_columns is also a list with 2 stuffs in it. first is the column name, second is the type. So with this for loop i can build the required list
    if not source_tables:
        source_cols_from_dt = provider.get_columns(selected_schema, selected_object_name, 'Dynamic Table')
        for col_name, col_type, nullable in source_cols_from_dt:
             rows_list.append({
                "src_col_nm": col_name,
                "new_col_nm": col_name,
                "transformation": provider.get_transform_by_alias(selected_schema,selected_object_name,'Dynamic Table',col_name),
                "data_type": col_type   #can be number(38,0)
            })
    
        #Add this specific/more precise type to list if it's not there
             if col_type not in sf_types:
                sf_types.append(col_type)
    else:
        # Source-Driven Logic
        current_dt_defs = provider.get_transform(selected_schema, selected_object_name, 'Dynamic Table')
        
        src_to_target = {}
        for cdd in current_dt_defs:
            clean_transform = cdd['transformation'].strip()
            src_to_target[clean_transform] = cdd
            
        for tbl in source_tables:
            schema = tbl['schema']
            table = tbl['table']
            alias = tbl['alias']
            
            source_cols = provider.get_columns(schema, table, 'Table') # Assuming sources are tables
            
            for col_name, col_type, nullable in source_cols:
                src_col_full = f"{alias}.{col_name}"
                
                match = src_to_target.get(src_col_full)
                
                if match:
                    new_name = match['alias']
                    transform = "" 
                    d_type = match['type']
                else:
                    new_name = col_name 
                    transform = "" 
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
        key="dynamictable_modify_editor"     #unique ID badge for this 'widget' 
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
    cols_names_str = ",\n\t".join(col_names_only)       #Result: "ID, NAME"  

    # Construct source object from passed sources
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
        source_schema_name, source_obj_name = provider.get_source(selected_schema,selected_object_name,'Dynamic Table')
        source_object = f"{source_schema_name}.{source_obj_name}"
        
    warehouse, target_lag = provider.get_dynamic_table_config(selected_schema,selected_object_name)

    #5. Object display  
    result = DynamicTable(
        schema = selected_schema, 
        name = selected_object_name, 
        columns=cols_sql,
        col_names=cols_names_str,
        source_object = source_object,
        warehouse=warehouse,
        target_lag=target_lag)
    
    
    return result.create_ddl()