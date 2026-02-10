# utils/data_provider.py
import streamlit as st
import pandas as pd
from utils.snowflake_connector import get_session

#Get some sample data for offline dev
class MockDataProvider:
    def get_schemas(self, db_name):
        return["BRONZE", "SILVER", "GOLD"]

    def get_tables(self, schema_name):
        #Returns a fake list of tables for testing UI
        if "BRONZE" in schema_name:
            return ["LANDING_USERS", "LANDING_ORDERS", "RAW_LOGS"]
        elif "SILVER" in schema_name:
            return ["DIM_CUSTOMERS", "FACT_ORDERS"]
        else:
            return ["UNKNOWN_TABLE"]

    def get_columns(self, schema_name, table_name, obj_type):
        #Returns fake columns based on table name
        if "USERS" in table_name:
            return [("ID", "NUMBER"), ("NAME", "VARCHAR"), ("CREATED_AT", "TIMESTAMP")]
        elif "ORDERS" in table_name:
            return [("ORDER_ID", "NUMBER"), ("USER_ID", "NUMBER"), ("AMOUNT", "FLOAT")]
        else:
            return [("COL_1", "VARCHAR"), ("COL_2", "NUMBER")]


#returns real data from snowflake
class RealDataProvider:
    def __init__(self):
        self.session = get_session()

    #Get schemas in the current db
    def get_schemas(self, db_name):
        df = self.session.sql(f"SHOW SCHEMAS IN DATABASE {db_name}").collect()
        schemas = [
                row["name"] 
                for row in df 
                if row["name"] not in ["INFORMATION_SCHEMA", "PUBLIC"] #Optional filtering
            ]
        return schemas

    #Get tables in a specific schema, default is all so don't need to specify in some cases
    def get_tables(self, schema_name, obj_type='all'):
        #1 collect all data
        #maybe use UPPER() later, if someone was stupid enough to name the table with lowercase 
        df_all = self.session.sql(f"SHOW TABLES IN SCHEMA {schema_name}").collect()
        tables_all = [row["name"] for row in (df_all)]
        if obj_type == 'all':
            return tables_all

        #2 collect dt data
        df_dt = self.session.sql(f"SHOW DYNAMIC TABLES IN SCHEMA {schema_name}").collect()
        tables_dt = [row["name"] for row in (df_dt)]
        
        #handle dt/normal
        if obj_type == 'normal':
            return list(set(tables_all) - set(tables_dt))  #Have to use "sets" bc cant substract 1list rom another. INVALID:[1, 2, 3] - [2], VALID:{1, 2, 3} - {2}  
        elif obj_type == 'dynamic':
            return tables_dt

    
    #Get views in a specific schema
    def get_views(self, schema_name):
        df = self.session.sql(f"SHOW VIEWS IN SCHEMA {schema_name}").collect()
        views = [row["name"] for row in df]
        return views

    #Get columns in a specific table/view 
    def get_columns(self, schema_name, obj_name, obj_type):
        if obj_type in ('Table','Dynamic Table'):
            df = self.session.sql(f"DESCRIBE TABLE {schema_name}.{obj_name}").collect()
        elif obj_type == 'View':
            df = self.session.sql(f"DESCRIBE VIEW {schema_name}.{obj_name}").collect()
        columns = [(row["name"], row["type"], row["null?"]) for row in df]
        return columns
    
    #simple DESC command not enough to get the transforms like LEFT(ID,2)
    def get_transform(self, schema_name, obj_name, obj_type):
        if obj_type == 'View':
            df = self.session.sql(f"SELECT GET_DDL('VIEW', '{schema_name}.{obj_name}')").collect()
            

        elif obj_type == 'Dynamic Table':
            df = self.session.sql(f"SELECT GET_DDL('TABLE', '{schema_name}.{obj_name}')").collect()
            

        ddl = df[0][0]  # Extract the DDL string

        # Find the SELECT statement part
        select_start = ddl.upper().find('SELECT')
        from_start = ddl.upper().find('FROM', select_start)

        if select_start != -1 and from_start != -1:
            # Extract the column definitions between SELECT and FROM
            select_clause = ddl[select_start + 6:from_start].strip()

            # Split by comma (handling potential commas in functions)
            columns = []
            paren_depth = 0
            current_col = []

            # If we just split by commas, LEFT(KEK,2) would be incorrectly split into LEFT(KEK and 2)
            # At this point I understand this, but tomorrow i'll need AI again
            for char in select_clause:
                if char == '(':
                    paren_depth += 1
                elif char == ')':
                    paren_depth -= 1
                elif char == ',' and paren_depth == 0:
                    columns.append(''.join(current_col).strip())
                    current_col = []
                    continue
                current_col.append(char)

            # Add the last column
            if current_col:
                columns.append(''.join(current_col).strip())

            # Parse columns with transformations into different variables to be able to re-use them.
            results = []
            for col in columns:
                col_upper = col.upper()
                if ' AS ' in col_upper and '::' in col:
                    # Find the AS keyword position
                    as_pos = col_upper.rfind(' AS ')
                    alias = col[as_pos + 4:].strip()

                    # Everything before AS
                    before_as = col[:as_pos].strip()

                    # Find the :: to split transformation and type
                    type_pos = before_as.rfind('::')
                    transformation = before_as[:type_pos].strip()
                    data_type = before_as[type_pos + 2:].strip()

                    results.append({
                        'alias': alias,
                        'type': data_type,
                        'transformation': transformation
                    })

            return results

        return []
        
    #Helper method for transform, to be able to get the transformation based on the "alias"
    def get_transform_by_alias(self, schema_name, obj_name, obj_type, alias):
        transformations = self.get_transform(schema_name, obj_name, obj_type)
     
        for tf in transformations:
            if tf['alias'].upper() == alias.upper():
                return tf['transformation'].upper()
     
        return None  #or return {'alias': alias, 'type': None, 'transformation': None}
    

    # Returns the source details (tables and joins) from the DDL
    def get_source_details(self, schema_name, obj_name, obj_type):
        if obj_type == 'View':
            df = self.session.sql(f"SELECT GET_DDL('VIEW', '{schema_name}.{obj_name}')").collect()
        elif obj_type == 'Dynamic Table':
            df = self.session.sql(f"SELECT GET_DDL('TABLE', '{schema_name}.{obj_name}')").collect()
        
        ddl = df[0][0]
        
        # 1. Extract the FROM clause
        # We need to find the FROM that is part of the main SELECT, not a subquery
        # Simple heuristic: It's after the column definitions which are after "AS SELECT" or just "AS"
        
        # Setup defaults
        source_tables = []
        joins = []
        
        # Find FROM
        # This is a bit brittle with simple string manipulation, but sufficient for the generated DDLs
        # We assume the DDL structure is relatively standard as generated by this tool
        
        from_pos = ddl.upper().find('FROM')
        if from_pos == -1:
            return source_tables, joins
            
        # Get everything after FROM until the end or WHERE/GROUP BY/etc if we supported them (we don't currently)
        # The tool currently only generates FROM ... JOIN ...
        # Stop at semicolon
        clause = ddl[from_pos+4:].split(';')[0].strip()
        
        # Split by newlines to separate joins usually
        # But DDL might be one line. 
        # Let's try to split by known keywords "LEFT JOIN", "INNER JOIN", etc.
        # OR simply split by "JOIN" and reconstruct
        
        # Better approach for this specific tool's generation:
        # The tool generates: FROM schema.table alias \n LEFT JOIN schema.table alias ON condition
        
        # normalization
        clause = clause.replace('\n', ' ').replace('\t', ' ')
        
        # We can split by 'JOIN' but need to preserve the type (LEFT, INNER, etc)
        # Let's look for " JOIN "
        
        # Actually, let's just parse the first part as base table
        # Then look for joins
        
        # Identify start of next join
        join_types = ["LEFT JOIN", "INNER JOIN", "RIGHT JOIN", "FULL OUTER JOIN", "JOIN"]
        
        # We need to tokenize or split carefully. 
        # Simple parser loop:
        
        parts = []
        current_part = ""
        words = clause.split()
        
        # This implementation assumes the standard format generated by the tool
        # Example: DB.SCHEMA.TABLE T1 LEFT JOIN DB.SCHEMA.TABLE T2 ON T1.ID = T2.ID
        
        # 1. Get Base Table
        # The first token is likely Schema.Table. The second is Alias.
        # But checks for JOIN keywords
        
        # Let's use a regex approach for robustness if possible, or simple state machine
        import re
        
        # Match base table:  database.schema.table alias   OR   schema.table alias
        # We expect:  SourceTable [Alias]  (JoinType JoinTable [Alias] ON Condition)*
        
        # Let's try to get the raw textual parts
        
        # Split by join keywords to separate chunks
        # We replace join keywords with a delimiter
        temp_clause = clause
        for jt in join_types:
            temp_clause = re.sub(f"(?i){jt}", f"<JOIN_MARKER>{jt}", temp_clause)
            
        segments = temp_clause.split('<JOIN_MARKER>')
        
        # Segment 0 is the base table definition
        base_def = segments[0].strip()
        base_parts = base_def.split()
        
        # base_parts[0] should be schema.table (or db.schema.table)
        # base_parts[1] should be alias (optional but usually present in our tool)
        
        base_obj_path = base_parts[0]
        base_alias = base_parts[1] if len(base_parts) > 1 else f"T1" 
        
        # Parse schema/table from path
        if '.' in base_obj_path:
             path_parts = base_obj_path.split('.')
             if len(path_parts) == 3:
                 b_schema = path_parts[1]
                 b_table = path_parts[2]
             elif len(path_parts) == 2:
                 b_schema = path_parts[0]
                 b_table = path_parts[1]
             else:
                 b_schema = schema_name # fallback
                 b_table = base_obj_path
        else:
             b_schema = schema_name
             b_table = base_obj_path
             
        source_tables.append({
            'schema': b_schema,
            'table': b_table,
            'alias': base_alias
        })
        
        # Process Joins
        for seg in segments[1:]:
            seg = seg.strip()
            # seg looks like: "LEFT JOIN schema.table alias ON condition"
            # distinct join type
            
            # Find the ON keyword
            on_match = re.search(r"(?i)\s+ON\s+", seg)
            if not on_match:
                continue
                
            on_start = on_match.start()
            on_end = on_match.end()
            
            pre_on = seg[:on_start].strip() # "LEFT JOIN schema.table alias"
            on_condition = seg[on_end:].strip() # "T1.ID = T2.ID"
            
            # Parse pre_on to get Join Type and Table/Alias
            # pre_on words: "LEFT", "JOIN", "schema.table", "alias"
            pre_parts = pre_on.split()
            
            # Join type is definitely at the start
            # We know it ends with "JOIN"
            join_keyword_idx = -1
            for i, word in enumerate(pre_parts):
                if word.upper() == 'JOIN':
                     join_keyword_idx = i
                     break
            
            if join_keyword_idx != -1:
                join_type = " ".join(pre_parts[:join_keyword_idx+1])
                table_def_parts = pre_parts[join_keyword_idx+1:]
                
                t_obj_path = table_def_parts[0]
                t_alias = table_def_parts[1] if len(table_def_parts) > 1 else f"T{len(source_tables)+1}"
                
                # Parse schema/table
                if '.' in t_obj_path:
                     path_parts = t_obj_path.split('.')
                     if len(path_parts) == 3:
                         t_schema = path_parts[1]
                         t_table = path_parts[2]
                     elif len(path_parts) == 2:
                         t_schema = path_parts[0]
                         t_table = path_parts[1]
                     else:
                         t_schema = schema_name
                         t_table = t_obj_path
                else:
                     t_schema = schema_name
                     t_table = t_obj_path
                
                source_tables.append({
                    'schema': t_schema,
                    'table': t_table,
                    'alias': t_alias
                })
                
                joins.append({
                    'join_type': join_type.upper(),
                    'right_alias': t_alias,
                    'on_condition': on_condition
                })

        return source_tables, joins
        



    def get_dynamic_table_config(self, schema_name,obj_name):
        df = self.session.sql(f"SELECT GET_DDL('TABLE', '{schema_name}.{obj_name}')").collect()
        ddl = df[0][0] #have to get the "body" part

        #Find target_lag
        target_lag = None
        target_lag_pos = ddl.upper().find('TARGET_LAG')
        if target_lag_pos != -1:
            #Find the opening quote after target_lag =
            quote_start = ddl.find("'", target_lag_pos)
            if quote_start != -1:
                #Find the closing quote
                quote_end = ddl.find("'", quote_start + 1)
                if quote_end != -1:
                    target_lag = ddl[quote_start + 1:quote_end]

        #Find warehouse
        warehouse = None
        warehouse_pos = ddl.upper().find('WAREHOUSE')
        if warehouse_pos != -1:
            #Find the = sign after warehouse
            equals_pos = ddl.find('=', warehouse_pos)
            if equals_pos != -1:
                #et everything after = and extract the warehouse name
                after_equals = ddl[equals_pos + 1:].strip()
                #Split by whitespace and take the first word
                warehouse = after_equals.split()[0].strip()

        return warehouse, target_lag

# Factory function to get the provider
def get_data_provider():
    #if local -> use Mock, if Server -> use Real
    return RealDataProvider()
    #return MockDataProvider()