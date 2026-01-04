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

    #Get tables in a specific schema
    def get_tables(self, schema_name):
        df = self.session.sql(f"SHOW TABLES IN SCHEMA {schema_name}").collect()
        tables = [row["name"] for row in df]
        return tables
    
    #Get views in a specific schema
    def get_views(self, schema_name):
        df = self.session.sql(f"SHOW VIEWS IN SCHEMA {schema_name}").collect()
        views = [row["name"] for row in df]
        return views

    #Get columns in a specific table/view 
    def get_columns(self, schema_name, table_name, obj_type):
        if obj_type in ('Table','Dynamic Table'):
            df = self.session.sql(f"DESCRIBE TABLE {schema_name}.{table_name}").collect()
        elif obj_type == 'View':
            df = self.session.sql(f"DESCRIBE VIEW {schema_name}.{table_name}").collect()
        columns = [(row["name"], row["type"]) for row in df]
        return columns

# Factory function to get the provider
def get_data_provider():
    #if local -> use Mock, if Server -> use Real
    return RealDataProvider()
    #return MockDataProvider()