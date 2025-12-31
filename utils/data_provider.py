# utils/data_provider.py
import streamlit as st

class MockDataProvider:
    def get_schemas(self):
        return["BRONZE", "SILVER", "GOLD"]

    def get_tables(self, schema_name):
        """Returns a fake list of tables for testing UI"""
        if "BRONZE" in schema_name:
            return ["LANDING_USERS", "LANDING_ORDERS", "RAW_LOGS"]
        elif "SILVER" in schema_name:
            return ["DIM_CUSTOMERS", "FACT_ORDERS"]
        else:
            return ["UNKNOWN_TABLE"]

    def get_columns(self, table_name):
        """Returns fake columns based on table name"""
        if "USERS" in table_name:
            return [("ID", "NUMBER"), ("NAME", "VARCHAR"), ("CREATED_AT", "TIMESTAMP")]
        elif "ORDERS" in table_name:
            return [("ORDER_ID", "NUMBER"), ("USER_ID", "NUMBER"), ("AMOUNT", "FLOAT")]
        else:
            return [("COL_1", "VARCHAR"), ("COL_2", "NUMBER")]

# Factory function to get the provider
def get_data_provider():
    #if local -> use Mock, if Server -> use Real
    return MockDataProvider()