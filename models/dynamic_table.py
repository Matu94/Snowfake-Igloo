from models.base import DatabaseObject


class DynamicTable(DatabaseObject):

    def __init__(self, name, schema, columns, source_object, warehouse, target_lag):
        # super(): pass the standard stuff to the Parent (base.py - DatabaseObject)
        super().__init__(name, schema, columns)
        
        # Save the new specific stuff to self
        self.sourceobject = source_object
        self.warehouse = warehouse
        self.target_lag = target_lag

    def create_ddl(self):
            ddl = f"""
            CREATE OR REPLACE DYNAMIC TABLE {self.schema}.{self.name}
                TARGET_LAG = '{self.target_lag}'
                WAREHOUSE = {self.warehouse}
                AS
                {self.columns}
                FROM {self.sourceobject}
            """
            return ddl.strip() # strip() removes extra whitespace from the start/end