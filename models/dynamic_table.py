from models.base import DatabaseObject


class DynamicTable(DatabaseObject):

    def __init__(self, schema, name, columns, col_names, source_object, warehouse, target_lag):
        # super(): pass the standard stuff to the Parent (base.py - DatabaseObject)
        super().__init__(schema, name, columns)
        
        # Save the new specific stuff to self
        self.col_names = col_names #to store only the name of the columns, withput the types
        self.sourceobject = source_object
        self.warehouse = warehouse
        self.target_lag = target_lag

    def create_ddl(self):
            ddl = f"""CREATE OR REPLACE DYNAMIC TABLE {self.schema}.{self.name}\nTARGET_LAG = '{self.target_lag}'\nWAREHOUSE = {self.warehouse}\n(\n\t{self.col_names}\n)\nAS SELECT\n\t{self.columns}\nFROM {self.sourceobject};
            """
            return ddl.strip() # strip() removes extra whitespace from the start/end