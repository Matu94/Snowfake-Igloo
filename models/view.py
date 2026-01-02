from models.base import DatabaseObject


class View(DatabaseObject):

    def __init__(self, name, schema, columns, source_object):
        # super(): pass the standard stuff to the Parent (base.py - DatabaseObject)
        super().__init__(name, schema, columns)
        
        # Save the new specific stuff to self
        self.sourceobject = source_object


    def create_ddl(self):       
        ddl = f"""
CREATE OR REPLACE VIEW {self.schema}.{self.name}
({self.columns})
FROM {self.sourceobject};
        """
        return ddl