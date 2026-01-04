from models.base import DatabaseObject


class View(DatabaseObject):

    def __init__(self, schema, name, columns,col_names,source_object):
        # super(): pass the standard stuff to the Parent (base.py - DatabaseObject)
        super().__init__(schema, name, columns)
        
        # Save the new specific stuff to self
        self.col_names = col_names #to store only the name of the columns, withput the types
        self.sourceobject = source_object


    def create_ddl(self):       
        ddl = f"""CREATE OR REPLACE VIEW {self.schema}.{self.name}(\n\t{self.col_names}\n)\nAS SELECT\n\t{self.columns}\nFROM {self.sourceobject};
        """
        return ddl