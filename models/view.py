from models.base import DatabaseObject


class View(DatabaseObject):

    def create_ddl(self):
        
        ddl = f"""
CREATE OR REPLACE VIEW {self.schema}.{self.name}
({self.columns});
        """
        return ddl