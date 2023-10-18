from sqlalchemy.sql import text
import pandas as pd

class Localidades:
    def __init__(self, db) :
        self.comunas = None
        self.regiones = None
        self.db = db
    
    def getComunas(self):
        #Database query
        query = text("SELECT comuna_id, nombre, poblacion, area, geometria FROM Comuna")

        if(self.comunas == None):
            self.comunas = []
            with self.db.connect() as con:
                result = con.execute(query)
                for comuna in result:
                    self.comunas.append(list(comuna))   
        df = pd.DataFrame(self.comunas, columns=['comuna_id', 'Nombre', 'Poblacion', 'Area', 'Geometria'])
        return df
    
    def getRegiones():
        pass
    

