from sqlalchemy.sql import text
import pandas as pd

class Localidades:
    def __init__(self, db) :
        self.db = db
        self.comunas = self.getComunas()
        self.poblacionTotal = self.comunas['poblacion'].sum()
        print(self.poblacionTotal)
        self.regiones = None
                
    def getComunas(self):
        #Database query
        self.comunas = []
        with self.db.connect() as con:
            query = text("SELECT * FROM Comunas")
            result = con.execute(query)
            comunas = result.fetchall()
            column_names = result.keys()
            for comuna in comunas:
                self.comunas.append(list(comuna))   
        df_comunas = pd.DataFrame(self.comunas, columns=column_names)
        return df_comunas
        
    def getPoblacionTotal(self):
        return self.poblacionTotal

    def getDataComunas(self):
        return self.comunas