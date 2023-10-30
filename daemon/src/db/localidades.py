from sqlalchemy.sql import text
import pandas as pd

class Localidades:
    def __init__(self, db) :
        self.db = db
        self.comunas = self.getComunas()
        self.poblacionTotal = self.comunas['poblacion'].sum()
        print(self.poblacionTotal)
        self.regiones = self.getRegiones()
                
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
    def getRegiones(self):
        self.regiones = []
        with self.db.connect() as con:
            query = text("SELECT * FROM Region")
            result = con.execute(query)
            regiones = result.fetchall()
            column_names = result.keys()
            for region in regiones:
                self.regiones.append(list(region))   
        df_regiones = pd.DataFrame(self.regiones, columns=column_names)
        return df_regiones
    
    def getPoblacionTotal(self):
        return self.poblacionTotal

    def getDataComunas(self):
        return self.comunas

    def getDataRegiones(self):
        return self.regiones