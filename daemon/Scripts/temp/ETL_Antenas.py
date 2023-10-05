import traceback
import pandas as pd
from sqlalchemy.sql import text
from datetime import datetime

class ETL:
    def __init__(self, db, localidades):
        self.fuente = "Subtel"
        self.nombre = "Antenas"        
        self.valor = 0
        self.PATH = "Source/Subtel_antenas/Numero_de_antenas.csv"

        self.extractedData = None
        self.db = db
        self.localidades = localidades
        

    def __string__(self):
        return str(self.nombre)

    def Extract(self):

        self.extractedData = pd.read_csv(self.PATH, delimiter= ";")
        self.extractedData = self.extractedData[
            ["Codigo comuna", "conectividad"]
        ]
        self.extractedData = self.extractedData.dropna()
        self.extractedData["Codigo comuna"] = self.extractedData[
            "Codigo comuna"
        ].astype(int)

    def Tranform(self, comuna):
        # Transforma datos para ser subidos a database
        df = self.extractedData[self.extractedData['Codigo comuna'] == comuna['CUT']]
        if(df.empty):
            self.valor = 0    
            #print("[ ETL_ANTENAS ] Error comuna: " + comuna["Nombre"])
            return
        self.valor = df.iloc[-1, -1]
    def getDimension(self, dimension, comuna):
        
        return

    def Load(self, comuna):

        query = text("INSERT INTO dataenbruto (valor, nombre, fuente, fecha, dimension_id) VALUES (:valor, :nombre, :fuente, :fecha, :dimension_id)")
        values = {
            'valor': float(self.valor),
            'nombre': self.nombre,
            'fuente': self.fuente,
            'fecha': datetime.now().date(),
            'dimension_id': self.getDimension("fisica", comuna["CUT"])
        }
        with self.db.connect() as con:
            con.execute(query, values)
            con.commit()  

    def ETLProcess(self):
        try:
            self.Extract()
            
            comunas = self.localidades.getComunas()
            for _, comuna in comunas.iterrows():
                self.Tranform(comuna)
                #self.Load(comuna)
        except Exception:
            traceback.print_exc()
        return {"OK": 200, "mesagge": "Indicators is updated successfully"}


