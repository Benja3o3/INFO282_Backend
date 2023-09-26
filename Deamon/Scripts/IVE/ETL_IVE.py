import pandas as pd
from sqlalchemy.sql import text

class ETL:
    def __init__(self, db, localidades):
        self.fuente = "JUNAEB: IVE"
        self.nombre = "IVE"        
        self.valor = 0
        self.PATH = "Source/IVE/IVE-2023.xlsx"

        self.extractedData = None
        self.db = db
        self.localidades = localidades
        

    def __string__(self):
        return str(self.nombre)

    def Extract(self):

        self.extractedData = pd.read_excel(self.PATH, sheet_name="COMUNA", header=3)
        self.extractedData = self.extractedData[
            ["ID_COMUNA_ESTABLE", self.extractedData.columns[-1]]
        ]
        self.extractedData = self.extractedData.dropna()
        self.extractedData["ID_COMUNA_ESTABLE"] = self.extractedData[
            "ID_COMUNA_ESTABLE"
        ].astype(int)

    def Tranform(self, comuna):
        # Transforma datos para ser subidos a database
        df = self.extractedData[self.extractedData['ID_COMUNA_ESTABLE'] == comuna['CUT']]
        if(df.empty):
            self.valor = 0
        self.valor = df.iloc[-1, -1]
        
    def Load(self, comuna):
        query = text("INSERT INTO dataenbruto (valor, nombre, fuente, comuna_id) VALUES (:valor, :nombre, :fuente, :comuna_id)")
        values = {
            'valor': self.valor,
            'nombre': self.nombre,
            'fuente': self.fuente,
            'comuna_id': comuna['CUT']
        }
        with self.db.connect() as con:
            con.execute(query, values)
            con.commit()  

    def ETLProcess(self):
        self.Extract()
        
        comunas = self.localidades.getComunas()
        for _, comuna in comunas.iterrows():
            self.Tranform(comuna)
            self.Load(comuna)

        return {"OK": 200, "mesagge": "Indicators is updated successfully"}


