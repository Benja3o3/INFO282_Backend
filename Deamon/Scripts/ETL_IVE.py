import pandas as pd
from sqlalchemy.sql import text
import traceback
from datetime import datetime

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
        query = text("INSERT INTO dataenbruto (valor, nombre, fuente, fecha, comuna_id) VALUES (:valor, :nombre, :fuente, :fecha, :comuna_id)")
        values = {
            'valor': float(self.valor),
            'nombre': self.nombre,
            'fuente': self.fuente,
            'fecha': datetime.now().date(),
            'comuna_id': comuna['CUT']
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
                self.Load(comuna)
        except KeyError as error:
            print(error)

        return {"OK": 200, "mesagge": "Indicators is updated successfully"}


class ETL_Processing:
    def __init__(self, dbTransaccional, dbProcessing, localidades):
        #Constructor
        self.fuente = "JUNAEB: IVE"
        self.nombreIndicador = "IVE"        
        self.valor = 0
    

        #Constructor
        self.dbTransaccional = dbTransaccional
        self.dbProcessing = dbProcessing
        self.localidades = localidades

        #Data
        self.transaccionalData = None

    def __string__(self):
        return str(self.nombreIndicador)

    def Extract(self):
        #Codigo generico, no tocar
        query = text("SELECT * FROM dataenbruto WHERE fuente = :fuente")
        query = query.bindparams(fuente=self.fuente)
        result = []
        with self.dbTransaccional.connect() as con:
            result = con.execute(query)
            rows = result.fetchall()
            column_names = result.keys()
        df = pd.DataFrame(columns=column_names)
        for row in rows:
            row_dict = {column_name: value for column_name, value in zip(column_names, row)}
            df = pd.concat([df, pd.DataFrame(row_dict, index=[0])], ignore_index=True)
        self.transaccionalData = df
        max_date = self.transaccionalData['fecha'].max()
        self.transaccionalData = self.transaccionalData[self.transaccionalData['fecha'] == max_date]
        return 

    def Tranform(self, comuna):
        # Calculo indicador IVE
        df = self.transaccionalData[self.transaccionalData['comuna_id'] == comuna['CUT']]
        if(df.empty):
            self.valor = 0
        self.valor = df["valor"].tail(1).iloc[0] * 100
        return
        
    def Load(self, comuna):
        query = text("INSERT INTO indicador (nombre, prioridad, fuente, valor, fecha, dimension_id) VALUES (:nombre, :prioridad, :fuente, :valor, :fecha ,:dimension_id)")
        values = {
            'nombre': self.nombre,
            'prioridad': self.prioridad,
            'fuente': self.fuente,
            'valor': float(self.valor),
            'fecha': datetime.now().date(),
            'comuna_id': comuna['CUT']
        }
        with self.dbProcessing.connect() as con:
            con.execute(query, values)
            con.commit()  

    def ETLProcess(self):
        try:
            self.Extract()
            comunas = self.localidades.getComunas()
            for _, comuna in comunas.iterrows():
                self.Tranform(comuna)
            #     self.Load(comuna)
        except Exception:
            traceback.print_exc()
        return {"OK": 200, "mesagge": "Indicators is updated successfully"}


