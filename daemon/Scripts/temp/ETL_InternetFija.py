import pandas as pd
import traceback
from datetime import datetime
from sqlalchemy.sql import text
from utils import getDimension 
from utils import getDateFile
from utils import getLastFile

class ETL_Transactional:
    def __init__(self, db, localidades):
        self.fuente = "BCN_InternetFija"
        self.nombre = "BCN_Internet"        
        self.valor = 0
        self.FOLDER = ".\Source\BNC_InternetFija"
        self.PATH = getLastFile(self.PATH)

        self.extractedData = None
        self.db = db
        self.localidadesPATH = localidades
        
    def __string__(self):
        return str(self.nombre)

    def Extract(self):
        self.extractedData = pd.read_csv(self.PATH)
        # self.extractedData = self.extractedData[self.extractedData['Variable'] == "Cantidad de Conexiones de internet fijas"]
        # print(self.extractedData)
        self.extractedData = self.extractedData[['Unidad territorial', ' 2022']]

    def Tranform(self, comuna, conflict_names):
        # Transforma datos para ser subidos a database
        df = self.extractedData[(self.extractedData['Unidad territorial']).
                                str.contains(comuna['Nombre'], case=False)]
        if(df.empty):
            self.valor = 0
            #print("[ ERROR ]" + comuna["Nombre"])  
            return         
        self.valor = df.iloc[-1, -1]
        # print(self.valor)
        
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
        query = text("SELECT MAX(fecha) FROM dataenbruto WHERE fuente = :fuente")
        query = query.bindparams(fuente=self.fuente)
        result = []
        with self.db.connect() as con:
            result = con.execute(query)
            rows = result.fetchall()
        result = rows[0][0]
        if(result == None or self.uploadDate > result):
            try:
                self.Extract()
                comunas = self.localidades.getComunas()
                for _, comuna in comunas.iterrows():
                    self.Tranform(comuna)
                    self.Load(comuna)
            except KeyError as error:
                print(error)
        else:
            print("Archivo ya actualizado")
            return True     #Si estaban procesados
        return False        #No estaban procesados

        # try:
        #     self.Extract()
            
        #     conflict_names = {
        #         # "Paihuano": "Paiguano",
        #         # "Marchigüe": "Marchihue",
        #         # "Ranquil": "Ránquil",
        #         # "Los Alamos": "Los Álamos",
        #         # "Los Angeles": "Los Ángeles",
        #         # "O'Higgins": "O’higgins"
        #         #"Arica":"Arica (Región de Arica y Parinacota)"
        #     }

        #     comunas = self.localidades.getComunas()
        #     for _, comuna in comunas.iterrows():
        #         self.Tranform(comuna, conflict_names)
        #        #self.Load(comuna)
        # except Exception:
        #     traceback.print_exc()
        # return {"OK": 200, "mesagge": "Indicators is updated successfully"}


class ETL_Processing:
    def __init__(self, dbTransaccional, dbProcessing, localidades):
        #Constructor
        self.fuente = "BNC_InternetFija"
        self.nombreIndicador = "BCN_Internet"  
        self.dimension = "Tecnologica"   
        self.prioridad = 1  
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
            'nombre': self.nombreIndicador,
            'prioridad': self.prioridad,
            'fuente': self.fuente,
            'valor': float(self.valor),
            'fecha': datetime.now().date(),
            'dimension_id': getDimension(self.dbProcessing, self.dimension, comuna["CUT"])
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
                self.Load(comuna)
        except Exception:
            traceback.print_exc()
    
        return {"OK": 200, "mesagge": "Indicators is updated successfully"}



