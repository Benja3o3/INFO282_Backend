import pandas as pd
import traceback
from datetime import datetime
from sqlalchemy.sql import text
from sqlalchemy import Column, Integer, String, Boolean, Date, update, MetaData, Table
from sqlalchemy.ext.declarative import declarative_base
from utils import getDimension
from utils import getDateFile
from utils import getLastFile

import psycopg2
from psycopg2 import sql




class ETL_Transactional:
    def __init__(self, db, localidades):
        #Informacion base de datos
        self.fuente = "Subtel_antenas"  # Añadir nombre sin espacio con "_" y el nombre debe ser igual a la carpeta
        self.nombre = "Numero de antenas"

        # FILE
        self.FOLDER = "Source/" + self.fuente + "/"
        self.PATH = getLastFile(self.FOLDER)
        self.uploadDate = getDateFile(self.PATH)

        # Acceso a informacion
        self.db = db
        self.localidades = localidades
        
        # Informacion
        self.extractedData = None
        self.valor = 0

    def __string__(self):
        return str(self.nombre)

    def Extract(self):
        self.extractedData = pd.read_csv(self.PATH, delimiter=";")

    def Tranform(self, comuna):
        self.extractedData = self.extractedData[["Codigo comuna", "conectividad"]]
        comunaData = self.extractedData[self.extractedData['Codigo comuna'] == comuna["comuna_id"]]
        return comunaData    
    
    def Load(self, data):
        with self.db.connect() as conn:
            try:
                data = {
                    "comuna_id" : int(data["Codigo comuna"].iloc[0]),
                    "conectividad": int(data["conectividad"].iloc[0]),
                    "fecha" : self.uploadDate,
                    "flag" : True
                }
                print(data)
                query = text(
                    f"""
                    INSERT INTO {self.fuente} (comuna_id, conectividad, fecha, flag)
                    VALUES (:comuna_id, :conectividad, :fecha, :flag)
                    """
                )
                conn.execute(query, data)
                conn.commit()
            except:
                print("Data not found")

    def ETLProcess(self):
        with self.db.connect() as con:     
            createTableQuery = text(
                f"CREATE TABLE IF NOT EXISTS {self.fuente} ("
                " id serial PRIMARY KEY,"
                " comuna_id INT,"
                " conectividad INT,"
                " fecha Date,"
                " flag Boolean"
                ")"
            )      

            con.execute(createTableQuery)
                        
            queryFecha = text(f"SELECT MAX(fecha) FROM {self.fuente} WHERE flag=:flag")
            queryFecha = queryFecha.bindparams(flag=True)

            maxFecha = con.execute(queryFecha)
            rows = maxFecha.fetchall()
            maxFecha = rows[0][0]
            con.commit()


        if maxFecha == None or self.uploadDate > maxFecha:
            try:
                self.Extract()
                comunas = self.localidades.getComunas()
                for _, comuna in comunas.iterrows():
                    data = self.Tranform(comuna)
                    self.Load(data)
            except KeyError as error:
                print(error)
        else:
            print("Datos en bruto ya actualizados: ", self.fuente)
            return True  
        
        return False 


# class ETL_Processing:
#     def __init__(self, dbTransaccional, dbProcessing, localidades):
#         # Constructor
#         self.fuente = "Subtel: Antenas"
#         self.nombreIndicador = "Numero de antenas"
#         self.dimension = "Tecnologia"
#         self.prioridad = 1
#         self.valor = 0

#         # Constructor
#         self.dbTransaccional = dbTransaccional
#         self.dbProcessing = dbProcessing
#         self.localidades = localidades

#         # Data
#         self.transaccionalData = None

#     def __string__(self):
#         return str(self.nombreIndicador)

#     def Extract(self):
#         # Codigo generico, no tocar
#         query = text("SELECT * FROM dataenbruto WHERE fuente = :fuente")
#         query = query.bindparams(fuente=self.fuente)
#         result = []
#         with self.dbTransaccional.connect() as con:
#             result = con.execute(query)
#             rows = result.fetchall()
#             column_names = result.keys()
#         df = pd.DataFrame(columns=column_names)
#         for row in rows:
#             row_dict = {
#                 column_name: value for column_name, value in zip(column_names, row)
#             }
#             df = pd.concat([df, pd.DataFrame(row_dict, index=[0])], ignore_index=True)
#         self.transaccionalData = df
#         max_date = self.transaccionalData["fecha"].max()
#         self.transaccionalData = self.transaccionalData[
#             self.transaccionalData["fecha"] == max_date
#         ]
#         return

#     def Tranform(self, comuna):
#         # Calculo indicador IVE
#         df = self.transaccionalData[
#             self.transaccionalData["comuna_id"] == comuna["CUT"]
#         ]

#         if df.empty:
#             self.valor = 0
#         self.valor = (df["valor"].tail(1).iloc[0] / comuna["Poblacion"]) * 100
#         return

#     def Load(self, comuna):
#         query = text(
#             "INSERT INTO indicador (nombre, prioridad, fuente, valor, fecha, dimension_id) VALUES (:nombre, :prioridad, :fuente, :valor, :fecha ,:dimension_id)"
#         )
#         values = {
#             "nombre": self.nombreIndicador,
#             "prioridad": self.prioridad,
#             "fuente": self.fuente,
#             "valor": float(self.valor),
#             "fecha": datetime.now().date(),
#             "dimension_id": getDimension(
#                 self.dbProcessing, self.dimension, comuna["CUT"]
#             ),
#         }
#         with self.dbProcessing.connect() as con:
#             con.execute(query, values)
#             con.commit()

#     def ETLProcess(self):
#         try:
#             self.Extract()
#             comunas = self.localidades.getComunas()
#             for _, comuna in comunas.iterrows():
#                 self.Tranform(comuna)
#                 self.Load(comuna)
#         except Exception:
#             traceback.print_exc()

#         return {"OK": 200, "mesagge": "Indicators is updated successfully"}
