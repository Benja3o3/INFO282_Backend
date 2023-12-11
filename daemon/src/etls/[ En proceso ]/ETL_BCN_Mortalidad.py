import pandas as pd
from src.calculo.utils import getDateFile, getDimension, getLastFile, dataNormalize
from sqlalchemy.sql import text
from psycopg2 import sql
from datetime import datetime


class ETL_Transactional:
    def __init__(self, querys, localidades):

        self.fuente = "BCN_Tasa_Mortalidad"
        self.dimension = "Salud"
        self.tableName = "data_" + self.fuente
        
        # << No modificar >>
        self.FOLDER = "Source/" + self.fuente + "/"
        self.PATH = getLastFile(self.FOLDER)
        self.uploadDate = getDateFile(self.PATH)
        self.localidades = localidades
        self.extractedData = None
        self.querys = querys
        # << No modificar <<

    def __string__(self):
        return str(self.nombreData)

    def Extract(self):
        self.extractedData = pd.read_csv(self.PATH, delimiter="\t", na_values='-', encoding='utf-8-sig')
        print (self.extractedData)

    def Tranform(self, comunas, conflictNames):
        dataToLoad = []
        self.extractedData['Unidad territorial'] = self.extractedData['Unidad territorial'].str.lower()

        print(self.extractedData)
        
        # for _, comuna in comunas.iterrows():
        #     # self.extractedData = self.extractedData[["Unidad territorial", " 2020"]]
        #     comunaData = self.extractedData[(self.extractedData['Unidad territorial']).str.contains(comuna["nombre"].lower())]
        #     self.extractedData[:, -1] = self.extractedData[:, -1].str.replace(r'\s+', '', regex=True)
        #     print(self.extractedData)

        #     if(comunaData.empty):
        #         comunaData = self.extractedData[(self.extractedData['Unidad territorial']).str.contains(conflictNames[comuna["nombre"]].lower())]
        #     valor = 0
        #     variable = ""
        #     try:
        #         variable = str(comunaData[' Variable'].iloc[0])
        #         valor = float(comunaData[" 2020"].iloc[0])
        #     except:
        #         print("No existe información de: ", comuna['nombre'])

        #     data = {
        #         "variable": variable,
        #         "tasa_mortalidad": valor,
        #         "fecha" : self.uploadDate,
        #         "flag" : True,
        #         "comuna_id": comuna['comuna_id'],
        #         "dimension_id": getDimension(self.dimension)              
        #         }
        #     # dataToLoad.append(data)
        return(dataToLoad)
    
    def Load(self, data):
        self.querys.loadFileTransactional(self.tableName, data)

    def ETLProcess(self):
        conflictNames = {
            "Ranquil": "Ránquil", 
            "Los Alamos": "Los Álamos", 
            "Paihuano": "Paiguano",
            "Marchigüe": "Marchihue",
            "Los Angeles": "Los Ángeles",
            "O'Higgins": "O’higgins"
        }
        maxDate = self.querys.getMaxDate(self.tableName) 
        try:
            if True:
            # if maxDate == None or self.uploadDate > maxDate:
                self.Extract()
                print("DATA")
                self.querys.updateFlagFuente(self.tableName)
                comunas = self.localidades.getDataComunas()
                data = self.Tranform(comunas, conflictNames)
                self.Load(data)
                print("Data cargada...")
            else:
                print("Datos en bruto ya actualizados: ", self.fuente)
                return True  # Ya actualizados 
            return False     # No actualizados
        except KeyError as error:
            print("ERRROR", error)
    
## -------------------------------------- ##
## -------------------------------------- ##
## -------------------------------------- ##

# class ETL_Processing:
#     def __init__(self, querys, localidades):
#         # Para la base de datoss
#         self.fuente = "BCN_Mortalidad"              
#         self.nombreIndicador = "Numero de camaras"
        
#         # informacion indicador
#         self.indicador_id = 8  ## Valor numerico, revisar si no existe en bd
#         self.dimension = "Seguridad"
#         self.prioridad = 1
#         self.url =  "https://www.bcn.cl/siit/estadisticasterritoriales//descargar-resultados/258480/datos.csv"
#         self.descripcion = "Indicador asociado al numero de camaras de vigilancia que hay en una comuna"
        
#         # << No modificar >>
#         self.tableName = "data_" + self.fuente
#         self.dimension = getDimension(self.dimension)
#         self.localidades = localidades
#         self.transaccionalData = None
#         self.querys = querys
#         # << No modificar >>  

#     def __string__(self):
#         return str(self.nombreIndicador)

#     def Extract(self):
#         self.transaccionalData = self.querys.getTransactionalData(self.tableName)
        
#     def Transform(self, comuna):
#         df = self.transaccionalData
#         df_merged = df.merge(comuna, left_on='comuna_id', right_on='comuna_id', how='right')
#         df_merged['valor'] = df_merged['camaras'] / comuna['poblacion']
#         data = df_merged[['comuna_id', 'valor', 'dimension_id']]
        
#         normalized = dataNormalize(data)
#         return normalized
 
#     def Load(self, data):    
#         all_data = [] 
#         for _, values in data.iterrows():
#             valor = values['valor']
#             comuna_id = values['comuna_id']
#             if pd.isnull(valor):
#                 valor = 0
#             data = {
#                 "valor": valor,
#                 "fecha" : datetime.now().date(),
#                 "flag" : True,
#                 "dimension_id" : self.dimension,
#                 "comuna_id" : comuna_id,
#                 "indicador_id": self.indicador_id
#             }
#             all_data.append(data)
#         self.querys.loadDataProcessing(all_data)
        
#     def addETLinfo(self):
#         data = {
#             "indicadoresinfo_id": self.indicador_id,
#             "nombre": self.nombreIndicador,
#             "prioridad": self.prioridad,
#             "descripcion": self.descripcion,
#             "fuente": self.url,
#             "dimension": self.dimension
#         }
#         self.querys.addIndicatorsInfo(data)

#     def ETLProcess(self):
#         try:
#             self.addETLinfo()
#             self.Extract()
#             self.querys.updateFlagProcessing(self.indicador_id)
#             comunas = self.localidades.getDataComunas()
#             data = self.Transform(comunas)
#             self.Load(data)

#         except Exception as error:
#             print(error)
#         return {"OK": 200, "mesagge": "Indicators is updated successfully"}
