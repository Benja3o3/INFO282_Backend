# import pandas as pd
# from src.calculo.utils import getDateFile, getDimension, getLastFile
# from sqlalchemy.sql import text
# from psycopg2 import sql
# from datetime import datetime

# class ETL_Transactional:
#     def __init__(self, querys, localidades):
#         self.fuente = "CEM_Establecimientos"
#         self.dimension = "Educacional"
#         self.tableName = "data_" + self.fuente
        
#         # << No modificar >>
#         self.FOLDER = "Source/" + self.fuente + "/"
#         self.PATH = getLastFile(self.FOLDER)
#         self.uploadDate = getDateFile(self.PATH)
#         self.localidades = localidades
#         self.extractedData = None
#         self.querys = querys
#         # << No modificar <<

#     def __string__(self):
#         return str(self.nombreData)

#     def Extract(self):
#         self.extractedData = pd.read_csv(self.PATH, delimiter=";")

#     def Tranform(self, comunas):
#         dataToLoad = []
#         for _, comuna in comunas.iterrows():
#             self.extractedData = self.extractedData[["Codigo comuna", "conectividad"]]
#             comunaData = self.extractedData[self.extractedData['Codigo comuna'] == comuna["comuna_id"]]
#             try:
#                 conectividad = int(comunaData["conectividad"].iloc[0])
#                 data = {
#                     "conectividad": conectividad,
#                     "fecha" : self.uploadDate,
#                     "flag" : True,
#                     "comuna_id": comuna['comuna_id'],
#                     "dimension_id": getDimension(self.dimension)              
#                   }
#                 dataToLoad.append(data)
#             except:
#                 conectividad = 0
#                 print("No existe información de: ", comuna['nombre'])
          
#         return(dataToLoad)
    
#     def Load(self, data):
#         self.querys.loadFileTransactional(self.tableName, data)

#     def ETLProcess(self):
#         maxDate = self.querys.getMaxDate(self.tableName) 
#         try:
#             if maxDate == None or self.uploadDate > maxDate:
#                 self.Extract()
#                 self.querys.updateFlagFuente(self.tableName)
#                 comunas = self.localidades.getDataComunas()
#                 data = self.Tranform(comunas)
#                 self.Load(data)
#             else:
#                 print("Datos en bruto ya actualizados: ", self.fuente)
#                 return True  # Ya actualizados 
#             return False     # No actualizados
#         except Exception as error:
#             print(error)