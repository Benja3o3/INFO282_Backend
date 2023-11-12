import pandas as pd
from src.calculo.utils import getDateFile, getDimension, getLastFile, dataNormalize, createFolderNoProcesado
from sqlalchemy.sql import text
from psycopg2 import sql
from datetime import datetime


class ETL_Transactional:
    def __init__(self, querys, localidades):

        self.fuente = "SISS_Agua_potable"
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
        self.extractedData = pd.read_csv(self.PATH, delimiter=",")

    def Tranform(self, comunas, conflictNames):
        dataToLoad = []
        
        self.extractedData['Unidad territorial'] = self.extractedData['Unidad territorial'].str.lower()

        for _, comuna in comunas.iterrows():
            comunaData = self.extractedData[(self.extractedData['Unidad territorial']).str.contains(comuna["nombre"].lower())]
            if(comunaData.empty):
                comunaData = self.extractedData[(self.extractedData['Unidad territorial']).str.contains(conflictNames[comuna["nombre"]].lower())]

            try:
                variable = str(comunaData[' Variable'].iloc[0])
                cobertura = float(comunaData.iloc[:, -1].iloc[0])
                data = {
                    "variable": variable,
                    "cobertura": cobertura,
                    "fecha" : self.uploadDate,
                    "flag" : True,
                    "comuna_id": comuna['comuna_id'],
                    "dimension_id": getDimension(self.dimension)              
                  }
                dataToLoad.append(data)
            except:
                print("No existe información de: ", comuna['nombre'])
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
            if maxDate == None or self.uploadDate > maxDate:
                self.Extract()
                self.querys.updateFlagFuente(self.tableName)
                comunas = self.localidades.getDataComunas()
                data = self.Tranform(comunas, conflictNames)
                self.Load(data)
            else:
                print("Datos en bruto ya actualizados: ", self.fuente)
                return True  # Ya actualizados 
            return False     # No actualizados
        except Exception as error:
            createFolderNoProcesado(self.PATH, self.FOLDER)
            print(error)
    
## -------------------------------------- ##
## -------------------------------------- ##
## -------------------------------------- ##

class ETL_Processing:
    def __init__(self, querys, localidades):
        # Para la base de datos
        self.fuente = "SISS_Agua_potable"              
        self.nombreIndicador = "Cobertura del agua potable en base a la poblacion comunal"
        
        # informacion indicador
        self.indicador_id = 7  ## Valor numerico, revisar si no existe en bd
        self.dimension = "Salud"
        self.prioridad = 1
        self.url =  ""
        self.descripcion = "Indica el porcentaje de la cobertura del agua potable en base a la poblacion"
        
        # << No modificar >>
        self.tableName = "data_" + self.fuente
        self.dimension = getDimension(self.dimension)
        self.localidades = localidades
        self.transaccionalData = None
        self.querys = querys
        # << No modificar >>  

    def __string__(self):
        return str(self.nombreIndicador)

    def Extract(self):
        self.transaccionalData = self.querys.getTransactionalData(self.tableName)
        
    def Transform(self, comuna):
        df = self.transaccionalData
        df_merged = df.merge(comuna, left_on='comuna_id', right_on='comuna_id', how='right')
        df_merged['valor'] = df_merged['cobertura'] / comuna['poblacion']
        data = df_merged[['comuna_id', 'valor', 'dimension_id']]
        
        normalized = dataNormalize(data)
        return normalized
 
    def Load(self, data):    
        all_data = [] 
        for _, values in data.iterrows():
            valor = values['valor']
            comuna_id = values['comuna_id']
            if pd.isnull(valor):
                valor = 0
            data = {
                "valor": valor,
                "fecha" : datetime.now().date(),
                "flag" : True,
                "dimension_id" : self.dimension,
                "comuna_id" : comuna_id,
                "indicador_id": self.indicador_id
            }
            all_data.append(data)
        self.querys.loadDataProcessing(all_data)
        
    def addETLinfo(self):
        data = {
            "indicadoresinfo_id": self.indicador_id,
            "nombre": self.nombreIndicador,
            "prioridad": self.prioridad,
            "descripcion": self.descripcion,
            "fuente": self.url,
            "dimension": self.dimension
        }
        self.querys.addIndicatorsInfo(data)

    def ETLProcess(self):
        try:
            self.addETLinfo()
            self.Extract()
            self.querys.updateFlagProcessing(self.indicador_id)
            comunas = self.localidades.getDataComunas()
            data = self.Transform(comunas)
            self.Load(data)

        except Exception as error:
            print(error)
        return {"OK": 200, "mesagge": "Indicators is updated successfully"}
