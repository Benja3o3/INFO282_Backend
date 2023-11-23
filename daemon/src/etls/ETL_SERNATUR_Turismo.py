import pandas as pd
from src.calculo.utils import getDateFile, getDimension, getLastFile, dataNormalize, createFolderNoProcesado, getExtension, getDateTimeFile
from sqlalchemy.sql import text
from psycopg2 import sql
from datetime import datetime


class ETL_Transactional:
    def __init__(self, querys, localidades):

        self.fuente = "SERNATUR_Turismo"
        self.dimension = "Economico"
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

    def addLog(self, error = ""):
        print("Creando log...")
        if(error == ""):
            estado = "Procesado"
        else:
            estado = "No procesado"
        filename = getLastFile(self.FOLDER)
        self.querys.addFileToLog({
            "fecha": getDateTimeFile(filename),
            "nombre_archivo": filename,
            "tipo_archivo": getExtension(filename),
            "error": error,
            "estado": estado
        })
        print("Log creado correctamente.")
        
    def Extract(self):
        self.extractedData = pd.read_csv(self.PATH, delimiter=";", encoding="ISO-8859-1")

    def Tranform(self, comunas):
        dataToLoad = []
        añoMax = self.extractedData['Anio '].max()
        self.extractedData = self.extractedData[self.extractedData['Anio '] == añoMax]

        
        self.extractedData['Ene '] = self.extractedData['Ene '].str.replace(',', '.')
        self.extractedData['Feb '] = self.extractedData['Feb '].str.replace(',', '.')
        self.extractedData['Mar '] = self.extractedData['Mar '].str.replace(',', '.')
        self.extractedData['Abr '] = self.extractedData['Abr '].str.replace(',', '.')
        self.extractedData['May '] = self.extractedData['May '].str.replace(',', '.')
        self.extractedData['Jun '] = self.extractedData['Jun '].str.replace(',', '.')
        self.extractedData['Jul '] = self.extractedData['Jul '].str.replace(',', '.')
        self.extractedData['Ago '] = self.extractedData['Ago '].str.replace(',', '.')
        self.extractedData['Sep '] = self.extractedData['Sep '].str.replace(',', '.')
        self.extractedData['Oct '] = self.extractedData['Oct '].str.replace(',', '.')
        self.extractedData['Nov '] = self.extractedData['Nov '].str.replace(',', '.')
        self.extractedData['Dic '] = self.extractedData['Dic '].str.replace(',', '.')

        for _, comuna in comunas.iterrows():
            comunaData = self.extractedData[self.extractedData['CUT Comuna Origen '] == comuna["comuna_id"]]
            for _, row in comunaData.iterrows():
                try:
                    data = {
                        "cut_comuna_origen": int(row["CUT Comuna Origen "]),
                        "cut_comuna_destino": int(row["CUT Comuna Destino "]) ,
                        "cut_provincia_origen": int(row["CUT Provincia Origen "]),
                        "cut_provincia_destino": int(row["CUT Provincia Destino "]),
                        "año": int(row["Anio "]),
                        "Ene": float(row["Ene "]),
                        "Feb": float(row["Feb "]),
                        "Mar": float(row["Mar "]),
                        "Abr": float(row["Abr "]),
                        "May": float(row["May "]),
                        "Jun": float(row["Jun "]),
                        "Jul": float(row["Jul "]),
                        "Ago": float(row["Ago "]),
                        "Sep": float(row["Sep "]),
                        "Oct": float(row["Oct "]),
                        "Nov": float(row["Nov "]),
                        "Dic": float(row["Dic "]),
                        "fecha" : self.uploadDate,
                        "flag" : True,
                        "comuna_id": comuna['comuna_id'],
                        "dimension_id": getDimension(self.dimension)              
                    }                   
                    dataToLoad.append(data)                    
                     
                except Exception as error:
                    print("Error al transformar datos ETL_Establecimientos")    
        print("FINISH")
        return(dataToLoad)
    
    def Load(self, data):
        self.querys.loadFileTransactional(self.tableName, data)

    def ETLProcess(self):
        maxDate = self.querys.getMaxDate(self.tableName) 
        try:
            if maxDate == None or self.uploadDate > maxDate:
                self.Extract()
                self.querys.updateFlagFuente(self.tableName)
                comunas = self.localidades.getDataComunas()
                data = self.Tranform(comunas)
                self.Load(data)
                self.addLog()
            else:
                print("Datos en bruto ya actualizados: ", self.fuente)
                return True  # Ya actualizados 
            return False     # No actualizados
        except Exception as error:
            self.addLog(str(error))
            createFolderNoProcesado(self.PATH, self.FOLDER)
            pass
    
## -------------------------------------- ##
## -------------------------------------- ##
## -------------------------------------- ##

class ETL_Processing:
    def __init__(self, querys, localidades):
        # Para la base de datos
        self.fuente = "SERNATUR_Turismo"              
        self.nombreIndicador = "Turismo intercomunal"
        
        # informacion indicador
        self.indicador_id = 12  ## Valor numerico, revisar si no existe en bd
        self.dimension = "Economico"
        self.prioridad = 1
        self.url =  "https://www.sernatur.cl/dataturismo/big-data-turismo-interno/"
        self.descripcion = "Indicador asociado al turismo interneto del pais"
        
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
        cols = ['ene', 'feb', 'mar', 'abr', 'may', 'jun', 'jul','ago', 'sep', 'oct', 'nov', 'dic']
        df_merged['valor'] = df_merged[cols].sum(axis=1)/6
        df_merged['comuna_id'] = df_merged['cut_comuna_destino']
        data = df_merged[['comuna_id', 'valor', 'dimension_id','poblacion']]
        data = data.groupby('comuna_id')['valor'].sum().reset_index()
        
        data = data.merge(comuna, left_on='comuna_id', right_on='comuna_id', how='right')
        data['valor'] = data['valor'] / comuna['poblacion']
        data['dimension_id'] = self.dimension
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
