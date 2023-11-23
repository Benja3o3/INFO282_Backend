import pandas as pd
from src.calculo.utils import getDateFile, getDimension, getLastFile, dataNormalize, createFolderNoProcesado, getDateTimeFile, getExtension
from sqlalchemy.sql import text
from psycopg2 import sql
from datetime import datetime

class ETL_Transactional:
    def __init__(self, querys, localidades):

        self.fuente = "CEM_Establecimientos"
        self.dimension = "Educacional"
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
        self.extractedData = pd.read_csv(self.PATH, delimiter=";")

    def Tranform(self, comunas):
        dataToLoad = []
        self.extractedData = self.extractedData[["COD_COM_RBD", "NOM_RBD", "COD_DEPE2", "RURAL_RBD", "LATITUD", "LONGITUD", "PAGO_MATRICULA", "PAGO_MENSUAL"]]
        for _, comuna in comunas.iterrows():
            comunaData = self.extractedData[self.extractedData['COD_COM_RBD'] == comuna["comuna_id"]]
            for _, row in comunaData.iterrows():
                try:
                    nombre_establecimiento = str(row["NOM_RBD"])
                    dependencia = int(row["COD_DEPE2"])
                    ruralidad = int(row["RURAL_RBD"])
                    latitud = str(row["LATITUD"])
                    longitud = str(row["LONGITUD"])
                    pago_matricula = str(row["PAGO_MATRICULA"]) 
                    pago_mensual = str(row["PAGO_MENSUAL"]) 
                    data = {
                        "nombre_establecimiento": nombre_establecimiento,
                        "dependencia": dependencia,
                        "ruralidad": ruralidad,
                        "latitud": latitud, 
                        "longitud": longitud,
                        "pago_matricula": pago_matricula,
                        "pago_mensual": pago_mensual,
                        "fecha" : self.uploadDate,
                        "flag" : True,
                        "comuna_id": comuna['comuna_id'],
                        "dimension_id": getDimension(self.dimension)              
                    }
                    dataToLoad.append(data)                    
                except:
                    print("Error al transformar datos ETL_Establecimientos")    
          
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
            print(error)
    
## -------------------------------------- ##
## -------------------------------------- ##
## -------------------------------------- ##

class ETL_Processing:
    def __init__(self, querys, localidades):
        # Para la base de datos
        self.fuente = "CEM_Establecimientos"              
        
        # indicador % establecimientos por cantidad por poblacion
        self.nombreIndicador = "Cantidad de establecimientos"
        self.indicador_id = 2  ## Valor numerico, revisar si no existe en bd
        self.dimension_id = getDimension("Educacional")
        self.prioridad = 1
        self.url =  "https://datosabiertos.mineduc.cl/directorio-de-establecimientos-educacionales/"
        self.descripcion = "Porcentaje de establecimientos educacionales respecto a la cantidad de población"
        
        # cantidad colegios municipales por poblacion
        self.nombreIndicador_B = "Cantidad de establecimientos municipales"
        self.indicador_id_B = 3  ## Valor numerico, revisar si no existe en bd
        self.dimension_id_B = getDimension("Economico")
        self.prioridad_B = 1
        self.url_B =  "https://datosabiertos.mineduc.cl/directorio-de-establecimientos-educacionales/"
        self.descripcion_B = "Establecimientos educacionales por poblacion"
        
        
        # << No modificar >>
        self.tableName = "data_" + self.fuente
        self.localidades = localidades
        self.transaccionalData = None
        self.querys = querys
        # << No modificar >>  

    def __string__(self):
        return str(self.nombreIndicador)

    def Extract(self):
        self.transaccionalData = self.querys.getTransactionalData(self.tableName)
    def TransformCantidadColegios(self, comuna):
        df = self.transaccionalData
        df_merged = df.merge(comuna, left_on='comuna_id', right_on='comuna_id', how='right')
        data = df_merged['comuna_id'].value_counts()
        data = data.reset_index()
        data.columns = ['comuna_id', 'valor']
        data['dimension_id'] = self.dimension_id
        data['valor'] = data['valor'] / (comuna['poblacion']/self.localidades.getPoblacionTotal())
        
        normalized = dataNormalize(data)
        return normalized
    
    def TransformMunicipal(self, comuna):
        df = self.transaccionalData
        df_merged = df.merge(comuna, left_on='comuna_id', right_on='comuna_id', how='right')
        df_merged = df_merged[df_merged["dependencia"] == 1]
        data = df_merged['comuna_id'].value_counts()
        data = data.reset_index()
        data.columns = ['comuna_id', 'valor']
        data['dimension_id'] = self.dimension_id
        data['valor'] = data['valor'] / comuna['poblacion']
        
        normalized = dataNormalize(data)
        return normalized
    
    def Load(self, data, indicador_id,  dimension_id):    
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
                "dimension_id" : dimension_id,
                "comuna_id" : comuna_id,
                "indicador_id": indicador_id
            }
            all_data.append(data)
        self.querys.loadDataProcessing(all_data)
        
    def addETLinfo(self):
        dataCantColegios = {
            "indicadoresinfo_id": self.indicador_id,
            "nombre": self.nombreIndicador,
            "prioridad": self.prioridad,
            "descripcion": self.descripcion,
            "fuente": self.url,
            "dimension": self.dimension_id
        }
        self.querys.addIndicatorsInfo(dataCantColegios)
        
        datColMunicipales = {
            "indicadoresinfo_id": self.indicador_id_B,
            "nombre": self.nombreIndicador_B,
            "prioridad": self.prioridad_B,
            "descripcion": self.descripcion_B,
            "fuente": self.url_B,
            "dimension": self.dimension_id_B
        }
        self.querys.addIndicatorsInfo(datColMunicipales)
    def ETLProcess(self):
        try:
            self.addETLinfo()
            self.Extract()
            self.querys.updateFlagProcessing(self.indicador_id)
            self.querys.updateFlagProcessing(self.indicador_id_B)
            comunas = self.localidades.getDataComunas()
            
            #Indicador cant colegios
            cantColegios = self.TransformCantidadColegios(comunas)
            self.Load(cantColegios, self.indicador_id, self.dimension_id)
            
            #Indicador % colegios municipales
            cantMunicipales = self.TransformMunicipal(comunas)
            self.Load(cantMunicipales, self.indicador_id_B, self.dimension_id_B)

        except Exception as error:
            print(error)
        return {"OK": 200, "mesagge": "Indicators is updated successfully"}
