import pandas as pd
from src.calculo.utils import getDateFile, getDimension, getLastFile, dataNormalize, createFolderNoProcesado, getDateTimeFile, getExtension
from sqlalchemy.sql import text
from psycopg2 import sql
from datetime import datetime

# SUP_VEGATA No tiene datos 

class ETL_Transactional:
    def __init__(self, querys, localidades):

        self.fuente = "MINVU_Areas_Verdes"
        self.dimension = "Ecologico"
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
        self.extractedData = pd.read_csv(self.PATH, delimiter=",")

    def Tranform(self, comunas):
        dataToLoad = []
        self.extractedData = self.extractedData[['CUT','SUP_TOTAL_','TIPO_EP', "Shape__Area"]]
        # exit()
        for _, comuna in comunas.iterrows():
            comunaData = self.extractedData[(self.extractedData['CUT'] == comuna['comuna_id'])]
            for _, row in comunaData.iterrows():
                superficieTotal = 0
                shapeArea = 0
                try:
                    superficieTotal = float(row["SUP_TOTAL_"])
                    shapeArea = float(row["Shape__Area"])
                    tipoEP = row["TIPO_EP"]
                except KeyError as e:
                    print("No existe información de: ", comuna['nombre'])
                data = {
                    "sup_total": superficieTotal,
                    "tipo_ep": tipoEP,
                    "shape_area": shapeArea,
                    "fecha" : self.uploadDate,
                    "flag" : True,
                    "comuna_id": comuna['comuna_id'],
                    "dimension_id": getDimension(self.dimension)              
                    }
                dataToLoad.append(data)
        return(dataToLoad)
    
    def Load(self, data):
        self.querys.loadFileTransactional(self.tableName, data)

    def ETLProcess(self):
        maxDate = self.querys.getMaxDate(self.tableName) 
        try:
            # if True:
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
        self.fuente = "MINVU_Areas_Verdes"              
        self.nombreIndicador = "Minvu_Areas_Verdes"
        
        # informacion indicador
        self.indicador_id = 10  ## Valor numerico, revisar si no existe en bd
        self.dimension = "Ecologico"
        self.prioridad = 1
        self.url =  "https://opendata.arcgis.com/api/v3/datasets/864ec380054143228818c6a4ccbbf570_0/downloads/data?format=csv&spatialRefId=4326&where=1%3D1"
        self.descripcion = "Indicador asociado a la suma de las superficies de las areas verdes por ciudad"
        
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
        #Revisar
        df = self.transaccionalData
        df_merged = df.merge(comuna, left_on='comuna_id', right_on='comuna_id', how='right')
        df_merged = df_merged[['comuna_id','dimension_id','sup_total']]
        sup_total_comunas =  df_merged.groupby('comuna_id')['sup_total'].sum().reset_index().drop_duplicates(subset="comuna_id")
        df_merged = df_merged.drop_duplicates(subset="comuna_id").reset_index()
        df_merged.loc[:, "valor"] = sup_total_comunas['sup_total'] / comuna['poblacion']
        data = df_merged[['comuna_id', 'valor', 'dimension_id']]
        data.loc[:, 'valor'] = data['valor'].fillna(0)
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
