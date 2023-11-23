import pandas as pd
from src.calculo.utils import getDateFile, getDimension, getLastFile, dataNormalize, createFolderNoProcesado, getDateTimeFile, getExtension
from sqlalchemy.sql import text
from psycopg2 import sql
from datetime import datetime


class ETL_Transactional:
    def __init__(self, querys, localidades):

        self.fuente = "Subtel_Conexion_fija_internet"
        self.dimension = "Tecnologia"
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
        df = pd.read_excel(self.PATH, sheet_name="7.11.CO_FIJAS_COMUNA", header = 8)
        ene_columns = [col for col in df.columns if col.startswith('Ene')]
        index_columna = df.columns.get_loc(ene_columns[-1])
        subset = df.iloc[:, index_columna:]
        subset['Comuna'] = df['Comuna']
        subset = subset.dropna(subset=['Comuna'])
        subset = subset[subset['Comuna'] != 'Sin clasificación']
        nuevos_nombres = [col.split('.')[0] for col in subset.columns]
        subset.columns = nuevos_nombres
        
        self.extractedData = subset
        
    def Tranform(self, comunas, conflictNames):
        dataToLoad = []
        for _, comuna in comunas.iterrows():
            comunaData = self.extractedData[self.extractedData['Comuna'].str.contains(comuna["nombre"])]
            if(comunaData.empty):
                comunaData = self.extractedData[self.extractedData['Comuna'].str.contains(conflictNames[comuna["nombre"]])]
            for dat in comunaData.iterrows(): 
                data = {}

                for mes in ['Ene', 'Feb', 'Mar', 'Abr', 'May', 'Jun', 'Jul', 'Ago', 'Sep', 'Oct', 'Nov', 'Dic']:
                    if mes in comunaData.columns:
                        try:
                            data[mes.lower()] = float(comunaData[mes].iloc[0])
                        except (ValueError, TypeError):
                            data[mes.lower()] = None
                    else:
                        data[mes.lower()] = None
                    
                data["fecha"] = self.uploadDate
                data["flag"] = True
                data["comuna_id"] = comuna['comuna_id']
                data["dimension_id"] = getDimension(self.dimension)      

                    
                dataToLoad.append(data)
             
        return(dataToLoad)
    
    def Load(self, data):
        self.querys.loadFileTransactional(self.tableName, data)

    def ETLProcess(self):
        conflictNames = {
            'Aysén' : 'Aisén',
            'Requínoa': 'Requinoa',
            'Tiltil': 'Til Til',
            'Hualaihué': 'Hualaihue',
            'Marchigüe': 'Marchihue',
            'Máfil': 'Mafil', 
            'Alto del Carmen': 'Alto Del Carmen'
        }
        
        maxDate = self.querys.getMaxDate(self.tableName) 
        try:
            if maxDate == None or self.uploadDate > maxDate:
                self.Extract()
                self.querys.updateFlagFuente(self.tableName)
                comunas = self.localidades.getDataComunas()
                data = self.Tranform(comunas, conflictNames)
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
        self.fuente = "Subtel_Conexion_fija_internet"              
        self.nombreIndicador = "Promedio anual de conexiones de internet fija"
        
        # informacion indicador
        self.indicador_id = 6  ## Valor numerico, revisar si no existe en bd
        self.dimension = "Tecnologia"
        self.prioridad = 1
        self.url =  "https://www.subtel.gob.cl/estudios-y-estadisticas/internet/"
        self.descripcion = "Indicador asociado al numero de conexiones fijas de internet"
        
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
        cols = ['ene', 'feb', 'mar', 'abr', 'may', 'jun', 'jul', 'ago', 'sep', 'oct', 'nov', 'dic']
        
        df_merged['valor'] = df_merged[cols].mean(axis = 1)       
        df_merged = df_merged[['comuna_id', 'valor', 'dimension_id']]   
        data = df_merged.groupby('comuna_id').mean().reset_index()
        data['valor'].fillna(0, inplace=True)

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
