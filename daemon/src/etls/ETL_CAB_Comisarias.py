import pandas as pd
from src.calculo.utils import getDateFile, getDimension, getLastFile, dataNormalize, createFolderNoProcesado, getExtension, getDateTimeFile
from sqlalchemy.sql import text
from psycopg2 import sql
from datetime import datetime


class ETL_Transactional:
    def __init__(self, querys, localidades):

        self.fuente = "CAB_Comisarias"
        self.dimension = "Seguridad"
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
        self.extractedData = pd.read_csv(self.PATH, delimiter=";", encoding='latin-1')
        
    def Tranform(self, comunas):
        dataToLoad = []
        
        for _, comuna in comunas.iterrows():
            comunaData = self.extractedData[self.extractedData['Id_comuna'] == comuna["comuna_id"]]
            for _, row in comunaData.iterrows():
                try:
                    id_comisaria = int(row["Id_comisaria"])
                    nombre_comisaria = str(row["Nombre_comisaria"])
                    direccion = str(row["Direccion"])
                    tipo_comisaria = str(row["Tipo"])
                    
                    data = {
                        "id_comisaria": id_comisaria,
                        "nombre_comisaria": nombre_comisaria,
                        "direccion": direccion,
                        "tipo_comisaria": tipo_comisaria,
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
    
# ## -------------------------------------- ##
# ## -------------------------------------- ##
# ## -------------------------------------- ##

class ETL_Processing:
    def __init__(self, querys, localidades):
        # Para la base de datos
        self.fuente = "CAB_Comisarias"              
        self.nombreIndicador = "Cantidad de unidades por poblacion"
        
        # informacion indicador
        self.indicador_id = 8  ## Valor numerico, revisar si no existe en bd
        self.dimension = "Seguridad"
        self.prioridad = 1
        self.url =  "https://www.carabineros.cl/detalleUnidad.php"
        self.descripcion = "Alcance policial por poblacion (RETEN + COMISARIA + TENENCIA + SUBCOMISARIA)"
        
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
        df_merged = df_merged[['comuna_id', 'dimension_id']]
        data = df_merged.groupby('comuna_id').size().reset_index(name='valor')
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
