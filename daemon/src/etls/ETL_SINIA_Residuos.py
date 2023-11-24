import pandas as pd
from src.calculo.utils import getDateFile, getDimension, getLastFile, dataNormalize, createFolderNoProcesado, getDateTimeFile, getExtension
from sqlalchemy.sql import text
from psycopg2 import sql
from datetime import datetime


class ETL_Transactional:
    def __init__(self, querys, localidades):

        self.fuente = "SINIA_Residuos"
        self.dimension = "Social"
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
        self.extractedData = pd.read_excel(self.PATH, sheet_name="Res_Generación_residuos_peligro")
        
    def Tranform(self, comunas, conflictNames):
        dataToLoad = []
        for _, comuna in comunas.iterrows():            
            comunaData = self.extractedData[self.extractedData['comuna'].str.lower().str.contains(comuna["nombre"].lower())]
            if(comunaData.empty):
                try:
                    comunaData = self.extractedData[self.extractedData['comuna'].str.lower().str.contains(conflictNames[comuna["nombre"]].lower())]
                except:
                    continue
            
            for _, row in comunaData.iterrows():
                try:            
                    data = {
                        "razon_soc" : row['razón_soc'],
                        "nombre_est": row['nombre_est'],
                        "actividad": row['ciiu6'],
                        "coordenada_1": row['coordenada'],
                        "coordenada_2": row['coordena_1'],
                        "contaminante": row['contaminan'],
                        "peligro_id": row['peligrosid'],
                        "fecha" : self.uploadDate,
                        "flag" : True,
                        "comuna_id": comuna['comuna_id'],
                        "dimension_id": getDimension(self.dimension)              
                        }
                except:
                    print("No existe información de: ", comuna['nombre'])
                    
                dataToLoad.append(data)
                
        return(dataToLoad)
    
    def Load(self, data):
        self.querys.loadFileTransactional(self.tableName, data)

    def ETLProcess(self):
        maxDate = self.querys.getMaxDate(self.tableName) 
        
        conflictNames = {
            "Marchigüe": "Marchihue",
            "Coyhaique" : "Coihaique",
            "San Pedro de Atacama" : "San Pedro Atacama",
            "Ranquil" : "Ránquil",
            "Paihuano": "Paiguano"
        }
        
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
            # self.addLog(str(error))
            # createFolderNoProcesado(self.PATH, self.FOLDER)
            print(error)
    
## -------------------------------------- ##
## -------------------------------------- ##
## -------------------------------------- ##

class ETL_Processing:
    def __init__(self, querys, localidades):
        # Para la base de datos
        self.fuente = "SINIA_Residuos"              
        
        # Mortalidad general
        self.nombreIndicadorA = "Cant. campos residuales"
        self.indicador_idA = 25  ## Valor numerico, revisar si no existe en bd
        self.dimensionA = 6
        self.prioridadA = 1
        self.urlA =  "https://arcgis.mma.gob.cl/portal/apps/webappviewer/index.html?id=b4ff003778ef4a7a83a2d54693487a48"
        self.descripcionA = "Cant. de campos residuales respecto al terreno de la comuna"


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
        
    def TransformA(self, comuna):
        df = self.transaccionalData
        df_count = df['comuna_id'].value_counts().reset_index()
        df_count = df_count.rename(columns={'count': 'valor'})
        df_count.loc[:, 'valor'] = df_count['valor']/comuna['area']
        normalized = dataNormalize(df_count)
        return normalized
        
    def Load(self, data, dim, id):    
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
                "dimension_id" : dim,
                "comuna_id" : comuna_id,
                "indicador_id": id
            }
            all_data.append(data)
        self.querys.loadDataProcessing(all_data)
        
    def addETLinfo(self, id, n, p, d, u, dim):
        data = {
            "indicadoresinfo_id": id,
            "nombre": n,
            "prioridad": p,
            "descripcion": d,
            "fuente": u,
            "dimension": dim
        }
        self.querys.addIndicatorsInfo(data)

    def ETLProcess(self):
        try:
            self.addETLinfo(self.indicador_idA, self.nombreIndicadorA, self.prioridadA, self.descripcionA, self.urlA, self.dimensionA)

            self.Extract()
            self.querys.updateFlagProcessing(self.indicador_idA)

            comunas = self.localidades.getDataComunas()
            dataA = self.TransformA(comunas)
            self.Load(dataA, self.dimensionA, self.indicador_idA)


        except Exception as error:
            print(error)
        return {"OK": 200, "mesagge": "Indicators is updated successfully"}
