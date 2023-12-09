import pandas as pd
from src.calculo.utils import getDateFile, getDimension, getLastFile, dataNormalize, createFolderNoProcesado, getDateTimeFile, getExtension
from sqlalchemy.sql import text
from psycopg2 import sql
from datetime import datetime


class ETL_Transactional:
    def __init__(self, querys, localidades):

        self.fuente = "BCN_Mortalidad"
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
        self.extractedData = pd.read_csv(self.PATH, delimiter=",")
        
    def Tranform(self, comunas, conflictNames):
        dataToLoad = []
        for _, comuna in comunas.iterrows():
            
            
            comunaData = self.extractedData[self.extractedData['Unidad  '].str.lower().str.contains(comuna["nombre"].lower())]
            if(comunaData.empty):
                comunaData = self.extractedData[self.extractedData['Unidad  '].str.contains(conflictNames[comuna["nombre"]], regex=False)]
            comunaData.loc[:, ' Variable'] = comunaData[' Variable'].str.strip('\xa0 ')


            try:            
                mortalidad_general = comunaData[ comunaData[' Variable'] == "Tasa de Mortalidad General Total (Por mil Habitantes)"].iloc[:, -1].values[0]
                mortalidad_general = (float(mortalidad_general)/1000) * comuna['poblacion']
            except:
                mortalidad_general = 0
                
            try:
                mortalidad_hombres = comunaData[ comunaData[' Variable'] == "Tasa de Mortalidad General Hombre (Por mil Hombres)"].iloc[:, -1].values[0]
                mortalidad_hombres = (float(mortalidad_hombres)/1000) * comuna['poblacion']
            except:
                mortalidad_hombres = 0    
            
            try:
                mortalidad_mujeres = comunaData[ comunaData[' Variable'] == "Tasa de Mortalidad General Mujer (Por mil Mujeres)"].iloc[:, -1].values[0]
                mortalidad_mujeres = (float(mortalidad_mujeres)/1000) * comuna['poblacion']
            except:
                mortalidad_mujeres = 0
                
            data = {
                "mortalidad_general": mortalidad_general,
                "mortalidad_hombre": mortalidad_hombres,
                "mortalidad_mujer": mortalidad_mujeres,
                
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
        
        conflictNames = {
            "O'Higgins": "O’higgins", 
            'Los Alamos' : 'Los Álamos',
            'Ranquil':'Ránquil (Región del Ñuble)',
            'Marchigüe': 'Marchihue',
            'Paihuano': 'Paiguano',
            'Los Angeles' : 'Los Ángeles',
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
        self.fuente = "BCN_Mortalidad"              
        
        # Mortalidad general
        self.nombreIndicadorA = "Tasa de mortalidad"
        self.indicador_idA = 26  ## Valor numerico, revisar si no existe en bd
        self.dimensionA = getDimension("Salud")
        self.prioridadA = 1
        self.urlA =  "https://www.bcn.cl/siit/estadisticasterritoriales/tema?id=57"
        self.descripcionA = "Indicador que representa la tasa de mortalidad respecto a la poblacion (1 - tasa)"

        # Mortalidad mujeres
        self.nombreIndicadorB = "Tasa de mortalidad en Mujeres"
        self.indicador_idB = 27  ## Valor numerico, revisar si no existe en bd
        self.dimensionB = getDimension("Salud")
        self.prioridadB = 1
        self.urlB =  "https://www.bcn.cl/siit/estadisticasterritoriales/tema?id=57"
        self.descripcionB = "Indicador que respresenta la tasa mortalidad de mujeres respecto al total de asesinatos (1 - tasa)"
        
  
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
        # indicador reprobados / (aprobados + reprobados + retirados)
        df = self.transaccionalData
        df_merged = df.merge(comuna, left_on='comuna_id', right_on='comuna_id', how='right')
        df_merged.loc[:, 'valor'] = 1 - df_merged['mortalidad_general'] / comuna['poblacion']
        data = df_merged[['comuna_id', 'valor', 'dimension_id']]
        
        normalized = dataNormalize(data)
        return normalized
    
         
    def TransformB(self, comuna):
        # indicador reprobados / (aprobados + reprobados + retirados)
        df = self.transaccionalData
        df_merged = df.merge(comuna, left_on='comuna_id', right_on='comuna_id', how='right')
        df_merged.loc[:, 'valor'] = 1 -  df_merged['mortalidad_mujer'] / df_merged['mortalidad_general']
        data = df_merged[['comuna_id', 'valor', 'dimension_id']]
        
        
        
        normalized = dataNormalize(data)
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
            # self.addETLinfo(self.indicador_idB, self.nombreIndicadorB, self.prioridadB, self.descripcionB, self.urlB, self.dimensionB)

            self.Extract()
            self.querys.updateFlagProcessing(self.indicador_idA)
            # self.querys.updateFlagProcessing(self.indicador_idB)

            comunas = self.localidades.getDataComunas()
            dataA = self.TransformA(comunas)
            self.Load(dataA, self.dimensionA, self.indicador_idA)
            
            # dataB = self.TransformB(comunas)
            # self.Load(dataB, self.dimensionB, self.indicador_idB)

        except Exception as error:
            print(error)
        return {"OK": 200, "mesagge": "Indicators is updated successfully"}
