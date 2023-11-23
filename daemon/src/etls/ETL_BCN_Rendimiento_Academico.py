import pandas as pd
from src.calculo.utils import getDateFile, getDimension, getLastFile, dataNormalize, createFolderNoProcesado, getDateTimeFile, getExtension
from sqlalchemy.sql import text
from psycopg2 import sql
from datetime import datetime


class ETL_Transactional:
    def __init__(self, querys, localidades):

        self.fuente = "BCN_Rendimiento_academico"
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
        self.extractedData = pd.read_csv(self.PATH, delimiter=",")

    def Tranform(self, comunas, conflictNames):
        dataToLoad = []
        for _, comuna in comunas.iterrows():
            comunaData = self.extractedData[self.extractedData['Unidad territorial'].str.lower().str.contains(comuna["nombre"].lower())]
            if(comunaData.empty):
                comunaData = self.extractedData[self.extractedData['Unidad territorial'].str.contains(conflictNames[comuna["nombre"]], regex=False)]
            try:
                data = {
                    "retirados_hombres": comunaData[ comunaData[' Variable'] == " Retirados Hombres"].iloc[:, -1].values[0],
                    "retirados_mujeres": comunaData[ comunaData[' Variable'] == " Retirados Mujeres"].iloc[:, -1].values[0],
                    "retirados_total": comunaData[ comunaData[' Variable'] == " Retirados Total"].iloc[:, -1].values[0],
                    "aprobados_hombre": comunaData[ comunaData[' Variable'] == " Aprobados Hombres"].iloc[:, -1].values[0],
                    "aprobados_mujeres": comunaData[ comunaData[' Variable'] == " Aprobados Mujeres"].iloc[:, -1].values[0],
                    "aprobados_total": comunaData[ comunaData[' Variable'] == " Aprobados Total"].iloc[:, -1].values[0],
                    "reprobados_hombres": comunaData[ comunaData[' Variable'] == " Reprobados Hombres"].iloc[:, -1].values[0],
                    "reprobados_mujeres": comunaData[ comunaData[' Variable'] == " Reprobados Mujeres"].iloc[:, -1].values[0],
                    "reprobados_total": comunaData[ comunaData[' Variable'] == " Reprobados Total"].iloc[:, -1].values[0],
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
            self.addLog(str(error))
            createFolderNoProcesado(self.PATH, self.FOLDER)
            print(error)
    
## -------------------------------------- ##
## -------------------------------------- ##
## -------------------------------------- ##

class ETL_Processing:
    def __init__(self, querys, localidades):
        # Para la base de datos
        self.fuente = "BCN_Rendimiento_academico"              
        
        # indicador reprobados / (aprobados + reprobados + retirados)
        self.nombreIndicadorA = "Tasa de reprobacion"
        self.indicador_idA = 14  ## Valor numerico, revisar si no existe en bd
        self.dimensionA = getDimension("Educacional")
        self.prioridadA = 1
        self.urlA =  "https://www.bcn.cl/siit/estadisticasterritoriales/tema?id=42"
        self.descripcionA = "Indicador asociado a la cantidad de reprobados respecto a la poblacion estudantil"
        
        # indicador aprobados / (aprobados + reprobados + retirados)
        self.nombreIndicadorB = "Tasa de aprobacion"
        self.indicador_idB = 15  ## Valor numerico, revisar si no existe en bd
        self.dimensionB = getDimension("Educacional")
        self.prioridadB = 1
        self.urlB =  "https://www.bcn.cl/siit/estadisticasterritoriales/tema?id=42"
        self.descripcionB = "Indicador asociado a la cantidad de reprobados respecto a la poblacion estudantil "
        
        # indicador retirados / (aprobados + reprobados + retirados)
        self.nombreIndicadorC = "Tasa de retirados"
        self.indicador_idC = 16  ## Valor numerico, revisar si no existe en bd
        self.dimensionC = getDimension("Educacional")
        self.prioridadC = 1
        self.urlC =  "https://www.bcn.cl/siit/estadisticasterritoriales/tema?id=42"
        self.descripcionC = "Indicador asociado a la cantidad de retirados respecto a la poblacion estudantil"
        
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
        df_merged['valor'] =  df_merged['aprobados_total']
        data = df_merged[['comuna_id', 'valor', 'dimension_id']]
        
        normalized = dataNormalize(data)
        return normalized
    
    def TransformB(self, comuna):
        # indicador aprobados / (aprobados + reprobados + retirados)
        df = self.transaccionalData
        df_merged = df.merge(comuna, left_on='comuna_id', right_on='comuna_id', how='right')
        total_sum = df_merged['retirados_total'] + df_merged['aprobados_total'] + df_merged['reprobados_total']
        df_merged['valor'] = df_merged['aprobados_total'] / total_sum
        data = df_merged[['comuna_id', 'valor', 'dimension_id']]
        
        normalized = dataNormalize(data)
        
        return normalized
 
    def TransformC(self, comuna):
        # indicador retirados / (aprobados + reprobados + retirados)
        df = self.transaccionalData
        df_merged = df.merge(comuna, left_on='comuna_id', right_on='comuna_id', how='right')
        total_sum = df_merged['retirados_total'] + df_merged['aprobados_total'] + df_merged['reprobados_total']
        df_merged['valor'] = df_merged['retirados_total'] / total_sum
        data = df_merged[['comuna_id', 'valor', 'dimension_id']]
        data.loc[:, 'valor'] = 1- data['valor']
        
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
            # self.addETLinfo(self.indicador_idA, self.nombreIndicadorA, self.prioridadA, self.descripcionA, self.urlA, self.dimensionA)
            self.addETLinfo(self.indicador_idB, self.nombreIndicadorB, self.prioridadB, self.descripcionB, self.urlB, self.dimensionB)
            self.addETLinfo(self.indicador_idC, self.nombreIndicadorC, self.prioridadC, self.descripcionC, self.urlC, self.dimensionC)

            self.Extract()
            # self.querys.updateFlagProcessing(self.indicador_idA)
            self.querys.updateFlagProcessing(self.indicador_idB)
            self.querys.updateFlagProcessing(self.indicador_idC)
            
            comunas = self.localidades.getDataComunas()
            # dataA = self.TransformA(comunas)
            # self.Load(dataA, self.dimensionA, self.indicador_idA)
            
            dataB = self.TransformB(comunas)
            self.Load(dataB, self.dimensionB, self.indicador_idB)
            
            dataC = self.TransformC(comunas)
            self.Load(dataC, self.dimensionC, self.indicador_idC)
            


        except Exception as error:
            print(error)
        return {"OK": 200, "mesagge": "Indicators is updated successfully"}
