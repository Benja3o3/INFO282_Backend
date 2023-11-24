import pandas as pd
from src.calculo.utils import getDateFile, getDimension, getLastFile, dataNormalize, createFolderNoProcesado, getDateTimeFile, getExtension
from sqlalchemy.sql import text
from psycopg2 import sql
from datetime import datetime


class ETL_Transactional:
    def __init__(self, querys, localidades):

        self.fuente = "BCN_Deficit_habitacional"
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
        self.extractedData = pd.read_excel(self.PATH, sheet_name="Datos")
        
    def Tranform(self, comunas, conflictNames):
        dataToLoad = []
        for _, comuna in comunas.iterrows():            
            comunaData = self.extractedData[self.extractedData['Unidad territorial'].str.lower().str.contains(comuna["nombre"].lower())]
            if(comunaData.empty):
                comunaData = self.extractedData[self.extractedData['Unidad territorial'].str.contains(conflictNames[comuna["nombre"]], regex=False)]
                
            
            deficit_total = comunaData[ comunaData[' Variable'] == " Déficit Habitacional Total"].iloc[0, -2]
            hogares_allegados = comunaData[ comunaData[' Variable'] == " Hogares Allegados"].iloc[0, -2]
            hogares_total = comunaData[ comunaData[' Variable'] == " Hogares totales"].iloc[0, -2]
            nucleos_allegados = comunaData[ comunaData[' Variable'] == " Núcleos Allegados"].iloc[0, -1]
            viviendas_irrecuperables = comunaData[ comunaData[' Variable'] == " Viviendas Irrecuperables"].iloc[0, -2]
            viviendas_totales = comunaData[ comunaData[' Variable'] == " Viviendas totales"].iloc[0, -2]

            try:            
                data = {
                    "deficit_total": float(deficit_total),
                    "hogares_allegados": float(hogares_allegados),
                    "hogares_total": float(hogares_total),
                    "nucleos_allegados": float(nucleos_allegados),
                    "viviendas_irrecuperables": float(viviendas_irrecuperables),
                    "viviendas_totales": float(viviendas_totales),
                    
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
        self.fuente = "BCN_Deficit_habitacional"              
        
        # Mortalidad general
        self.nombreIndicadorA = "Indice deficit habitacional "
        self.indicador_idA = 24  ## Valor numerico, revisar si no existe en bd
        self.dimensionA = 7
        self.prioridadA = 1
        self.urlA =  "https://www.bcn.cl/siit/estadisticasterritoriales/tema?id=190"
        self.descripcionA = "Deficit habitacional total / viviendas totales"


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
        df_merged.loc[:, 'valor'] = 1 - df_merged['deficit_total'] / df_merged['viviendas_totales']
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

            self.Extract()
            self.querys.updateFlagProcessing(self.indicador_idA)

            comunas = self.localidades.getDataComunas()
            dataA = self.TransformA(comunas)
            self.Load(dataA, self.dimensionA, self.indicador_idA)


        except Exception as error:
            print(error)
        return {"OK": 200, "mesagge": "Indicators is updated successfully"}
