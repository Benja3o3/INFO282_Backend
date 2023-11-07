import pandas as pd
from src.calculo.utils import getDateFile, getDimension, getLastFile, dataNormalize
from sqlalchemy.sql import text
from psycopg2 import sql
from datetime import datetime

# SUP_VEGATA No tiene datos 

class ETL_Transactional:
    def __init__(self, querys, localidades):

        self.fuente = "CENSO_Cantidad_Viviendas"
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

    def Extract(self):
        self.extractedData = pd.read_excel(self.PATH, sheet_name="COMUNAS")

    def Tranform(self, comunas):
        dataToLoad = []
        # print(self.extractedData)
        #Correccion de datos
        self.extractedData.columns = self.extractedData.iloc[0]
        self.extractedData = self.extractedData[1:]
        self.extractedData = self.extractedData.iloc[1:338, 2:].reset_index() 
        self.extractedData.columns = self.extractedData.columns.str.replace('Viviendas Particulares Ocupadas con Moradores Presentes', 'Viviendas Con Moradores Presentes')
        self.extractedData = self.extractedData[['Código Comuna','Viviendas Colectivas','TOTAL VIVIENDAS','Viviendas Con Moradores Presentes']]
        # self.extractedData['NOMBRE COMUNA'] = self.extractedData['NOMBRE COMUNA'].str.lower()
        print(self.extractedData)
        comunaswithoutname = []
        for _, comuna in comunas.iterrows():

            comunaData = self.extractedData[self.extractedData['Código Comuna'] == comuna["comuna_id"]]
            for _, row in comunaData.iterrows():
                totalViviendas = 0
                viviendasColectivas = 0
                viviendasMoradoresPresentes = 0
                try:
                    totalViviendas = float(row["TOTAL VIVIENDAS"])
                    viviendasColectivas = float(row["Viviendas Colectivas"])
                    viviendasMoradoresPresentes = float(row["Viviendas Con Moradores Presentes"])
                except KeyError as e:
                    # print(e)
                    print("No existe información de: ", comuna['nombre'])
                data = {
                    "total_viviendas": totalViviendas,
                    "viviendas_colectivas": viviendasColectivas,
                    "viviendas_moradores_presentes": viviendasMoradoresPresentes,
                    "fecha" : self.uploadDate,
                    "flag" : True,
                    "comuna_id": comuna['comuna_id'],
                    "dimension_id": getDimension(self.dimension)              
                    }
                print(data)
                dataToLoad.append(data)
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
            else:
                print("Datos en bruto ya actualizados: ", self.fuente)
                return True  # Ya actualizados 
            return False     # No actualizados
        except Exception as error:
            print(error)
    
## -------------------------------------- ##
## -------------------------------------- ##
## -------------------------------------- ##

class ETL_Processing:
    def __init__(self, querys, localidades):
        # Para la base de datos
        self.fuente = "CENSO_Cantidad_Viviendas"              
        self.nombreIndicador = "CENSO_Cantidad_Viviendas"
        
        # informacion indicador
        self.indicador_id = 12  ## Valor numerico, revisar si no existe en bd
        self.dimension = "Economico"
        self.prioridad = 1
        self.url =  "http://www.censo2017.cl/wp-content/uploads/2017/12/Cantidad-de-Viviendas-por-Tipo.xlsx"
        self.descripcion = "Indicador asociado a la cantidad total de viviendas"
        
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
        df_merged['valor'] = df_merged['total_viviendas'] / comuna['poblacion']
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
