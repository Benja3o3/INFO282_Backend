import pandas as pd
from src.calculo.utils import getDateFile, getDimension, getLastFile, dataNormalize, createFolderNoProcesado, getExtension, getDateTimeFile
from sqlalchemy.sql import text
from psycopg2 import sql
from datetime import datetime

class ETL_Transactional:
    def __init__(self, querys, localidades):

        self.fuente = "CEM_Evaluacion_docente"
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
        self.extractedData = self.extractedData[["AGNO_EVAL", "NOM_RBD", "COD_COM_RBD", "COD_DEPROV_RBD", "DOC_GENERO", "DOC_FEC_NAC", "PF_PJE", "PF_Cat_Carrera"]]
    
        for _, comuna in comunas.iterrows():
            comunaData = self.extractedData[self.extractedData['COD_COM_RBD'] == comuna["comuna_id"]]
            for _, row in comunaData.iterrows():
                try:
                    año_evaluacion = int(row["AGNO_EVAL"])
                    nombre_establecimiento = str(row["NOM_RBD"])
                    codigo_departamento_provincial = int(row["COD_DEPROV_RBD"])
                    genero_docente = int(row["DOC_GENERO"])
                    fecha_nacimiento_docente = str(row["DOC_FEC_NAC"])
                    puntaje_mod = row["PF_PJE"].replace(',', '.')
                    if(puntaje_mod != ' '):
                        puntaje_final = float(puntaje_mod)
                    else:
                        continue
                            
                    escala_final = str(row["PF_Cat_Carrera"]) 
                    data = {
                        "año_evaluacion": año_evaluacion,
                        "nombre_establecimiento": nombre_establecimiento,
                        "codigo_departamento_provincial": codigo_departamento_provincial,
                        "genero_docente": genero_docente, 
                        "fecha_nacimiento_docente": fecha_nacimiento_docente,
                        "puntaje_final": puntaje_final,
                        "escala_final": escala_final,
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
        val = True
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
        self.fuente = "CEM_Evaluacion_docente"              
        self.nombreIndicador = "Calidad docente"
        
        # informacion indicador
        self.indicador_id = 5 ## Valor numerico, revisar si no existe en bd
        self.dimension = "Educacional"
        self.prioridad = 1
        self.url =  ""
        self.descripcion = "Indicador asociado al promedio de calificaciones en la evaluacion docente"
        
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
        df_merged['valor'] = df_merged['puntaje_final']     
        data = df_merged[['comuna_id', 'valor', 'dimension_id']]
        resultado = data.groupby('comuna_id').agg({'valor': 'mean', 'dimension_id': 'first'}).reset_index()
        
        normalized = dataNormalize(resultado)
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
