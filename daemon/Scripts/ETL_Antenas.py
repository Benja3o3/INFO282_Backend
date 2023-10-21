import pandas as pd
import traceback
from sqlalchemy.sql import text
from utils import getDimension
from utils import getDateFile
from utils import getLastFile

import psycopg2
from psycopg2 import sql


class ETL_Transactional:
    def __init__(self, db, localidades):
        self.fuente = "Subtel_antenas"
        self.nombreData = "Numero_de_antenas"
        self.dimension = "Tecnologia"

        # << No modificar >>
        self.FOLDER = "Source/" + self.fuente + "/"
        self.TABLENAME = "data_" + self.fuente
        self.PATH = getLastFile(self.FOLDER)
        self.uploadDate = getDateFile(self.PATH)
        self.db = db
        self.localidades = localidades
        self.extractedData = None
        self.valor = 0
        # << No modificar <<

    def __string__(self):
        return str(self.nombreData)

    def Extract(self):
        self.extractedData = pd.read_csv(self.PATH, delimiter=";")

    def Tranform(self, comuna):
        self.extractedData = self.extractedData[["Codigo comuna", "conectividad"]]
        comunaData = self.extractedData[self.extractedData['Codigo comuna'] == comuna["comuna_id"]]
        return comunaData    
    
    def Load(self, data):
        self.updateFlag()

        with self.db.connect() as conn:
            try:
                data = {
                    "nombre": self.nombreData,
                    "conectividad": int(data["conectividad"].iloc[0]),
                    "fecha" : self.uploadDate,
                    "flag" : True,
                    "comuna_id": int(data["Codigo comuna"].iloc[0]),
                    "dimension_id": getDimension(self.db, self.dimension)
                                     
                }
                query = text(
                    f"""
                    INSERT INTO {self.TABLENAME} (nombre, conectividad, fecha, flag, comuna_id, dimension_id)
                    VALUES (:nombre, :conectividad, :fecha, :flag, :comuna_id, :dimension_id)
                    """
                )
                conn.execute(query, data)
                conn.commit()
                
            except:
                print("Datos no encontrados")

    def updateFlag(self):
        with self.db.connect() as conn:
            try:
                data = {
                    "nombre": self.nombreData, 
                }
                
                query = text(
                    f"""
                    UPDATE {self.TABLENAME}
                    SET flag = False
                    WHERE nombre = :nombre
                    AND flag = True
                    """
                )
                conn.execute(query, data)
                conn.commit()     

            except KeyError as error:
                print(error)
                
    def ETLProcess(self):
        with self.db.connect() as con:     
            createTableQuery = text(
                f"CREATE TABLE IF NOT EXISTS {self.TABLENAME} ("
                " data_id serial PRIMARY KEY,"
                " nombre VARCHAR(255),"
                " conectividad INT,"
                " fecha Date,"
                " flag Boolean,"
                " comuna_id INT,"
                " dimension_id INT"
                ")"
            )      

            con.execute(createTableQuery)
                        
            queryFecha = text(f"SELECT MAX(fecha) FROM {self.TABLENAME} WHERE flag=:flag")
            queryFecha = queryFecha.bindparams(flag=True)

            maxFecha = con.execute(queryFecha)
            rows = maxFecha.fetchall()
            maxFecha = rows[0][0]
            con.commit()

        if maxFecha == None or self.uploadDate > maxFecha:
            try:
                self.Extract()
                comunas = self.localidades.getComunas()
                for _, comuna in comunas.iterrows():
                    data = self.Tranform(comuna)
                    self.Load(data)
            except KeyError as error:
                print(error)
        else:
            print("Datos en bruto ya actualizados: ", self.fuente)
            return True  
        return False


class ETL_Processing:
    def __init__(self, dbTransaccional, dbProcessing, localidades):
        # Para la base de datos
        self.fuente = "Subtel_antenas"              
        self.nombreIndicador = "Numero_de_antenas"
        self.idIndicador = "TEC_NUM_ANTENAS"
        self.dimension = "Tecnologia"
        self.prioridad = 1
        self.url =  "https://antenas.subtel.gob.cl/leydetorres/mapaAntenasAutorizadas.html"
        
        # << No modificar >>
        self.TABLENAME = "data_" + self.fuente
        self.INDICADORTABLE = "ind_" + self.fuente

        self.valor = 0
        self.flag = True

        self.dbTransaccional = dbTransaccional
        self.dbProcessing = dbProcessing
        self.localidades = localidades

        self.transaccionalData = None
        self.calculedData = None
        # << No modificar >>
        

    def __string__(self):
        return str(self.nombreIndicador)

    def Extract(self):
        with self.dbTransaccional.connect() as con:     
            query = text(f"SELECT * FROM {self.TABLENAME} WHERE flag = true")
            result = con.execute(query)
            rows = result.fetchall()
            column_names = result.keys()
            
        #Transforma tabla en dataframe
        df = pd.DataFrame(columns=column_names)
        for row in rows:
            row_dict = {
                column_name: value for column_name, value in zip(column_names, row)
            }
            df = pd.concat([df, pd.DataFrame(row_dict, index=[0])], ignore_index=True)

        self.fecha = df['fecha'].iloc[0]
        self.transaccionalData = df           

              
    def Transform(self, comuna):
        df = self.transaccionalData
        df_merged = df.merge(comuna, left_on='comuna_id', right_on='comuna_id', how='inner')
        df_merged['valor'] = df_merged['conectividad'] / df_merged['Poblacion']
        data = df_merged[['comuna_id', 'valor', 'dimension']]
        min_value = df_merged['valor'].min()
        max_value = df_merged['valor'].max()
        data.loc[:, 'valor'] = (data['valor'] - min_value) / (max_value - min_value)
        return data
 
    def Load(self, data):
        self.updateFlag()
        with self.dbProcessing.connect() as conn:       
            for index, row in data.iterrows():
                try:
                    data = {
                        "indicador": self.idIndicador,
                        "valor": row['valor'],
                        "fecha" : self.fecha,
                        "flag" : self.flag,
                        "comuna_id" : row['comuna_id'],
                        "dimension" : self.dimension
                    }
                    query = text(
                        f"""
                        INSERT INTO {self.INDICADORTABLE} (indicador, valor, fecha, flag, comuna_id, dimension)
                        VALUES (:indicador, :valor, :fecha, :flag, :comuna_id, :dimension)
                        """
                    )
                    conn.execute(query, data)
                    conn.commit()
                except KeyError as error:
                    print(error)
                    

    def updateFlag(self):
        with self.dbProcessing.connect() as conn:
            try:
                data = {
                    "table": self.INDICADORTABLE,
                    "indicador_id": self.idIndicador, 
                }
                
                query = text(
                    f"""
                    UPDATE {self.INDICADORTABLE}
                    SET flag = False
                    WHERE indicador = :indicador_id
                    AND flag = True
                    """
                )
                conn.execute(query, data)
                conn.commit()     

            except KeyError as error:
                print(error)
                
    
    def ETLProcess(self):
        with self.dbProcessing.connect() as con:     
            createTableQuery = text(
                f"CREATE TABLE IF NOT EXISTS {self.INDICADORTABLE} ("
                " id serial PRIMARY KEY,"
                " indicador VARCHAR(255),"
                " valor FLOAT, "
                " fecha Date,"
                " flag Boolean,"
                " comuna_id INT,"
                " dimension VARCHAR(255)"
                ")"
            )      
            con.execute(createTableQuery)
            
            addIndicadorQuery = text(
                "INSERT INTO indicadores (id, nombre, prioridad, dimension, fuente) "
                "VALUES (:ind_id, :nombre, :prioridad, :dimension, :fuente )"
                "ON CONFLICT (id) DO NOTHING"
            )
            data = {
                "ind_id": self.idIndicador,
                "nombre": self.nombreIndicador,
                "prioridad": self.prioridad,
                "dimension": self.dimension,
                "fuente": self.url
            }
            con.execute(addIndicadorQuery, data)
            
            con.commit()

        try:
            self.Extract()
            comunas = self.localidades.getComunas()
            data = self.Transform(comunas)
            self.Load(data)

        except Exception:
            traceback.print_exc()

        return {"OK": 200, "mesagge": "Indicators is updated successfully"}
