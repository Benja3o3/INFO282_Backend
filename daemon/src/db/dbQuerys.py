from sqlalchemy.sql import text
import pandas as pd
from sqlalchemy import insert



class Querys:
    def __init__(self, dbTransaccional, dbProcesada):
        self.dbTran = dbTransaccional
        self.dbPro = dbProcesada
    
    def loadFileTransactional(self, tableName, data_list ):
        columns = ', '.join(data_list[0].keys())
        values_list = []


        values = ', '.join([f":{key}" for key in data_list[0].keys()])
        values_list.append(f"({values})")
    
        values = ', '.join(values_list)
      
        
        with self.dbTran.connect() as conn:
            query = text(f"""INSERT INTO {tableName} ({columns}) 
                            VALUES {values}""")
            conn.execute(query, data_list)
            conn.commit()
 
        
            
    def loadDataProcessing(self, data_list):
        columns = ', '.join(data_list[0].keys())
        values_list = []


        values = ', '.join([f":{key}" for key in data_list[0].keys()])
        values_list.append(f"({values})")
    
        values = ', '.join(values_list)
        
        with self.dbPro.connect() as conn:
            query = text(f"""INSERT INTO calculoindicadorescomuna({columns}) 
                            VALUES {values}""")
            conn.execute(query, data_list)
            conn.commit()
                
    def updateFlagFuente(self, tableName):
        with self.dbTran.connect() as conn:
            try:
                query = text(
                            f"""
                            UPDATE {tableName}
                            SET flag = False
                            WHERE flag = True
                            """ )
                conn.execute(query)
                conn.commit()   
            except:
                print("Error al actualizar <<flags>>")
              
    def getMaxDate(self, tableName):
        with self.dbTran.connect() as conn:
            try:
                query = text(f"""
                            SELECT MAX(fecha)
                            FROM {tableName}
                            WHERE flag=true
                            """)
                maxDate = conn.execute(query)
                rows = maxDate.fetchall()
                maxDate = rows[0][0]
                return maxDate
            except Exception as error:
                print("Error al conseguir fecha maxima")
    
    
    def getTransactionalData(self, tableName):
        with self.dbTran.connect() as con:
            try:
                query = text(f"""
                            SELECT * FROM {tableName}
                            WHERE flag = true""")
                result = con.execute(query)
                rows = result.fetchall()

                column_names = result.keys()
                data_list = []

                for row in rows:
                    row_dict = {
                        column_name: value for column_name, value in zip(column_names, row)
                    }
                    data_list.append(row_dict)

                df = pd.DataFrame(data_list)

                return df
            except Exception as error:
                print("Error al recuperar informacion de db transaccional", tableName)

    def updateFlagProcessing(self, indicador_id):
        with self.dbPro.connect() as conn:
            try:
                query = text(
                    f"""
                    UPDATE calculoindicadorescomuna 
                    SET flag = False
                    WHERE indicador_id = {indicador_id}
                    AND flag = True
                    """
                )
                conn.execute(query)
                conn.commit()     

            except:
                print("Error al actualizar <<Flags>>")
    def addIndicatorsInfo(self, data):
        with self.dbPro.connect() as conn:
            try:
                query = text(
                    """
                    INSERT INTO indicadoresinfo (indicadoresinfo_id, nombre, prioridad, descripcion, fuente, dimension)
                    VALUES (:indicadoresinfo_id, :nombre, :prioridad, :descripcion, :fuente, :dimension)
                    ON CONFLICT (indicadoresinfo_id) DO NOTHING
                    """
                )
                conn.execute(query, data)
                conn.commit()   
            except :
                print("Error al añadir informacion a indicadores")

    def addFileToLog(self, data):
        with self.dbPro.connect() as conn:
            try:
                query = text(
                    """
                    INSERT INTO log_archivos (fecha, nombre_archivo, tipo_archivo, error, estado)
                    VALUES (:fecha, :nombre_archivo, :tipo_archivo, :error, :estado)
                    """
                )
                conn.execute(query, data)
                conn.commit()   
            except Exception as error:
                # print(error)
                print("Error al añadir informacion al log")