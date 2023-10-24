from sqlalchemy.sql import text
import pandas as pd
import datetime



class Dimensiones():
    def __init__(self, db_processing, localidades):
        self.db = db_processing
        comunas = localidades.getComunas()
        self.allDimensiones = ["Educacional", "Salud", "Seguridad", "Tecnologia",
                        "Economico", "Ecologico", "Movilidad", "Diversion"] 
        self.dimensiones = self.calculateDimensiones(comunas)


    def calculateDimensiones(self, comunas):
        comunas = comunas[['comuna_id']]

        with self.db.connect() as conn:
            
            getTableNames = text(f"""
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_name LIKE 'ind_%'""")
            result = conn.execute(getTableNames)
            indicadores = result.fetchall()
            

            cartesian_product = pd.MultiIndex.from_product([self.allDimensiones, 
                                                            comunas['comuna_id']], 
                                                            names=['dimension', 'comuna_id'])
            comunaDimensionPD = pd.DataFrame(index=cartesian_product).reset_index()
        
            for index, row in comunaDimensionPD.iterrows():
                valor = 0
                count = 0
                for table_name in indicadores:
                    if table_name[0] != "indicadores":
                        searchDataInTables = text(f"""
                            SELECT * FROM {table_name[0]}
                            WHERE dimension = :dim 
                            AND comuna_id = :comuna_id
                        """)
                        data = {
                            "dim": row["dimension"],
                            "comuna_id": row["comuna_id"]
                        }
                        result = conn.execute(searchDataInTables, data)
                        values = result.fetchall()
                        
                        for value in values:
                            valor += value[2]
                            count += 1
                            
                if(count != 0):
                    valor = valor/count
                else:
                    valor = 0 
                    
                current_date = datetime.date.today()

                dimensionData = {
                    "nombre": row["dimension"],
                    "dimension_id" : getDimension(row["dimension"]),
                    "valor": valor,
                    "fecha": current_date,
                    "flag": True,
                    "comuna_id": row["comuna_id"],
                }
                addDataInTable = text(f"""
                                INSERT INTO dimension (nombre, valor, fecha, flag, comuna_id)
                                VALUES (:nombre, :valor, :fecha, :flag, :comuna_id)
                                      """)
                self.updateFlag(row["dimension"], row["comuna_id"])
                conn.execute(addDataInTable, dimensionData)
                conn.commit()

    def updateFlag(self, dimension, comuna_id):
        with self.db.connect() as conn:
            try:
                query = text(
                    f"""
                    UPDATE dimension
                    SET flag = False
                    WHERE nombre = '{dimension}'
                    AND comuna_id = {comuna_id}
                    AND flag = True
                    """
                )
                conn.execute(query)
                conn.commit()     

            except KeyError as error:
                pass    
            