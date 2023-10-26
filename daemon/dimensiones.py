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
            queryGetIndicators = text(
                """ 
                SELECT * FROM indicadoresinfo 
                """
            )  
            result = conn.execute(queryGetIndicators)
            indicadoresInfo = result.fetchall()
            column_names = result.keys()
            indicadoresinfo = pd.DataFrame(indicadoresInfo, columns=column_names)

            queryGetDimensiones = text(
                """ 
                SELECT * FROM dimensiones 
                """
            )  
            result = conn.execute(queryGetDimensiones)
            dimensionesinfo = result.fetchall()

            queryGetDimensionComuna = text(
                """
                SELECT * FROM dimensionescomunas
                """
            )
            result = conn.execute(queryGetDimensionComuna)
            dimensioncomuna = result.fetchall()

            queryGetIndicadores = text(
                """
                SELECT * FROM indicadoresCalculo
                WHERE flag = true
                """
            )
            result = conn.execute(queryGetIndicadores)
            indicadores = result.fetchall()
            column_names = result.keys()
            df = pd.DataFrame(indicadores, columns=column_names)
            self.updateFlag()
            
        all_data = []
        for dimcom in dimensioncomuna:
            com = dimcom[0]
            dim = dimcom[1]
            dimPeso = dimensionesinfo[dim-1][-1]

            dimcomdf = df.loc[(df['comuna_id'] == com) & (df['dimension_id'] == dim)]
            suma_valores = dimcomdf['valor'].sum()
            cantidad_elementos = len(dimcomdf)
            if(cantidad_elementos == 0):
                valor = 0
            else:
                promedio = suma_valores / cantidad_elementos 
                valor = promedio * dimPeso
            current_date = datetime.date.today()
            data = {
                    "valor": valor,
                    "fecha": current_date,
                    "flag": True,
                    "comuna_id": com,
                    "dimension_id" : dim
                }
            all_data.append(data)
        self.loadDimensiones(all_data)
            
    def loadDimensiones(self, data_list):
        columns = ', '.join(data_list[0].keys())
        values_list = []

        values = ', '.join([f":{key}" for key in data_list[0].keys()])
        values_list.append(f"({values})")
    
        values = ', '.join(values_list)
    
        
        with self.db.connect() as conn:
            query = text(f"""INSERT INTO calculodimensiones({columns}) 
                            VALUES {values}""")
            
            conn.execute(query, data_list)
            conn.commit()

    def updateFlag(self):
        with self.db.connect() as conn:
            try:
                query = text(
                    f"""
                    UPDATE calculodimensiones
                    SET flag = False
                    WHERE flag = True
                    """
                )
                conn.execute(query)
                conn.commit()     

            except KeyError as error:
                pass    
