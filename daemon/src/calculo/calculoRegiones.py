from sqlalchemy.sql import text
import datetime
import pandas as pd
class calculoRegiones():    
    def __init__(self, db_processing, localidades):
        self.db = db_processing
        regiones = localidades.getDataRegiones()     
        self.allDimensiones = ["Educacional", "Salud", "Seguridad", "Tecnologia",
                        "Economico", "Ecologico", "Movilidad", "Diversion"]     
        self.calculateIndicadores()
        self.calculateDimensiones()
        self.calculateBienestar()
        
    def calculateIndicadores(self):        
        with self.db.connect() as conn:
            getIndicatorsInfo = text(
                f"""
                    SELECT * FROM indicadoresinfo
                """
            )
            
            result = conn.execute(getIndicatorsInfo)
            indicadores = result.fetchall()
            date = datetime.date.today().isoformat()
    
            getIndicatorsRegion = text(
                f"""
                    INSERT INTO calculoindicadoresregion (valor, fecha, flag, dimension_id, region_id, indicador_id)
                    SELECT AVG(cc.valor) AS valor, '{date}'::date as fecha, 
                    true as flag, cc.dimension_id AS dimension_id, c.region_id AS region_id, cc.indicador_id AS indicador_id
                    FROM calculoindicadorescomuna cc
                    JOIN comunas c ON cc.comuna_id = c.comuna_id
                    GROUP BY c.region_id, cc.indicador_id,  cc.dimension_id;
                """
            )
            result = conn.execute(getIndicatorsRegion)
            conn.commit()
            
            
    def calculateDimensiones(self):
        with self.db.connect() as conn:
            
            queryGetDimensionComuna = text(
                """
                SELECT * FROM dimensionesregiones
                """
            )
            
            queryGetIndicadores = text(
                """
                SELECT * FROM calculoindicadoresregion
                WHERE flag = true
                """
            )
            result = conn.execute(queryGetIndicadores)
            indicadores = result.fetchall()
            column_names = result.keys()
            df = pd.DataFrame(indicadores, columns=column_names)
            self.updateFlag()
            
            result = conn.execute(queryGetDimensionComuna)
            dimensionesregiones = result.fetchall()
            
            all_data = []
            for dimreg in dimensionesregiones:
                reg = dimreg[0]
                dim = dimreg[1]
                dimregdf = df.loc[(df['region_id'] == reg) & (df['dimension_id'] == dim)]
                suma_valores = dimregdf['valor'].sum()
                cantidad_elementos = len(dimregdf)
                if(cantidad_elementos == 0):
                    valor = 0
                else:
                    promedio = suma_valores / cantidad_elementos 
                    valor = promedio 
                current_date = datetime.date.today()
                data = {
                        "valor": valor,
                        "fecha": current_date,
                        "flag": True,
                        "region_id": reg,
                        "dimension_id" : dim
                    }
                all_data.append(data)
            self.loadDimensiones(all_data)
    def updateFlag(self):
        with self.db.connect() as conn:
            try:
                query = text(
                    f"""
                    UPDATE calculodimensionesregion
                    SET flag = False
                    WHERE flag = True
                    """
                )
                conn.execute(query)
                conn.commit()     

            except KeyError as error:
                pass   
    def loadDimensiones(self, data_list):
        columns = ', '.join(data_list[0].keys())
        values_list = []

        values = ', '.join([f":{key}" for key in data_list[0].keys()])
        values_list.append(f"({values})")
    
        values = ', '.join(values_list)
    
        
        with self.db.connect() as conn:
            query = text(f"""INSERT INTO calculodimensionesregion({columns}) 
                            VALUES {values}""")
            
            conn.execute(query, data_list)
            conn.commit()
    def calculateBienestar(self):
        
        with self.db.connect() as conn:
            queryChangeFlag = text(
                f"""
                UPDATE calculobienestarregion  
                SET flag = False
                WHERE flag = True
                """
            )
            conn.execute(queryChangeFlag)
            date = datetime.date.today().isoformat()
            queryCalculatePromedio = text(
                        f"""
                        INSERT INTO calculobienestarregion (region_id, valor_bienestar, flag, fecha)
                        SELECT region_id, AVG(valor) as valor_bienestar, true as flag, '{date}'::date as fecha
                        FROM calculodimensionesregion
                        WHERE flag = true 
                        GROUP BY region_id
                        """
                    )
            conn.execute(queryCalculatePromedio)
            conn.commit()

        