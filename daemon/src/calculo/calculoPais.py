from sqlalchemy.sql import text
import datetime
import pandas as pd
class calculoPais():    
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
                    INSERT INTO calculoindicadorespais (valor, fecha, flag, dimension_id, pais_id, indicador_id)
                    SELECT AVG(cc.valor) AS valor, '{date}'::date as fecha, 
                    true as flag, cc.dimension_id AS dimension_id, r.pais_id AS pais_id, cc.indicador_id AS indicador_id
                    FROM calculoindicadoresregion cc
                    JOIN regiones r ON cc.region_id = r.region_id
                    GROUP BY r.pais_id, cc.indicador_id,  cc.dimension_id;
                """
            )
            result = conn.execute(getIndicatorsRegion)
            conn.commit()
            
            
    def calculateDimensiones(self):
        with self.db.connect() as conn:
            
            queryGetDimensionComuna = text(
                """
                SELECT * FROM dimensionespais
                """
            )
            
            queryGetIndicadores = text(
                """
                SELECT * FROM calculoindicadorespais
                WHERE flag = true
                """
            )
            result = conn.execute(queryGetIndicadores)
            indicadores = result.fetchall()
            column_names = result.keys()
            df = pd.DataFrame(indicadores, columns=column_names)
            self.updateFlag()
            
            result = conn.execute(queryGetDimensionComuna)
            dimensionespais = result.fetchall()
            
            all_data = []
            for dimpais in dimensionespais:
                pais = dimpais[0]
                dim = dimpais[1]
                dimpaisdf = df.loc[(df['pais_id'] == pais) & (df['dimension_id'] == dim)]
                suma_valores = dimpaisdf['valor'].sum()
                cantidad_elementos = len(dimpaisdf)
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
                        "pais_id": pais,
                        "dimension_id" : dim
                    }
                all_data.append(data)
            self.loadDimensiones(all_data)
    def updateFlag(self):
        with self.db.connect() as conn:
            try:
                query = text(
                    f"""
                    UPDATE calculodimensionespais
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
            query = text(f"""INSERT INTO calculodimensionespais({columns}) 
                            VALUES {values}""")
            
            conn.execute(query, data_list)
            conn.commit()
    def calculateBienestar(self):
        
        with self.db.connect() as conn:
            queryChangeFlag = text(
                f"""
                UPDATE calculobienestarpais
                SET flag = False
                WHERE flag = True
                """
            )
            conn.execute(queryChangeFlag)
            date = datetime.date.today().isoformat()
            queryCalculatePromedio = text(
                        f"""
                        INSERT INTO calculobienestarpais (pais_id, valor_bienestar, flag, fecha)
                        SELECT pais_id, AVG(valor) as valor_bienestar, true as flag, '{date}'::date as fecha
                        FROM calculodimensionespais
                        WHERE flag = true 
                        GROUP BY pais_id
                        """
                    )
            conn.execute(queryCalculatePromedio)
            conn.commit()

        