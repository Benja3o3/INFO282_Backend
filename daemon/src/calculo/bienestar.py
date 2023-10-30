from sqlalchemy.sql import text
import datetime

class Bienestar():    
    def __init__(self, db_processing, localidades):
        self.db = db_processing
        comunas = localidades.getComunas()        
        self.allDimensiones = ["Educacional", "Salud", "Seguridad", "Tecnologia",
                        "Economico", "Ecologico", "Movilidad", "Diversion"]     
        self.calculateBienestar(comunas)
        
    def calculateBienestar(self, comunas):
        comunas = comunas[['comuna_id']]
        
        with self.db.connect() as conn:
            queryChangeFlag = text(
                f"""
                UPDATE calculobienestarcomuna  
                SET flag = False
                WHERE flag = True
                """
            )
            conn.execute(queryChangeFlag)
            date = datetime.date.today().isoformat()
            queryCalculatePromedio = text(
                        f"""
                        INSERT INTO calculobienestarcomuna (comuna_id, valor_bienestar, flag, fecha)
                        SELECT comuna_id, AVG(valor) as valor_bienestar, true as flag, '{date}'::date as fecha
                        FROM calculodimensionescomuna 
                        WHERE flag = true 
                        GROUP BY comuna_id 
                        """
                    )
            conn.execute(queryCalculatePromedio)
            conn.commit()