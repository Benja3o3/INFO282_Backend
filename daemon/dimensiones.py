from sqlalchemy.sql import text

class Dimensiones():
    def __init__(self, db_processing):
        self.db = db_processing
        self.dimensiones = self.getDimensiones()
        self.foreachDimension()
        print(" [ x ] Dimensiones actualizadas ")

    def getDimensiones(self):
        query = text("SELECT * FROM dimension")
        result = []
        with self.db.connect() as con:
            result = con.execute(query)
            dimensiones = result.fetchall()
        return dimensiones
    
    def foreachDimension(self):
        for dim in self.dimensiones:
            self.calculateDimension(dim[0])

    def calculateDimension(self, id):
        query = text("""
            SELECT *
            FROM Indicador
            WHERE fecha = (
                SELECT MAX(fecha)
                FROM Indicador
            )
            AND dimension_id = :dimension_id
        """)
        query = query.bindparams(dimension_id=id)

        result = []
        with self.db.connect() as con:
            result = con.execute(query)
            indicadores = result.fetchall()

        dim_value = 0
        if len(indicadores) != 0:
            for ind in indicadores:
                dim_value = dim_value + ind[4]
            dim_value = dim_value/len(indicadores)
            self.load(id, dim_value)
    
    def load(self, id, valor):
        query = text("""
                UPDATE dimension
                SET valor = :valor
                WHERE id = :id
            """)        
        values = {
            'valor': valor,
            'id': id
        }

        with self.db.connect() as con:
            con.execute(query, values)
            con.commit()

        


# 1- obtengo localidades
# En  base a las localidad obtengo