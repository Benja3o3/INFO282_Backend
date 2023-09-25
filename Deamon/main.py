import glob
import os
import importlib
import importlib.machinery
import psycopg2

# from ..Deamon.Scripts.IVE.ETL_IVE import ETL_Trans


class database:
    def __init__(self):
        self.conn = None
        self.host = "localhost"
        self.database = "db_transactional"

        self.port = "3310"
        self.user = "root"
        self.password = "root"

        # self.user = "postgres"
        # self.password = "benja123"
        # self.port = "5432"

    def conection(self):
        # # Parámetros de conexión
        parametros = {
            "host": self.host,
            "port": self.port,
            "user": self.user,
            "password": self.password,
            "database": self.database,
        }
        self.conn = psycopg2.connect(**parametros)

    def connectionProcess(self):
        self.conection()
        return {"OK": 200, "mesagge": "Connection DB success"}


db = database()
db.connectionProcess()


ruta_actual = os.path.dirname(__file__)
archivos_py = glob.glob(os.path.join(ruta_actual, "Scripts/**/*.py"), recursive=True)

etls = []  # -> Clases

for archivo_py in archivos_py:
    nombre_modulo = os.path.splitext(os.path.basename(archivo_py))[0]
    loader = importlib.machinery.SourceFileLoader(nombre_modulo, archivo_py)
    module = loader.load_module()
    etl = module.ETL(db.conn.cursor())
    etl.ETLProcess()
    etls.append(etls)
