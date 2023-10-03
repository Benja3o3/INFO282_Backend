import glob
import os
import importlib
import importlib.machinery
import psycopg2
from localidades import Localidades
from sqlalchemy import create_engine

class database:
    def __init__(self, database):
        self.conn = None
        self.host = "localhost"
        self.database = database

        self.port = "3310"
        self.user = "root"
        self.password = "root"

        # self.user = "postgres"
        # self.password = "benja123"
        # self.port = "5432"
        self.conection()

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


    def create_sqlalchemy_engine(self):
            db_uri = f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"
            engine = create_engine(db_uri)
            return engine

#db transactional
dbTransaccional = database("db_transactional")
dbEngineTransaccional = dbTransaccional.create_sqlalchemy_engine()
localidadesTransaccional = Localidades(dbEngineTransaccional)

#db processing

dbProcessing = database("db_processing")
dbEngineProcessing = dbProcessing.create_sqlalchemy_engine()
localidadesProcessing = Localidades(dbEngineProcessing)


ruta_actual = os.path.dirname(__file__)
archivos_py = glob.glob(os.path.join(ruta_actual, "Scripts/*.py"), recursive=True)

etls = []  # -> Clases
etlsProcessing = []  # -> Clases

for archivo_py in archivos_py:
    nombre_modulo = os.path.splitext(os.path.basename(archivo_py))[0]
    loader = importlib.machinery.SourceFileLoader(nombre_modulo, archivo_py)
    module = loader.load_module()
    etl = module.ETL(dbEngineTransaccional, localidadesTransaccional)
    etl.ETLProcess()
    etls.append(etls)
    try:
        etlProcesing = module.ETL_Processing(dbEngineTransaccional, dbEngineProcessing, localidadesTransaccional)
        etlProcesing.ETLProcess()
        etlsProcessing.append(etlProcesing)
    except:
         print("No existe ETL PROCESSING")
        