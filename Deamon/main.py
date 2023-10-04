import glob
import os
import importlib
import importlib.machinery
import sys
import psycopg2
#from dotenv import load_dotenv

from localidades import Localidades
from sqlalchemy import create_engine



class database:
    def __init__(self, database):
        #load_dotenv()
        self.conn = None
        self.database = database

        # self.host = os.getenv('HOST_DAEMON')
        # self.port = os.getenv('PORT_DAEMON')
        # self.user = os.getenv('USER_DAEMON')
        # self.password = os.getenv('PASSWORD_DAEMON')
        self.host = "localhost"
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


#Primera ejecucion
etls = []  # -> Clases
etlsProcessing = []  # -> Clases

for archivo_py in archivos_py:
    nombre_modulo = os.path.splitext(os.path.basename(archivo_py))[0]
    utils_dir = os.path.dirname(os.path.abspath(__file__))
    utils_dir = os.path.join(utils_dir, './')

    # Agrega la ruta al sys.path
    sys.path.append(utils_dir)

    loader = importlib.machinery.SourceFileLoader(nombre_modulo, archivo_py)
    module = loader.load_module()


    # try:
    etl = module.ETL(dbEngineTransaccional, localidadesTransaccional)
    result = etl.ETLProcess()
    etls.append(etls)

    if(result == False):    
        etlProcesing = module.ETL_Processing(dbEngineTransaccional, dbEngineProcessing, localidadesTransaccional)
        etlProcesing.ETLProcess()
        etlsProcessing.append(etlProcesing)
    else:
        print("Datos ya actualizados")

    # except:
    #      print("No existe ETL")
        