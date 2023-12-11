import glob
import os
import importlib
import importlib.machinery
import sys
import traceback

# import psycopg2
#from dotenv import load_dotenv

from src.db.localidades import Localidades
from sqlalchemy import create_engine
# from src.calculo.calculoRegiones import calculoRegiones
# from src.calculo.calculoPais import calculoPais
# from src.calculo.calculoComunas import calculoComunas
from src.db import db
from src.db import dbQuerys

#Cambio de directorio
if(len(sys.argv) != 2):
    print("Run python3 testETL.py <name>")
    exit()

nombreETL = sys.argv[1]
directorio_padre = os.path.dirname(os.getcwd())
nuevo_directorio = os.path.join(os.getcwd(),directorio_padre)
os.chdir(nuevo_directorio)

daemon_folder = os.path.join(os.getcwd(),"./daemon")
os.chdir(daemon_folder)

#databases
try:
    dbTransaccional = db.database("db_transactional")
    dbEngineTransaccional = dbTransaccional.create_sqlalchemy_engine()
    localidadesTransaccional = Localidades(dbEngineTransaccional)

    dbProcessing = db.database("db_processing")
    dbEngineProcessing = dbProcessing.create_sqlalchemy_engine()

    querys = dbQuerys.Querys(dbEngineTransaccional, dbEngineProcessing)

except Exception:
    print("No se logro conectar con las bases de datos")
    traceback.print_exc()
    exit()

ruta_actual = os.path.dirname(__file__)
archivos_py = glob.glob(os.path.join(ruta_actual, "Scripts/*.py"), recursive=True)


# nombreETL = input("Ingrese nombre del archivo ETL (sin .py): ")
utils_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(utils_dir)
dirETL = "/daemon/src/etls/" + nombreETL + ".py"
loader = importlib.machinery.SourceFileLoader(nombreETL, dirETL)
module = loader.load_module()

try:
    #ETL Transactional
    etl = module.ETL_Transactional(querys, localidadesTransaccional)
    etl.ETLProcess()
    print("Paso ETL_Transactional")
    #ETL Processing
    etlProcesing = module.ETL_Processing(querys, localidadesTransaccional)
    etlProcesing.ETLProcess()        
            
except Exception:
    print("No se logro procesar la ETL")
    traceback.print_exc()
