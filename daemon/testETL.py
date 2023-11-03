import glob
import os
import importlib
import importlib.machinery
import sys
import traceback
import psycopg2
#from dotenv import load_dotenv

from src.db.localidades import Localidades
from sqlalchemy import create_engine
from src.calculo.calculoRegiones import calculoRegiones
from src.calculo.calculoPais import calculoPais
from src.calculo.calculoComunas import calculoComunas
from src.db import db
from src.db import dbQuerys

#Cambio de directorio
directorio_padre = os.path.dirname(os.getcwd())
nuevo_directorio = os.path.join(os.getcwd(),directorio_padre)
os.chdir(nuevo_directorio)

daemon_folder = os.path.join(os.getcwd(),"./daemon")
os.chdir(daemon_folder)

#databases
dbTransaccional = db.database("db_transactional")
dbEngineTransaccional = dbTransaccional.create_sqlalchemy_engine()
localidadesTransaccional = Localidades(dbEngineTransaccional)

dbProcessing = db.database("db_processing")
dbEngineProcessing = dbProcessing.create_sqlalchemy_engine()

querys = dbQuerys.Querys(dbEngineTransaccional, dbEngineProcessing)

ruta_actual = os.path.dirname(__file__)
archivos_py = glob.glob(os.path.join(ruta_actual, "Scripts/*.py"), recursive=True)



nombreETL = input("Ingrese nombre del archivo ETL (sin .py): ")
utils_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(utils_dir)
dirETL = "/daemon/src/etls/" + nombreETL + ".py"
loader = importlib.machinery.SourceFileLoader(nombreETL, dirETL)
module = loader.load_module()

try:
    #ETL Transactional
    etl = module.ETL_Transactional(querys, localidadesTransaccional)
    etl.ETLProcess()

    #ETL Processing
    etlProcesing = module.ETL_Processing(querys, localidadesTransaccional)
    etlProcesing.ETLProcess()        
            
except Exception:
    traceback.print_exc()
