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
from src.calculo.dimensiones import Dimensiones
from src.calculo.bienestar import Bienestar
from src.calculo.calculoRegiones import calculoRegiones
from src.calculo.calculoPais import calculoPais

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
archivos_py = glob.glob(os.path.join(ruta_actual, "src/etls/*.py"), recursive=True)


for archivo_py in archivos_py:
    nombre_modulo = os.path.splitext(os.path.basename(archivo_py))[0]
    utils_dir = os.path.dirname(os.path.abspath(__file__))
    utils_dir = os.path.join(utils_dir, './')

    # Agrega la ruta al sys.path
    sys.path.append(utils_dir)

    loader = importlib.machinery.SourceFileLoader(nombre_modulo, archivo_py)
    module = loader.load_module()
    try:
        etl = module.ETL_Transactional(querys, localidadesTransaccional)
        isUpdated = etl.ETLProcess()
        
        if(isUpdated == False):
            etlProcesing = module.ETL_Processing(querys, localidadesTransaccional)
            etlProcesing.ETLProcess()        
                    
        
    except Exception:
        traceback.print_exc()
    print(">")
        
# Calculo dimensiones
calculoDimensiones = Dimensiones(dbEngineProcessing, localidadesTransaccional)
calculoBienestar = Bienestar(dbEngineProcessing, localidadesTransaccional)

# Calculo externos
calculoRegiones = calculoRegiones(dbEngineProcessing, localidadesTransaccional)
calculoPais = calculoPais(dbEngineProcessing, localidadesTransaccional)
