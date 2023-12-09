import glob
import os
import importlib
import importlib.machinery
import sys
import traceback
import time

# import psycopg2
#from dotenv import load_dotenv

from src.db.localidades import Localidades
from sqlalchemy import create_engine
from src.calculo.calculoRegiones import calculoRegiones
from src.calculo.calculoPais import calculoPais
from src.calculo.calculoComunas import calculoComunas
from src.db import db
from src.db import dbQuerys
import concurrent.futures

#Cambio de directorio
inicio = time.time()
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
except:
    print("No se pudo conectar con las bases de datos")
    traceback.print_exc()
    exit()
    
ruta_actual = os.path.dirname(__file__)
archivos_py = glob.glob(os.path.join(ruta_actual, "src/etls/*.py"), recursive=True)

# for archivo_py in archivos_py:
#     nombre_modulo = os.path.splitext(os.path.basename(archivo_py))[0]
#     utils_dir = os.path.dirname(os.path.abspath(__file__))
#     utils_dir = os.path.join(utils_dir, './')
#     # Agrega la ruta al sys.path
#     sys.path.append(utils_dir)

#     loader = importlib.machinery.SourceFileLoader(nombre_modulo, archivo_py)
#     module = loader.load_module()
#     try:
#         etl = module.ETL_Transactional(querys, localidadesTransaccional)
#         etl.ETLProcess()
    
#         etlProcesing = module.ETL_Processing(querys, localidadesTransaccional)
#         etlProcesing.ETLProcess()        
#     except Exception:
#         print("No se logro procesar la ETL:", archivo_py)
#         traceback.print_exc()
#     print(">---------------------------------------------------------------------------------------")
    
    
    
def process_file(archivo_py):
    nombre_modulo = os.path.splitext(os.path.basename(archivo_py))[0]
    utils_dir = os.path.dirname(os.path.abspath(__file__))
    utils_dir = os.path.join(utils_dir, './')
    # Agrega la ruta al sys.path
    sys.path.append(utils_dir)

    loader = importlib.machinery.SourceFileLoader(nombre_modulo, archivo_py)
    module = loader.load_module()

    try:
        etl = module.ETL_Transactional(querys, localidadesTransaccional)
        etl.ETLProcess()

        etlProcesing = module.ETL_Processing(querys, localidadesTransaccional)
        etlProcesing.ETLProcess()        
    except Exception:
        print("No se logró procesar la ETL:", archivo_py)
        traceback.print_exc()
    print(">---------------------------------------------------------------------------------------")   

max_threads = os.cpu_count()
num_threads = min(8, max_threads) if max_threads else 1

with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
    executor.map(process_file, archivos_py)



    
print("--- Recopilacion de datos de archivos completados ---")
fin = time.time()
print("--- Time: {0} secs ---".format(fin-inicio)) 
# Calculo dimensiones
# Calculo externos
inicio = time.time()
calculoComunas = calculoComunas(dbEngineProcessing, localidadesTransaccional)
print("Calculo comunas completado")
calculoRegiones = calculoRegiones(dbEngineProcessing, localidadesTransaccional)
print("Calculo regiones completado")
calculoPais = calculoPais(dbEngineProcessing, localidadesTransaccional)
print("Calculo pais completado")
fin = time.time()
print("--- Time Calculo: {0} secs ---".format(fin-inicio)) 