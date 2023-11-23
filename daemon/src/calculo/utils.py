from datetime import datetime
from sqlalchemy.sql import text
import os
# import platform
import glob

import pandas as pd
import numpy as np

dimensiones = {
    "Educacional": 1,
    "Salud": 2,
    "Seguridad": 3,
    "Tecnologia": 4,
    "Economico": 5,
    "Ecologico": 6,
    "Social": 7
}

def dataNormalize(data):
    mean = data['valor'].mean()
    dv = data['valor'].std()
    data.loc[:, 'valor'] = (data['valor'] - mean) / dv
    data.loc[:, 'valor'] = expit(data['valor'])
    return data

def expit(x):
    #Funcion logistica inversa, transforma valores a un rango entre 0 a 1 
    #En base a un logaritmo natural
    return 1 / (1 + np.exp(-x))

def getDimension(dimension):
    try:
        return dimensiones[dimension]
    except:
        return

def getDateFile(file_path):
    PATH = file_path
    creation_time = os.path.getmtime(PATH)
    date = datetime.fromtimestamp(creation_time).date()
    print(date)
    return date


def getLastFile(folderPath):
    archivos = glob.glob(os.path.join(folderPath, '*'))
    archivos.sort(key=os.path.getmtime, reverse=True)
    if archivos:
        pos = 0
        while pos < len(archivos) and os.path.isdir(archivos[pos]):
            pos += 1
        ultimo_archivo = archivos[pos]
        print("LastFile:", ultimo_archivo)
        return ultimo_archivo
    else:
        print("La carpeta está vacía." ,folderPath)

def createFolderNoProcesado(filePath, folderPath):
    os.chdir(folderPath)
    directory = "/daemon/{0}NoProcesado/".format(folderPath)
    if os.path.exists(directory) and os.path.isdir(directory):
        print(f"El directorio {directory} existe.")
    else:
        os.mkdir(directory)
        print(f"Directorio creado {directory}")
    nombre = os.path.basename(filePath)
    os.rename("/daemon/{0}".format(filePath), directory + nombre)
    print("No se puede procesar el archivo: {0}".format(filePath))
    print("Archivo agregado a la carpeta de no procesados:\n {0}{1}".format(directory,nombre))

def getExtension(file):
    _, extension = os.path.splitext(file)
    return extension

def getDateTimeFile(file_path):
    PATH = file_path
    creation_time = os.path.getmtime(PATH)
    date = datetime.fromtimestamp(creation_time)
    return date
