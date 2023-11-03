from datetime import datetime
from sqlalchemy.sql import text
import os
import platform
import glob

import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler
from scipy.special import expit

dimensiones = {
    "Educacional": 1,
    "Salud": 2,
    "Seguridad": 3,
    "Tecnologia": 4,
    "Economico": 5,
    "Ecologico": 6,
    "Movilidad": 7,
    "Diversion": 8
}

def dataNormalize(data):
    scaler = StandardScaler()
    data.loc[:, 'valor'] = scaler.fit_transform(data[['valor']])
    data.loc[:, 'valor'] = expit(data['valor'])

    return data

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
        ultimo_archivo = archivos[0]
        print("LastFile:", ultimo_archivo)
        return ultimo_archivo
    else:
        print("La carpeta está vacía." ,folderPath)
