from datetime import datetime
from sqlalchemy.sql import text
import os
import platform
import glob

import pandas as pd


def getDimension(db, dimension, cut):
    query = text("SELECT * FROM dimension WHERE comuna_id = :comuna_id AND nombre = :nombre")
    values = {
        'comuna_id': cut,
        'nombre': dimension
    }
    with db.connect() as con:
        result = con.execute(query, values)
        results_list = result.fetchall()
    return results_list[0][0]

def getDateFile(file_path):
    if platform.system() == 'Windows':
        creation_time = os.path.getctime(file_path)
    else:
        creation_time = os.path.getmtime(file_path)

    date = datetime.fromtimestamp(creation_time).date()
    return date

def getLastFile(folderPath):
    carpeta = folderPath 
    archivos = glob.glob(os.path.join(carpeta, '*'))
    archivos.sort(key=os.path.getctime, reverse=True)
    if archivos:
        ultimo_archivo = archivos[0]
        return ultimo_archivo
    else:
        print("La carpeta está vacía.")

