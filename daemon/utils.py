from datetime import datetime
from sqlalchemy.sql import text
import os
import platform
import glob

import pandas as pd



def getDimension(db, dimension, cut):
    try:
        query = text("SELECT * FROM dimension WHERE comuna_id = :comuna_id AND nombre = :nombre")
        with db.connect() as con:
            data = {
                "comuna_id": cut,
                "nombre": dimension
            }
            result = con.execute(query, data)
            results_list = result.fetchall()
        return results_list[0][0]
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

