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
    PATH = file_path
    creation_time = os.path.getmtime(PATH)
    date = datetime.fromtimestamp(creation_time).date()
    print(date)
    return date

def getLastFile(folderPath):
    archivos = glob.glob(os.path.join("./Source/IVE/", '*'))
    archivos.sort(key=os.path.getmtime, reverse=True)
    if archivos:
        ultimo_archivo = archivos[0]
        print("LastFile:", ultimo_archivo)
        return ultimo_archivo
    else:
        print("La carpeta está vacía." ,folderPath)


# def getRouteType(file_path):
#     newpath = ""
#     linux_pathtype = getTypePath(file_path)
#     if(platform.system() == 'Windows' and linux_pathtype):
#         for i in file_path:
#             if(i == '/'):
#                newpath += "\\"
#             else:
#                newpath += i
#     elif(not linux_pathtype):
#         for i in file_path:
#             if(i == "\\"):
#                newpath = '/'
#             else:
#                newpath = i
#     return newpath

# # Si es un file_path para ubuntu retorna true, sino false
# def getTypePath(file_path): 
#     i = 0
#     while (i < len(file_path) and file_path[i] != '/'):
#         i += 1
#     if( i < len(file_path)):
#         return True
#     return False

# a = getTypePath(./a/hola)
# print(str(a))