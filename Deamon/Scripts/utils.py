from datetime import datetime
from sqlalchemy.sql import text
import os
import platform


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

## OBTENER NOMBRE DEL ULTIMO ARCHIVO SUBIDO --FALTA