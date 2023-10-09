import pandas as pd
import traceback
from datetime import datetime
from sqlalchemy.sql import text
from utils import getDimension
from utils import getDateFile
from utils import getLastFile


class ETL_Transactional:
    def __init__(self, db, localidades):
        self.fuente = "Subtel: Antenas"
        self.nombre = "Numero de antenas"
        self.valor = 0

        # FILE
        self.FOLDER = "Source/Subtel_antenas/"
        self.PATH = getLastFile(self.FOLDER)
        self.uploadDate = getDateFile(self.PATH)
        print(self.PATH)

        self.extractedData = None
        self.db = db
        self.localidades = localidades

    def __string__(self):
        return str(self.nombre)

    def Extract(self):
        self.extractedData = pd.read_csv(self.PATH, delimiter=";")
        self.extractedData = self.extractedData[["Codigo comuna", "conectividad"]]
        self.extractedData = self.extractedData.dropna()
        self.extractedData["Codigo comuna"] = self.extractedData[
            "Codigo comuna"
        ].astype(int)

    def Tranform(self, comuna):
        df = self.extractedData[self.extractedData["Codigo comuna"] == comuna["CUT"]]
        if df.empty:
            self.valor = 0
            return
        self.valor = df.iloc[-1, -1]

    def Load(self, comuna):
        query = text(
            "INSERT INTO dataenbruto (valor, nombre, fuente, fecha, comuna_id) VALUES (:valor, :nombre, :fuente, :fecha, :comuna_id)"
        )
        values = {
            "valor": float(self.valor),
            "nombre": self.nombre,
            "fuente": self.fuente,
            "fecha": self.uploadDate,
            "comuna_id": comuna["CUT"],
        }
        with self.db.connect() as con:
            con.execute(query, values)
            con.commit()

    def ETLProcess(self):
        query = text("SELECT MAX(fecha) FROM dataenbruto WHERE fuente = :fuente")
        query = query.bindparams(fuente=self.fuente)
        result = []
        with self.db.connect() as con:
            result = con.execute(query)
            rows = result.fetchall()
        result = rows[0][0]
        if result == None or self.uploadDate > result:
            try:
                self.Extract()
                comunas = self.localidades.getComunas()
                for _, comuna in comunas.iterrows():
                    self.Tranform(comuna)
                    self.Load(comuna)
            except KeyError as error:
                print(error)
        else:
            print("Datos en bruto ya actualizados: ", self.fuente)
            return False  # Si estaban procesados
        return False  # No estaban procesados


class ETL_Processing:
    def __init__(self, dbTransaccional, dbProcessing, localidades):
        # Constructor
        self.fuente = "Subtel: Antenas"
        self.nombreIndicador = "Numero de antenas"
        self.dimension = "Tecnologia"
        self.prioridad = 1
        self.valor = 0

        # Constructor
        self.dbTransaccional = dbTransaccional
        self.dbProcessing = dbProcessing
        self.localidades = localidades

        # Data
        self.transaccionalData = None

    def __string__(self):
        return str(self.nombreIndicador)

    def Extract(self):
        # Codigo generico, no tocar
        query = text("SELECT * FROM dataenbruto WHERE fuente = :fuente")
        query = query.bindparams(fuente=self.fuente)
        result = []
        with self.dbTransaccional.connect() as con:
            result = con.execute(query)
            rows = result.fetchall()
            column_names = result.keys()
        df = pd.DataFrame(columns=column_names)
        for row in rows:
            row_dict = {
                column_name: value for column_name, value in zip(column_names, row)
            }
            df = pd.concat([df, pd.DataFrame(row_dict, index=[0])], ignore_index=True)
        self.transaccionalData = df
        max_date = self.transaccionalData["fecha"].max()
        self.transaccionalData = self.transaccionalData[
            self.transaccionalData["fecha"] == max_date
        ]
        return

    def Tranform(self, comuna):
        # Calculo indicador IVE
        df = self.transaccionalData[
            self.transaccionalData["comuna_id"] == comuna["CUT"]
        ]

        if df.empty:
            self.valor = 0
        self.valor = (df["valor"].tail(1).iloc[0] / comuna["Area"]) * 100
        # print(self.valor)
        return

    def Load(self, comuna):
        query = text(
            "INSERT INTO indicador (nombre, prioridad, fuente, valor, fecha, dimension_id) VALUES (:nombre, :prioridad, :fuente, :valor, :fecha ,:dimension_id)"
        )
        values = {
            "nombre": self.nombreIndicador,
            "prioridad": self.prioridad,
            "fuente": self.fuente,
            "valor": float(self.valor),
            "fecha": datetime.now().date(),
            "dimension_id": getDimension(
                self.dbProcessing, self.dimension, comuna["CUT"]
            ),
        }
        with self.dbProcessing.connect() as con:
            con.execute(query, values)
            con.commit()

    def ETLProcess(self):
        try:
            self.Extract()
            comunas = self.localidades.getComunas()
            for _, comuna in comunas.iterrows():
                self.Tranform(comuna)
                self.Load(comuna)
        except Exception:
            traceback.print_exc()

        return {"OK": 200, "mesagge": "Indicators is updated successfully"}