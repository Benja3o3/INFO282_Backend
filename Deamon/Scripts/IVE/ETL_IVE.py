import pandas as pd


class ETL:
    def __init__(self, database):
        self.nombre = "JUNAEB: IVE"
        self.extractedData = None
        self.database = database

    def __string__(self):
        return str(self.nombre)

    def Extract(self):
        PATH = "Source/IVE/IVE-2023.xlsx"
        self.extractedData = pd.read_excel(PATH, sheet_name="COMUNA", header=3)
        print(self.extractedData)

        # self.database.execute("SELECT * FROM comuna")
        # tablas = self.database.fetchall()
        # for tabla in tablas:
        #     print(tabla[0])

    def Tranform(self):
        self.extractedData = self.extractedData[
            ["ID_COMUNA_ESTABLE", self.extractedData.columns[-1]]
        ]
        self.extractedData = self.extractedData.dropna()
        self.extractedData["ID_COMUNA_ESTABLE"] = self.extractedData[
            "ID_COMUNA_ESTABLE"
        ].astype(int)

        print("Extract data")

    def Load(self):
        print("Put data on database")

    def ETLProcess(self):
        self.Extract()
        self.Tranform()
        self.Load()

        return {"OK": 200, "mesagge": "Indicators is updated successfully"}
