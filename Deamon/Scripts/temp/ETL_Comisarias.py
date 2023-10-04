# import pandas as pd
# import traceback
# from sqlalchemy.sql import text
# from datetime import datetime

# class ETL:
#     def __init__(self, db, localidades):
#         self.fuente = "Comisaria Virtual"
#         self.nombre = "Comisarias por comuna"        
#         self.valor = 0
#         self.PATH = "Source/Comisarias/comisarias.csv"

#         self.extractedData = None
#         self.db = db
#         self.localidades = localidades
        

#     def __string__(self):
#         return str(self.nombre)

#     def Extract(self):
#         self.extractedData = pd.read_csv(self.PATH, delimiter=';', encoding="latin-1")
#         self.extractedData = self.extractedData[['Id_comuna', 'Id_comisaria']]
#         self.extractedData = self.extractedData.groupby("Id_comuna").size().reset_index(name='valor')
#         # print(df)
#         # print(self.extractedData)

#     def Tranform(self, comuna):
#         # Transforma datos para ser subidos a database
#         df = self.extractedData[self.extractedData['Id_comuna'] == comuna['CUT']]
#         if(df.empty):
#             self.valor = 0
#             return
#         self.valor = df.iloc[-1, -1]
        
#     def Load(self, comuna):
#         query = text("INSERT INTO dataenbruto (valor, nombre, fuente, fecha, comuna_id) VALUES (:valor, :nombre, :fuente, :fecha, :comuna_id)")
#         values = {
#             'valor': float(self.valor),
#             'nombre': self.nombre,
#             'fuente': self.fuente,
#             'fecha': datetime.now().date(),
#             'comuna_id': comuna['CUT']
#         }
#         with self.db.connect() as con:
#             con.execute(query, values)
#             con.commit()  

#     def ETLProcess(self):
#         try:
#             self.Extract()

#             comunas = self.localidades.getComunas()
#             for _, comuna in comunas.iterrows():
#                 self.Tranform(comuna)
#                 #self.Load(comuna)
#         except Exception:
#             traceback.print_exc()

#         return {"OK": 200, "mesagge": "Indicators is updated successfully"}


