# import pandas as pd
# from sqlalchemy.sql import text
# import traceback
# from datetime import datetime

# class ETL:
#     def __init__(self, db, localidades):
#         self.fuente = "JUNAEB: IVE"
#         self.nombre = "IVE"        
#         self.valor = 0
#         self.PATH = "Source/BCN_TotalEstablecimientos/datos.csv"

#         self.extractedData = None
#         self.db = db
#         self.localidades = localidades
        
#     def __string__(self):
#         return str(self.nombre)

#     def Extract(self):
#         self.extractedData = pd.read_csv(self.PATH)
#         self.extractedData = self.extractedData[['Unidad territorial', ' 2022']]

#     def Tranform(self, comuna, conflict_names):
#         # Transforma datos para ser subidos a database
#         df = self.extractedData[(self.extractedData['Unidad territorial']).
#                                 str.contains(comuna['Nombre'], case=False)]
#         if(df.empty):
#             self.valor = 0
#             df = self.extractedData[(self.extractedData['Unidad territorial']).
#                                     str.contains(conflict_names[comuna['Nombre']], case=False)]            
#         self.valor = df.iloc[-1, -1]
        
#     def Load(self, comuna):
#             query = text("INSERT INTO dataenbruto (valor, nombre, fuente, fecha, comuna_id) VALUES (:valor, :nombre, :fuente, :fecha, :comuna_id)")
#             values = {
#                 'valor': float(self.valor),
#                 'nombre': self.nombre,
#                 'fuente': self.fuente,
#                 'fecha': datetime.now().date(),
#                 'comuna_id': comuna['CUT']
#             }
#             with self.db.connect() as con:
#                 con.execute(query, values)
#                 con.commit()  

#     def ETLProcess(self):
#         try:
#             self.Extract()
            
#             conflict_names = {
#                 "Paihuano": "Paiguano",
#                 "Marchigüe": "Marchihue",
#                 "Ranquil": "Ránquil",
#                 "Los Alamos": "Los Álamos",
#                 "Los Angeles": "Los Ángeles",
#                 "O'Higgins": "O’higgins"
#             }

#             comunas = self.localidades.getComunas()
#             for _, comuna in comunas.iterrows():
#                 self.Tranform(comuna, conflict_names)
#             #     self.Load(comuna)
#         except Exception:
#             traceback.print_exc()
#         return {"OK": 200, "mesagge": "Indicators is updated successfully"}

