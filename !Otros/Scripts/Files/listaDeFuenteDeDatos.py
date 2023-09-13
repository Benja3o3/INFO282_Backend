from typing import Self
from ..controladorDeFuentesDeDatos.fuenteDeDatos import FuenteDeDatos
from pandas import pandas as pd
from ...modeloCoreEtl.indicador import Indicador
from ...modeloCoreEtl.datosDurosDeFuente import DatosDurosDeFuente
from ...modeloCoreEtl.localidades import Localidades


# conflict_comunas = {
#            "La Calera" : "Calera" ,
#            "Paihuano" : "Paiguano",
#            "Llay-Llay": "Llaillay",
#            "O'Higgins": "O’higgins",
#            "Til Til" : "Tiltil"
#        }


Localidades = Localidades()
# comunas = Localidades.obtenerComunas()
conflict_comunas = Localidades.obtenerComunasNombreConflicto()
# regiones = Localidades.obtenerRegiones()
conflicts_regions = Localidades.obtenerRegionesNombreConflicto()


class INEPoblacionDosPuntoCuatro(FuenteDeDatos):
    def __init__(self):
        self.nombre = "Censo 2017 INE 2.4 Migración"
        self.datosExtraidos = None
        self.valor = 0
        self.datos = None 

        
    def __str__(self):
        return str(self.nombre)

    def establecerLocalidades(self, localidades):
        self.localidades = localidades

    def extraerDatos(self,file):
        self.datosExtraidos = pd.read_excel(file,sheet_name="Región", header=2)

    def transformarDatos(self,region, _elemDict):
        # Fuente: Censo 2017 INE 2.4 Migración
        # Indicador: Infrastructure capacity
        # Tema: Socio-demographics characteristics
        # Dimension: Socio comunitaria

        self.datos = self.datosExtraidos[
            self.datosExtraidos['NOMBRE REGIÓN \nRESIDENCIA HABITUAL ACTUAL'].str.contains(
                region["Región"].upper())]
        if self.datos.empty == True:
            self.datos = self.datosExtraidos[
                        self.datosExtraidos["NOMBRE REGIÓN \nRESIDENCIA HABITUAL ACTUAL"] 
                        == _elemDict[region["Región"]]]
        self.valor = float(self.datos.iloc[:, -1].values[0])
        nombreRegion = self.datos["NOMBRE REGIÓN \nRESIDENCIA HABITUAL ACTUAL"].iloc[0]
        self.datos = self.datos.rename(columns=lambda x: x.replace('\n', ''))
        self.datos = self.datos.rename(columns=lambda x: x.replace('’', "'"))
        totalPoblacion = self.datos["TOTAL POBLACIÓN EFECTIVAMENTE CENSADA"].iloc[0]
        self.datos = self.datos.drop(columns=nombreRegion)
        self.datos = self.datos.iloc[:, 7:-3]
        sumMigration = self.datos.iloc[0].sum()
        self.valor = float((sumMigration / totalPoblacion))*100



    def cargarIndicadores(self,region):
        indicador = Indicador("D-SCM-SO1_6726-T-61", Comuna = region["Región"].upper())
        indicador.guardarIndicadorEnBaseDeDatos(self.valor)

    def cargarDatosDuros(self, region):
        datosDuros = DatosDurosDeFuente(
            nombreFuente="Censo 2017 INE 2.4 Migración",
            nombreLocalidad=region["Región"].upper(),
            datos=self.datos.to_dict("records"),
        )
        datosDuros.guardarDatosDuros()

    def EtlDeDatos(self,file):
        #try:
            self.extraerDatos(file) # Extraer
            _elemDict = {
                "Magallanes y Antártica Chilena" : "MAGALLANES Y DE LA ANTÁRTICA CHILENA",
                "Lib. Gral. Bernardo O'Higgins" : "LIBERTADOR GENERAL BERNARDO O'HIGGINS",
            }
            for region in Localidades.obtenerRegiones():
                self.transformarDatos(region, _elemDict) # Transformar
                self.cargarIndicadores(region) # Cargar
                self.cargarDatosDuros(region)  # Cargar
        #except:
        #    return "Error"

            return 200

class CASENVivienda(FuenteDeDatos):
    def __init__(self):
        self.nombre = "Vivienda Casen 2017"
        self.datosExtraidos = None
        self.valor = 0
        self.datos = None 
        self.df = None
        self.index_2006 = 0
        
    def __str__(self):
        return str(self.nombre)

    def establecerLocalidades(self, localidades):
        self.localidades = localidades

    def extraerDatos(self,file):
        self.datosExtraidos = pd.read_excel(file, sheet_name="2", header=4)
        self.index_2006 = self.datosExtraidos.columns.get_loc("2006.1")
        self.df = self.datosExtraidos.iloc[:, :self.index_2006].copy()
        self.df = self.df.iloc[:self.df['NIVEL DE AGREGACIÓN'].isnull().idxmax(), :]
        self.df = self.df[self.df['NIVEL DE AGREGACIÓN'] == 'Región'].copy()
        self.df = self.df[self.df['CATEGORÍA'] == 'Propio'].copy()
        self.datosExtraidos = self.df[['ÁREA / GRUPO DE POBLACIÓN', self.df.columns[-1]]]


    def transformarDatos(self,region, _elemDict):
        # Indicador: Home ownership
        # Fuente: Vivienda Casen 2017
        # Tema: Home/Land ownership
        # Dimension: Económica

        self.datos = self.datosExtraidos[self.datosExtraidos['ÁREA / GRUPO DE POBLACIÓN'].str.contains(region["Región"])]
        if len(self.datos) == 0:
            print(region['Región'])
            self.datos = self.datosExtraidos[
                        self.datosExtraidos["ÁREA / GRUPO DE POBLACIÓN"] 
                        == _elemDict[region["Región"]]]
        self.valor = float(self.datos.iloc[:, -1].values[0])
        print(region['Región'], self.valor)


    def cargarIndicadores(self,region):
        indicador = Indicador("D-ENM-H/7_3536-T-164", Comuna = region["Región"].upper())
        indicador.guardarIndicadorEnBaseDeDatos(self.valor)

    def cargarDatosDuros(self, region):
        datosDuros = DatosDurosDeFuente(
            nombreFuente="Vivienda Casen 2017",
            nombreLocalidad=region["Región"].upper(),
            datos=self.datos.to_dict("records"),
        )
        datosDuros.guardarDatosDuros()

    def EtlDeDatos(self,file):
        #try:
            self.extraerDatos(file) # Extraer
            _elemDict = {
                "Lib. Gral. Bernardo O'Higgins" : "O Higgins",
                "Metropolitana de Santiago": "Metropolitana",
                "Aysén del General Carlos Ibáñez del Campo": "Aysén",
                "Magallanes y Antártica Chilena": "Magallanes"

            }
            for region in Localidades.obtenerRegiones():
                self.transformarDatos(region, _elemDict) # Transformar
                self.cargarIndicadores(region) # Cargar
                self.cargarDatosDuros(region)  # Cargar
        #except:
        #    return "Error"

            return 200


class JUNAEBIVE(FuenteDeDatos):
    def __init__(self):
        self.nombre = "JUNAEB: IVE"
        self.datosExtraidos = None
        self.valor = 0
        self.datos = None 
        
    def __str__(self):
        return str(self.nombre)

    def establecerLocalidades(self, localidades):
        self.localidades = localidades

    def extraerDatos(self,file):
        self.datosExtraidos = pd.read_excel(file, sheet_name="COMUNA", header=3)
        self.datosExtraidos = self.datosExtraidos[['ID_COMUNA_ESTABLE',  self.datosExtraidos.columns[-1] ]]
        self.datosExtraidos = self.datosExtraidos.dropna()
        self.datosExtraidos['ID_COMUNA_ESTABLE'] = self.datosExtraidos['ID_COMUNA_ESTABLE'].astype(int)

    def transformarDatos(self,comuna):
        # Indicador: INDICE VULNERABILIDAD
        # Fuente: JUNAEB HYYP://WWW.JUNAEB.CL/IVE
        # Tema: Income
        # Dimension: Económica
        self.datos = self.datosExtraidos[self.datosExtraidos['ID_COMUNA_ESTABLE'] == comuna['CUT ']]
        if self.datosExtraidos.empty == False:
            try:
                self.valor = float(self.datos[self.datos.columns[-1]].iloc[0])
            except:
                self.valor = 0
                return

    def cargarIndicadores(self,comuna):
        indicador = Indicador("D-ENM-IM5_5961-T-154", Comuna = comuna["Nombre"].upper())
        indicador.guardarIndicadorEnBaseDeDatos(self.valor)

    def cargarDatosDuros(self, comuna):
        datosDuros = DatosDurosDeFuente(
            nombreFuente="JUNAEB: IVE",
            nombreLocalidad=comuna["Nombre"].upper(),
            datos=self.datos.to_dict("records"),
        )
        datosDuros.guardarDatosDuros()

    def EtlDeDatos(self,file):
        #try:
            self.extraerDatos(file) # Extraer

            for comuna in Localidades.obtenerComunas():
                self.transformarDatos(comuna) # Transformar
                self.cargarIndicadores(comuna) # Cargar
                self.cargarDatosDuros(comuna)  # Cargar
        #except:
        #    return "Error"

            return 200
    

class MonumentosNacionales(FuenteDeDatos):
    def __init__(self):
        self.nombre = "Consejo de monumentos nacionales de Chile (localización) "
        self.datosExtraidos = None
        self.valor = 0
        self.datos = None 
        
    def __str__(self):
        return str(self.nombre)

    def establecerLocalidades(self, localidades):
        self.localidades = localidades

    def extraerDatos(self,file):
        self.datosExtraidos = pd.read_excel(file, sheet_name=0, header=4)
        self.datosExtraidos = self.datosExtraidos.drop(index=0).reset_index(drop=True)
        self.datosExtraidos = self.datosExtraidos[['Región', '%']]

    def transformarDatos(self,region, _elemDict):
        # Indicador: Cultural and historic preservation
        # Fuente: Consejo de monumentos nacionales de Chile (localización) 
        # Tema: Historical and cultural values
        # Dimension: Socio Comunitaria
        self.datos = self.datosExtraidos[
                self.datosExtraidos['Región'].str.contains(region['Región'].split()[-1])]
        if self.datos.empty == True:
            self.datos = self.datosExtraidos[
            self.datosExtraidos['Región'].str.contains(_elemDict[region['Región']])]
        self.valor = float(self.datos['%'].iloc[0])*100

    def cargarIndicadores(self,region):
        indicador = Indicador("D-SCM-HO3_8608-T-88", Comuna = region["Región"].upper())
        indicador.guardarIndicadorEnBaseDeDatos(self.valor)

    def cargarDatosDuros(self, region):
        datosDuros = DatosDurosDeFuente(
            nombreFuente="Consejo de monumentos nacionales de Chile (localización) ",
            nombreLocalidad=region["Región"].upper(),
            datos=self.datos.to_dict("records"),
        )
        datosDuros.guardarDatosDuros()

    def EtlDeDatos(self,file):
        #try:
            self.extraerDatos(file) # Extraer
            _elemDict = {
                "Lib. Gral. Bernardo O'Higgins" : "Libertador General Bernardo O’Higgins"
            }
            for region in Localidades.obtenerRegiones():
                self.transformarDatos(region, _elemDict) # Transformar
                self.cargarIndicadores(region) # Cargar
                self.cargarDatosDuros(region)  # Cargar
        #except:
        #    return "Error"

            return 200

class ONEMIAcademiaDeProteccionCivil(FuenteDeDatos):
    def __init__(self):
        self.nombre = "Estadísticas ONEMI Academia de Protección Civil"
        self.datosExtraidos = None
        self.valor = 0
        self.percent = 0
        self.participantes = 0
        self.totalParticipantes = 0
        self.datos = None 
    def __str__(self):
        return str(self.nombre)

    def establecerLocalidades(self, localidades):
        self.localidades = localidades

    def extraerDatos(self,file):
        self.datosExtraidos = pd.read_excel(file, sheet_name="ACADEMIA_FORM_ESPECIA_COMU", header=0)
        self.datosExtraidos = self.datosExtraidos.dropna(subset=['COMUNA'])
        
        self.totalParticipantes = float(self.datosExtraidos["PARTICIPANTES"].sum())
        self.datosExtraidos = self.datosExtraidos[['COMUNA', 'PARTICIPANTES']]

    def transformarDatos(self,comuna, conflict_comunas):
        # Indicador: Participation in training on emergency management and evacuation drills
        # Fuente: Estadísticas ONEMI Academia de Protección Civil
        # Tema: Educative and informative actions
        # Dimension: Institucional

        self.datos = self.datosExtraidos[self.datosExtraidos['COMUNA'].str.contains(comuna['Nombre'].upper())]
        if len(self.datos) == 0:
            try:
                self.datos = self.datosExtraidos[
                            self.datosExtraidos['COMUNA'] 
                            == conflict_comunas[comuna["Nombre"]]]
            except KeyError:
                self.valor = 0
                return

        self.participantes = self.datos['PARTICIPANTES'].sum()
        self.valor = (self.participantes/self.totalParticipantes)*100


    def cargarIndicadores(self,comuna):
        indicador = Indicador("D-INS-EA5_3676-T-129", Comuna = comuna["Nombre"].upper())
        indicador.guardarIndicadorEnBaseDeDatos(self.valor)

    def cargarDatosDuros(self, comuna):
        datosDuros = DatosDurosDeFuente(
            nombreFuente="Estadísticas ONEMI Academia de Protección Civil",
            nombreLocalidad=comuna["Nombre"].upper(),
            datos=self.datos.to_dict("records"),
        )
        datosDuros.guardarDatosDuros()

    def EtlDeDatos(self,file):
        #try:
            self.extraerDatos(file) # Extraer
            _elemDict = {
                'General Lagos': 'GRAL. LAGOS',
                'Taltal':'TAL TAL',
                'Ollagüe':'OLLAGUE',
                'Santo Domingo':'STO. DOMINGO',
                'Santa María' :'STA. MARIA',
                'Cholchol' : 'CHOL CHOL',
                'Coyhaique' : 'COIHAYQUE',
                'Río Ibáñez' : 'RÍO IBAÑEZ',
                "Macul" : "MACÚL",
                'Paihuano': 'Paiguano',
                'La Calera': 'Calera',
                'Llay-Llay': 'Llaillay',
                "O'Higgins": "O´Higgins"
            }

            for comuna in Localidades.obtenerComunas():
                self.transformarDatos(comuna,_elemDict) # Transformar
                self.cargarIndicadores(comuna) # Cargar
                self.cargarDatosDuros(comuna)  # Cargar
        #except:
        #    return "Error"

            return 200


class INESistemaDeIndicadores(FuenteDeDatos):
    def __init__(self):
        self.nombre = "INE. Sistema de Indicadores y Estándares de Desarrollo Urbano"
        
        self.df_counmem = None
        self.counmenValue = 0

        self.df_infcap = None
        self.infcapValue = 0

        self.datosCounmen = None
        self.datosInfcap = None

        self.empty_columns = None


    def __str__(self):
        return str(self.nombre)

    def establecerLocalidades(self, localidades):
        self.localidades = localidades

    def extraerDatos(self,file):
        self.df_counmem = pd.read_excel(file, sheet_name="IG_90_I", header=2)
        self.df_counmem = self.df_counmem[['CUT', 'IG_90 Porcentaje de participiación']]

        self.df_infcap = pd.read_excel(file, sheet_name="IP_47_IC", header=2)
        self.df_infcap = self.df_infcap.dropna(axis = 1, how = 'all')
        self.df_infcap = self.df_infcap[['Cod_Ciudad', self.df_infcap.columns[-1]]]
    
    def transformarDatos(self,comuna):
        self.datosCounmen = self.df_counmem[self.df_counmem['CUT'] == comuna['CUT ']]
        if self.datosCounmen.empty == False:
            self.counmenValue = float(self.datosCounmen['IG_90 Porcentaje de participiación'].iloc[0])
        else:
            self.counmenValue = 0
        
        self.datosInfcap = self.df_infcap[self.df_infcap['Cod_Ciudad'] == comuna['CUT ']]
        if self.datosInfcap.empty == False:
            self.empty_columns = self.datosInfcap.columns[self.datosInfcap.isnull().all()]
            self.datosInfcap = self.datosInfcap.drop(columns=self.empty_columns)
            self.infcapValue = float(self.datosInfcap.iloc[0, -1])
        else: 
            self.infcapValue = 0

    def cargarIndicadores(self,comuna):
        indicadorCounmem = Indicador("D-SCM-LI5_3741-T-93", Comuna = comuna["Nombre"].upper())
        indicadorCounmem.guardarIndicadorEnBaseDeDatos(self.counmenValue)

        indicadorInfCap = Indicador("D-FIS-CI2_3630-T-30", Comuna = comuna["Nombre"].upper())
        indicadorInfCap.guardarIndicadorEnBaseDeDatos(self.infcapValue)

    def cargarDatosDuros(self, comuna):
        datosDurosCounmen = DatosDurosDeFuente(
            nombreFuente="INE. Sistema de Indicadores y Estándares de Desarrollo Urbano",
            nombreLocalidad=comuna["Nombre"].upper(),
            datos=self.datosCounmen.to_dict("records"),
        )
        datosDurosCounmen.guardarDatosDuros()

        datosDurosInfcap = DatosDurosDeFuente(
            nombreFuente="INE. Sistema de Indicadores y Estándares de Desarrollo Urbano",
            nombreLocalidad=comuna["Nombre"].upper(),
            datos=self.datosInfcap.to_dict("records"),
        )
        datosDurosInfcap.guardarDatosDuros()

    def EtlDeDatos(self,file):
        #try:
            self.extraerDatos(file) # Extraer

            for comuna in Localidades.obtenerComunas():
                self.transformarDatos(comuna) # Transformar
                self.cargarIndicadores(comuna) # Cargar
                self.cargarDatosDuros(comuna)  # Cargar
        #except:
        #    return "Error"

            return 200



class INEPermisoDeCirculacion(FuenteDeDatos):
    def __init__(self):
        self.nombre = "INE. Base de datos permisos de circulación"
        self.datosExtraidos = None
        self.valor = 0
        self.motorizados = 0
        self.datos = None 
    def __str__(self):
        return str(self.nombre)

    def establecerLocalidades(self, localidades):
        self.localidades = localidades

    def extraerDatos(self,file):
        self.datosExtraidos = pd.read_excel(file, sheet_name=3, header=3)
        self.datosExtraidos.set_index(self.datosExtraidos.columns[1], inplace=True)
        num_rows = len(self.datosExtraidos['Motorizados'].dropna()) 
        self.datosExtraidos = self.datosExtraidos.head(num_rows)     
        self.datosExtraidos = self.datosExtraidos.loc[['Comuna']]     
        self.datosExtraidos = self.datosExtraidos[['Región , Provincia, Comuna', 'Motorizados']]

    def transformarDatos(self,comuna, conflict_comunas):
        # Indicator name: Mode of Transportation (household)
        # Fuente: INE. Base de datos permisos de circulación

        self.datos = self.datosExtraidos[self.datosExtraidos['Región , Provincia, Comuna'].str.contains(comuna["Nombre"])]
        #print(self.datos)
        if len(self.datos) == 0:
            try:
                self.datos = self.datosExtraidos[
                            self.datosExtraidos["Región , Provincia, Comuna"] 
                            == conflict_comunas[comuna["Nombre"]]]
            except KeyError:
                self.valor = 0
                return

        self.motorizados = self.datos["Motorizados"].iloc[0]
        self.valor = (self.motorizados / comuna["Población"])*100


    def cargarIndicadores(self,comuna):
        indicador = Indicador("D-FIS-HI4_3775-T-8", Comuna = comuna["Nombre"].upper())
        indicador.guardarIndicadorEnBaseDeDatos(self.valor)

    def cargarDatosDuros(self, comuna):
        datosDuros = DatosDurosDeFuente(
            nombreFuente="INE. Base de datos permisos de circulación",
            nombreLocalidad=comuna["Nombre"].upper(),
            datos=self.datos.to_dict("records"),
        )
        datosDuros.guardarDatosDuros()

    def EtlDeDatos(self,file):
        #try:
            self.extraerDatos(file) # Extraer
            _elemDict = {
                'Paihuano': 'Paiguano',
                'La Calera': 'Calera',
                'Llay-Llay': 'Llaillay',
                'Til Til': 'Tiltil',
                'Los Álamos': 'Los Alamos',
                'Padre Las Casas': 'Padre las Casas',
                "O'Higgins": "O´Higgins"
            }

            for comuna in Localidades.obtenerComunas():
                self.transformarDatos(comuna,_elemDict) # Transformar
                self.cargarIndicadores(comuna) # Cargar
                self.cargarDatosDuros(comuna)  # Cargar
        #except:
        #    return "Error"

            return 200



####
#   Biblioteca del congreso nacional de Chile
####


class BCNChEtnia(FuenteDeDatos):
    def __init__(self):
        self.nombre = "Biblioteca del congreso nacional de Chile: Etnia"
        self.datosExtraidos = None
        self.valor = 0

    def __str__(self):
        return str(self.nombre)

    def establecerLocalidades(self, localidades):
        self.localidades = localidades

    def extraerDatos(self, file):
        self.datosExtraidos = pd.read_excel(file)

    def transformarDatos(self, comuna):
        self.datos = self.datosExtraidos[
            self.datosExtraidos["Unidad territorial"].str.contains(
                comuna["Nombre"].title()
            )
        ]
        if len(self.datos) == 0:
            self.datos = self.datosExtraidos[
                self.datosExtraidos["Unidad territorial"]
                == conflict_comunas[comuna["Nombre"]]
            ]
        self.valor = self.datos[
            self.datos[" Variable"] == " Porcentaje de población que no declara etnia"
        ][" 2017"]
        self.valor = float(self.valor.iloc[0])

    def cargarIndicadores(self, comuna):
        indicador = Indicador("D-SCM-SO1_6726-T-70", Comuna=comuna["Nombre"].upper())
        indicador.guardarIndicadorEnBaseDeDatos(self.valor)

    def cargarDatosDuros(self, comuna):
        datosDuros = DatosDurosDeFuente(
            nombreFuente="Biblioteca del congreso nacional de Chile: Etnia",
            nombreLocalidad=comuna["Nombre"].upper(),
            datos=self.datos.to_dict("records"),
        )
        datosDuros.guardarDatosDuros()
        # data = {"Usuario":"nombreUsuario" ,"fechaSubida": datetime.now() ,"comuna": comuna["Nombre"].upper() ,
        #    "fuente":"Biblioteca del congreso nacional de Chile: Etnia", "usuario" : "admin", "datos": self.datos.to_dict('records')}
        # db.datosFuentes.insert_one(data)

    def EtlDeDatos(self, file):
        # try:
        self.extraerDatos(file)  # Extraer
        for comuna in Localidades.obtenerComunas():
            self.transformarDatos(comuna)  # Transformar
            self.cargarIndicadores(comuna)  # Cargar
            self.cargarDatosDuros(comuna)  # Cargar
        # except:
        #    return "Error"

        return 200


class BCNChDelitos:
    def __init__(self):
        self.nombre = "Biblioteca del congreso nacional de Chile: Delitos"
        self.datosExtraidos = None
        self.valor = 0

    def __str__(self):
        return str(self.nombre)

    def extraerDatos(self, file):
        self.datosExtraidos = pd.read_excel(file)

    def transformarDatos(self, comuna):
        self.datos = self.datosExtraidos[
            self.datosExtraidos["Unidad territorial"].str.contains(
                comuna["Nombre"].title()
            )
        ]
        if len(self.datos) == 0:
            self.datos = self.datosExtraidos[
                self.datosExtraidos["Unidad territorial"]
                == conflict_comunas[comuna["Nombre"]]
            ]
        self.valor = self.datos[
            self.datos[" Variable"] == " Tasa de Denuncias de DMCS"
        ][" 2022"]
        self.valor = float(self.valor.iloc[0])

    def cargarIndicadores(self, comuna, user="admin"):
        indicador_id = "D-SCM-CU9_2459-T-83"  # Identificador del indicador "Occurrence of Conflicts/Riots/Homicide incidents"
        indicador = Indicador(indicador_id, Comuna=comuna["Nombre"].upper())
        indicador.guardarIndicadorEnBaseDeDatos(self.valor)

    def cargarDatosDuros(self, comuna, user="admin"):
        datosDuros = DatosDurosDeFuente(
            nombreFuente="Biblioteca del congreso nacional de Chile: Delitos",
            nombreLocalidad=comuna["Nombre"].upper(),
            datos=self.datos.to_dict("records"),
        )
        datosDuros.guardarDatosDuros()

    def EtlDeDatos(self, file):
        self.extraerDatos(file)  # Extraer
        for comuna in Localidades.obtenerComunas():
            self.transformarDatos(comuna)  # Transformar
            self.cargarIndicadores(comuna)  # Cargar indicadores
            self.cargarDatosDuros(comuna)  # Cargar datos duros
        return 200


class BCNChRubro:
    def __init__(self):
        self.nombre = "Biblioteca del congreso nacional de Chile: Rubro"
        self.datosExtraidos = None
        self.valor = 0

    def __str__(self):
        return str(self.nombre)

    def extraerDatos(self, file):
        self.datosExtraidos = pd.read_excel(file)

    def transformarDatos(self, comuna):
        self.datos = self.datosExtraidos[
            self.datosExtraidos["Unidad territorial"].str.contains(
                comuna["Nombre"].title()
            )
        ]
        if len(self.datos) == 0:
            self.datos = self.datosExtraidos[
                self.datosExtraidos["Unidad territorial"]
                == conflict_comunas[comuna["Nombre"]]
            ]

        datos = self.datos[self.datos[" Variable"].str.contains("Trabajadores")]

        mayor = ""
        valor = 0

        for i in range(len(datos)):
            try:
                if float(datos.iloc[i][" 2021"]) > valor:
                    mayor = datos.iloc[i][" Variable"]
                    valor = int(datos.iloc[i]["2021"])
            except:
                try:
                    if float(datos.iloc[i]["Unnamed: 3"]) > valor:
                        mayor = datos.iloc[i][" Variable"]
                        valor = int(datos.iloc[i]["Unnamed: 3"])
                except:
                    if float(datos.iloc[i]["Unnamed: 4"]) > valor:
                        mayor = datos.iloc[i][" Variable"]
                        valor = int(datos.iloc[i]["Unnamed: 4"])

        valor = (valor / comuna["Población"]) * 100
        self.valor = valor

    def cargarIndicadores(self, comuna, user="admin"):
        indicador_id = "D-ENM-EO1_1728-T-145"  # Identificador del indicador "Indicador Matriz productiva más amplia"
        indicador = Indicador(indicador_id, Comuna=comuna["Nombre"].upper())
        indicador.guardarIndicadorEnBaseDeDatos(self.valor)

    def cargarDatosDuros(self, comuna, user="admin"):
        datosDuros = DatosDurosDeFuente(
            nombreFuente="Biblioteca del congreso nacional de Chile: Rubro",
            nombreLocalidad=comuna["Nombre"].upper(),
            datos=self.datos.to_dict("records"),
        )
        datosDuros.guardarDatosDuros()

    def EtlDeDatos(self, file):
        self.extraerDatos(file)  # Extraer
        for comuna in Localidades.obtenerComunas():
            self.transformarDatos(comuna)  # Transformar
            self.cargarIndicadores(comuna)  # Cargar indicadores
            self.cargarDatosDuros(comuna)  # Cargar datos duros
        return 200


class BCNChDiscapacidad:
    def __init__(self):
        self.nombre = "Biblioteca del congreso nacional de Chile: Discapacidad"
        self.datosExtraidos = None
        self.valor = 0

    def __str__(self):
        return str(self.nombre)

    def extraerDatos(self, file):
        self.datosExtraidos = pd.read_excel(file)

    def transformarDatos(self, comuna):
        self.datos = self.datosExtraidos[
            self.datosExtraidos["Unidad territorial"].str.contains(
                comuna["Nombre"].title()
            )
        ]
        if len(self.datos) == 0:
            self.datos = self.datosExtraidos[
                self.datosExtraidos["Unidad territorial"]
                == conflict_comunas[comuna["Nombre"]]
            ]

        try:
            valor = 100 - float(
                self.datos[
                    self.datos[" Variable"]
                    == " Porcentaje de población sin discapacidad"
                ][" 2002"].iloc[0]
            )
        except ValueError:
            valor = 0

        self.valor = valor

    def cargarIndicadores(self, comuna, user="admin"):
        indicador_id = "D-SCM-SO1_6726-T-62"  # Identificador del indicador "special need population"
        indicador = Indicador(indicador_id, Comuna=comuna["Nombre"].upper())
        indicador.guardarIndicadorEnBaseDeDatos(self.valor)

    def cargarDatosDuros(self, comuna, user="admin"):
        datosDuros = DatosDurosDeFuente(
            nombreFuente="Biblioteca del congreso nacional de Chile: Discapacidad",
            nombreLocalidad=comuna["Nombre"].upper(),
            datos=self.datos.to_dict("records"),
        )
        datosDuros.guardarDatosDuros()

    def EtlDeDatos(self, file):
        self.extraerDatos(file)  # Extraer
        for comuna in Localidades.obtenerComunas():
            self.transformarDatos(comuna)  # Transformar
            self.cargarIndicadores(comuna)  # Cargar indicadores
            self.cargarDatosDuros(comuna)  # Cargar datos duros
        return 200


class BCNChAgua:
    def __init__(self):
        self.nombre = "Biblioteca del congreso nacional de Chile: Agua"
        self.datosExtraidos = None
        self.valor = 0

    def __str__(self):
        return str(self.nombre)

    def extraerDatos(self, file):
        self.datosExtraidos = pd.read_excel(file)

    def transformarDatos(self, region, conflicts_regions):
        self.datos = self.datosExtraidos[
            self.datosExtraidos["Unidad territorial"].str.contains(
                region["Región"].title()
            )
        ]
        if len(self.datos) == 0:
            self.datos = self.datosExtraidos[
                self.datosExtraidos["Unidad territorial"].str.contains(
                    conflicts_regions[region["Región"]]
                )
            ]

        valor = float(
            self.datos[
                self.datos[" Variable"]
                == " Porcentaje de población urbana con cobertura de agua potable"
            ][" 2021"].iloc[0]
        )
        self.valor = valor

    def cargarIndicadores(self, region, user="admin"):
        indicador_id = "D-FIS-AL2_4124-T-38"  # Identificador del indicador "Indicador Access to clean water"
        indicador = Indicador(indicador_id, Region=region["Región"].upper())
        indicador.guardarIndicadorEnBaseDeDatos(self.valor)

    def cargarDatosDuros(self, region, user="admin"):
        datosDuros = DatosDurosDeFuente(
            nombreFuente="Biblioteca del congreso nacional de Chile: Agua",
            nombreLocalidad=region["Región"].upper(),
            datos=self.datos.to_dict("records"),
        )
        datosDuros.guardarDatosDuros()

    def EtlDeDatos(self, file):
        self.extraerDatos(file)  # Extraer
        for region in Localidades.obtenerRegiones():
            self.transformarDatos(region, conflicts_regions)  # Transformar
            self.cargarIndicadores(region)  # Cargar indicadores
            self.cargarDatosDuros(region)  # Cargar datos duros

        return 200


class BCNChPobreza:
    def __init__(self):
        self.nombre = "Biblioteca del congreso nacional de Chile: Pobreza"
        self.datosExtraidos = None
        self.valor = 0
        self.datos = None  # Variable para almacenar los datos específicos de la comuna

    def __str__(self):
        return str(self.nombre)

    def extraerDatos(self, file):
        self.datosExtraidos = pd.read_excel(file)

    def transformarDatos(self, comuna):
        self.datos = self.datosExtraidos[
            self.datosExtraidos["Unidad territorial"].str.contains(
                comuna["Nombre"].title()
            )
        ]
        if len(self.datos) == 0:
            try:
                self.datos = self.datosExtraidos[
                    self.datosExtraidos["Unidad territorial"]
                    == conflict_comunas[comuna["Nombre"]]
                ]
            except KeyError:
                self.valor = 0
                return

        valor = float(
            self.datos[
                self.datos[" Variable"]
                == " Porcentaje de Población en Pobreza por ingresos"
            ][" 2020"].iloc[0]
        )
        self.valor = valor

    def cargarIndicadores(self, comuna, user="admin"):
        indicador_id = (
            "D-SCM-SO1_6726-T-76"  # Identificador del indicador "Indicador Poverty"
        )
        indicador = Indicador(indicador_id, Comuna=comuna["Nombre"].upper())
        indicador.guardarIndicadorEnBaseDeDatos(self.valor)

    def cargarDatosDuros(self, comuna, user="admin"):
        datosDuros = DatosDurosDeFuente(
            nombreFuente="Biblioteca del congreso nacional de Chile: Pobreza",
            nombreLocalidad=comuna["Nombre"].upper(),
            datos=self.datos.to_dict("records"),
        )
        datosDuros.guardarDatosDuros()

    def EtlDeDatos(self, file):
        self.extraerDatos(file)  # Extraer
        for comuna in Localidades.obtenerComunas():
            self.transformarDatos(comuna)  # Transformar
            self.cargarIndicadores(comuna)  # Cargar indicadores
            self.cargarDatosDuros(comuna)  # Cargar datos duros
        return 200


class BCNChSaludHumano:
    def __init__(self):
        self.nombre = "Biblioteca del congreso nacional de Chile: Salud humano"
        self.datosExtraidos = None
        self.valor = 0
        self.datos = None  # Variable para almacenar los datos específicos de la comuna

    def __str__(self):
        return str(self.nombre)

    def extraerDatos(self, file):
        self.datosExtraidos = pd.read_excel(file)

    def transformarDatos(self, comuna):
        self.datos = self.datosExtraidos[
            self.datosExtraidos["Unidad territorial"].str.contains(
                comuna["Nombre"].title()
            )
        ]
        if len(self.datos) == 0:
            try:
                self.datos = self.datosExtraidos[
                    self.datosExtraidos["Unidad territorial"]
                    == conflict_comunas[comuna["Nombre"]]
                ]
                try:
                    self.valor = float(
                        self.datos[
                            self.datos[" Variable"]
                            == " Número Total de Enfermeras Contratadas al 31 de Diciembre"
                        ][" 2021"].iloc[0]
                    )
                except ValueError:
                    self.valor = 0
                self.valor = (self.valor / comuna["Población"]) * 1000
            except KeyError:
                self.valor = 0
        else:
            try:
                self.valor = float(
                    self.datos[
                        self.datos[" Variable"]
                        == " Número Total de Enfermeras Contratadas al 31 de Diciembre"
                    ][" 2021"].iloc[0]
                )
            except ValueError:
                self.valor = 0
            self.valor = (self.valor / comuna["Población"]) * 1000

    def cargarIndicadores(self, comuna, user="admin"):
        indicador_id = "D-SCM-SO1_6726-T-66"  # Identificador del indicador "Critical human capital"
        indicador = Indicador(indicador_id, Comuna=comuna["Nombre"].upper())
        indicador.guardarIndicadorEnBaseDeDatos(self.valor)

    def cargarDatosDuros(self, comuna, user="admin"):
        datosDuros = DatosDurosDeFuente(
            nombreFuente="Biblioteca del congreso nacional de Chile: Solud humano",
            nombreLocalidad=comuna["Nombre"].upper(),
            datos=self.datos.to_dict("records"),
        )
        datosDuros.guardarDatosDuros()

    def EtlDeDatos(self, file):
        self.extraerDatos(file)  # Extraer
        for comuna in Localidades.obtenerComunas():
            self.transformarDatos(comuna)  # Transformar
            self.cargarIndicadores(comuna)  # Cargar indicadores
            self.cargarDatosDuros(comuna)  # Cargar datos duros
        return 200


class BCNChOrganizacionesComunitarias:
    def __init__(self):
        self.nombre = (
            "Biblioteca del congreso nacional de Chile: Organizaciones Comunitarias"
        )
        self.datosExtraidos = None
        self.valor = 0
        self.datos = None  # Variable para almacenar los datos específicos de la comuna

    def __str__(self):
        return str(self.nombre)

    def extraerDatos(self, file):
        self.datosExtraidos = pd.read_excel(file)

    def transformarDatos(self, comuna):
        self.datos = self.datosExtraidos[
            self.datosExtraidos["Unidad territorial"].str.contains(
                comuna["Nombre"].title()
            )
        ]
        datos = self.datos[
            self.datos["Unidad territorial"].str.contains(comuna["Nombre"])
        ]
        self.valor = datos[" 2021"]
        try:
            self.valor = float(self.valor.iloc[0]) / comuna["Población"]
        except:
            self.valor = 0

    def cargarIndicadores(self, comuna, user="admin"):
        indicador_id = "D-SCM-BG9_3997-T-91"  # Identificador del indicador "AVAILABILITY OF SOCIAL CAPITAL"
        indicador = Indicador(indicador_id, Comuna=comuna["Nombre"].upper())
        indicador.guardarIndicadorEnBaseDeDatos(self.valor)

    def cargarDatosDuros(self, comuna, user="admin"):
        datosDuros = DatosDurosDeFuente(
            nombreFuente="Biblioteca del congreso nacional de Chile: Organizaciones Comunitarias",
            nombreLocalidad=comuna["Nombre"].upper(),
            datos=self.datos.to_dict("records"),
        )
        datosDuros.guardarDatosDuros()

    def EtlDeDatos(self, file):
        self.extraerDatos(file)  # Extraer
        for comuna in Localidades.obtenerComunas():
            self.transformarDatos(comuna)  # Transformar
            self.cargarIndicadores(comuna)  # Cargar indicadores
            self.cargarDatosDuros(comuna)  # Cargar datos duros
        return 200


class BCNChEmpresas:
    def __init__(self):
        self.nombre = "Biblioteca del congreso nacional de Chile: Empresas"
        self.datosExtraidos = None
        self.valor = 0
        self.datos = None  # Variable para almacenar los datos específicos de la comuna

    def __str__(self):
        return str(self.nombre)

    def extraerDatos(self, file):
        self.datosExtraidos = pd.read_excel(file)

    def transformarDatos(self, comuna):
        self.datos = self.datosExtraidos[
            self.datosExtraidos["Unidad territorial"].str.contains(
                comuna["Nombre"].title()
            )
        ]
        if len(self.datos) == 0:
            try:
                self.datos = self.datosExtraidos[
                    self.datosExtraidos["Unidad territorial"]
                    == conflict_comunas[comuna["Nombre"]]
                ]
            except KeyError:
                self.valor = 0
                return

        micro = (
            self.datos[self.datos[" Variable"].str.contains("Micro")][" 2021"]
            .sum()
            .split(" ")
        )
        sum_micro = sum(float(i) for i in micro if i != "" and i != "-")

        pequena = (
            self.datos[self.datos[" Variable"].str.contains("Pequeña")][" 2021"]
            .sum()
            .split(" ")
        )
        sum_pequena = sum(float(i) for i in pequena if i != "" and i != "-")

        mediana = (
            self.datos[self.datos[" Variable"].str.contains("Mediana")][" 2021"]
            .sum()
            .split(" ")
        )
        sum_mediana = sum(float(i) for i in mediana if i != "" and i != "-")

        grande = (
            self.datos[self.datos[" Variable"].str.contains("Grande")][" 2021"]
            .sum()
            .split(" ")
        )
        sum_grande = sum(float(i) for i in grande if i != "" and i != "-")

        valor = (sum_micro + sum_pequena + sum_mediana) / (
            sum_grande + sum_micro + sum_pequena + sum_mediana
        )
        self.valor = valor

        # Indicador Commercial establishments and businesses
        # Filtrar el número de trabajadores en empresas sin ventas
        cant_trabajadores_sin_ventas = pd.to_numeric(
            self.datos[
                self.datos[" Variable"]
                == " Cantidad de trabajadores en empresas sin ventas"
            ][" 2021"]
        )
        cant_trabajadores_sin_ventas
        # Filtrar el total de trabajadores registrados en la tabla
        lista_trabajadores = self.datos[" 2021"]
        total_trabajadores = 0

        for i in lista_trabajadores:
            try:
                total_trabajadores += float(i)
            except:
                pass
        # Calcular el porcentaje de trabajadores en empresas sin ventas del total de trabajadores
        self.valorEstadoDeServicios = (
            abs(
                (
                    float(cant_trabajadores_sin_ventas.iloc[0])
                    / float(total_trabajadores)
                )
                - 1
            )
            * 100
        )

    def cargarIndicadores(self, comuna, user="admin"):
        indicador_id = (
            "D-ENM-BN6_3420-T-172"  # Identificador del indicador Business size"
        )
        indicador = Indicador(indicador_id, comuna["Nombre"].upper())
        indicador.guardarIndicadorEnBaseDeDatos(self.valor)

        indicador_id_estado_servicios = (
            "D-FIS-CI2_3630-T-29"  # Indicador Commercial establishments and businesses
        )
        indicador = Indicador(
            indicador_id_estado_servicios, Comuna=comuna["Nombre"].upper()
        )
        indicador.guardarIndicadorEnBaseDeDatos(self.valorEstadoDeServicios)

    def cargarDatosDuros(self, comuna, user="admin"):
        datosDuros = DatosDurosDeFuente(
            nombreFuente="Biblioteca del congreso nacional de Chile: Empresas",
            nombreLocalidad=comuna["Nombre"].upper(),
            datos=self.datos.to_dict("records"),
        )
        datosDuros.guardarDatosDuros()

    def EtlDeDatos(self, file):
        self.extraerDatos(file)  # Extraer
        for comuna in Localidades.obtenerComunas():
            self.transformarDatos(comuna)  # Transformar
            self.cargarIndicadores(comuna)  # Cargar indicadores
            self.cargarDatosDuros(comuna)  # Cargar datos duros
        return 200


####
#   CENSO 2017 INE
####


class INEPoblacionUnoPuntoUno:
    def __init__(self):
        self.nombre = "Censo 2017 INE 1.1 Población"
        self.datosExtraidos = None
        self.valor_population_type = 0
        self.valor_urban_density = 0
        self.valor_gender = 0

    def __str__(self):
        return str(self.nombre)

    def extraerDatos(self, file, sheet_name, header):
        self.datosExtraidos = pd.read_excel(file, sheet_name=sheet_name, header=header)

    def transformarDatos(self, comuna):
        x = self.datosExtraidos[self.datosExtraidos["EDAD"] == "Total Comuna"]
        poblacion = x[self.datosExtraidos["CÓDIGO COMUNA"] == str(comuna["CUT "])][
            "TOTAL POBLACIÓN EFECTIVAMENTE CENSADA"
        ].iloc[0]
        rural = x[self.datosExtraidos["CÓDIGO COMUNA"] == str(comuna["CUT "])][
            "TOTAL ÁREA RURAL"
        ].iloc[0]
        self.valor_population_type = (rural / poblacion) * 100

        poblacion = x[self.datosExtraidos["CÓDIGO COMUNA"] == str(comuna["CUT "])][
            "TOTAL POBLACIÓN EFECTIVAMENTE CENSADA"
        ].iloc[0]
        superficie = float(comuna["Superficie"])
        self.valor_urban_density = poblacion / superficie

        poblacion = x[self.datosExtraidos["CÓDIGO COMUNA"] == str(comuna["CUT "])][
            "TOTAL POBLACIÓN EFECTIVAMENTE CENSADA"
        ].iloc[0]
        mujeres = x[self.datosExtraidos["CÓDIGO COMUNA"] == str(comuna["CUT "])][
            "MUJERES"
        ].iloc[0]
        self.valor_gender = (mujeres / poblacion) * 100

        self.datosComuna = x

    def cargarDatosDuros(self, comuna, user="admin"):
        datosDuros = DatosDurosDeFuente(
            nombreFuente="Censo 2017 INE 1.1 Población",
            nombreLocalidad=comuna["Nombre"].upper(),
            datos=self.datosComuna.to_dict("records"),
        )
        datosDuros.guardarDatosDuros()

    def cargarIndicadores(self, comuna, user="admin"):
        indicador_id = (
            "D-SCM-SO1_6726-T-70"  # Identificador del indicador "Population type"
        )
        indicador = Indicador(indicador_id, comuna["Nombre"].upper())
        indicador.guardarIndicadorEnBaseDeDatos(self.valor_population_type)

        indicador_id = "D-FIS-UN4_2748-T-11"  # Identificador del indicador "Indicador Urban density"
        indicador = Indicador(indicador_id, comuna["Nombre"].upper())
        indicador.guardarIndicadorEnBaseDeDatos(self.valor_urban_density)

        indicador_id = "D-SCM-SO1_6726-T-75"  # Identificador del indicador "Gender"
        indicador = Indicador(indicador_id, Comuna=comuna["Nombre"].upper())
        indicador.guardarIndicadorEnBaseDeDatos(self.valor_gender)

    def EtlDeDatos(self, file, sheet_name="Comuna", header=2, user="admin"):
        self.extraerDatos(file, sheet_name, header)  # Extraer
        for comuna in Localidades.obtenerComunas():
            self.transformarDatos(comuna)  # Transformar
            self.cargarIndicadores(comuna)  # Cargar indicadores
            self.cargarDatosDuros(comuna)  # Cargar datos duros
        return 200


class INEPoblacionUnoPuntoDos:
    def __init__(self):
        self.nombre = "Censo 2017 INE 1.2 Población"
        self.datosExtraidos = None
        self.valor_age = 0

    def __str__(self):
        return str(self.nombre)

    def extraerDatos(self, file, sheet_name, header):
        self.datosExtraidos = pd.read_excel(file, sheet_name=sheet_name, header=header)

    def transformarDatos(self, comuna):
        x = self.datosExtraidos[
            self.datosExtraidos["CÓDIGO COMUNA"] == str(comuna["CUT "])
        ]
        poblacion = int(
            x[x["GRUPOS DE EDAD"] == "Total Comuna"][
                "TOTAL POBLACIÓN EFECTIVAMENTE CENSADA"
            ].iloc[0]
        )
        old = (
            int(
                x[x["GRUPOS DE EDAD"] == "65 a 69"][
                    "TOTAL POBLACIÓN EFECTIVAMENTE CENSADA"
                ].iloc[0]
            )
            + int(
                x[x["GRUPOS DE EDAD"] == "70 a 74"][
                    "TOTAL POBLACIÓN EFECTIVAMENTE CENSADA"
                ].iloc[0]
            )
            + int(
                x[x["GRUPOS DE EDAD"] == "75 a 79"][
                    "TOTAL POBLACIÓN EFECTIVAMENTE CENSADA"
                ].iloc[0]
            )
            + int(
                x[x["GRUPOS DE EDAD"] == "80 a 84"][
                    "TOTAL POBLACIÓN EFECTIVAMENTE CENSADA"
                ].iloc[0]
            )
            + int(
                x[x["GRUPOS DE EDAD"] == "85 a 89"][
                    "TOTAL POBLACIÓN EFECTIVAMENTE CENSADA"
                ].iloc[0]
            )
            + int(
                x[x["GRUPOS DE EDAD"] == "90 a 94"][
                    "TOTAL POBLACIÓN EFECTIVAMENTE CENSADA"
                ].iloc[0]
            )
            + int(
                x[x["GRUPOS DE EDAD"] == "95 a 99"][
                    "TOTAL POBLACIÓN EFECTIVAMENTE CENSADA"
                ].iloc[0]
            )
            + int(
                x[x["GRUPOS DE EDAD"] == "100 o más"][
                    "TOTAL POBLACIÓN EFECTIVAMENTE CENSADA"
                ].iloc[0]
            )
        )
        kids = int(
            x[x["GRUPOS DE EDAD"] == "0 a 4"][
                "TOTAL POBLACIÓN EFECTIVAMENTE CENSADA"
            ].iloc[0]
        )
        self.valor_age = ((old + kids) / poblacion) * 100

    def cargarIndicadores(self, comuna, user="admin"):
        indicador_id = "D-SCM-SO1_6726-T-73"  # Identificador de Indicador Age
        indicador = Indicador(indicador_id, Comuna=comuna["Nombre"].upper())
        indicador.guardarIndicadorEnBaseDeDatos(self.valor_age)

    def cargarDatosDuros(self, comuna, user="admin"):
        datosDuros = DatosDurosDeFuente(
            nombreFuente="Censo 2017 INE 1.2 Población",
            nombreLocalidad=comuna["Nombre"].upper(),
            datos=self.datosExtraidos.to_dict("records"),
        )
        datosDuros.guardarDatosDuros()

    def EtlDeDatos(self, file, sheet_name="Comuna", header=2, user="admin"):
        self.extraerDatos(file, sheet_name, header)  # Extraer
        for comuna in Localidades.obtenerComunas():
            self.transformarDatos(comuna)  # Transformar
            self.cargarIndicadores(comuna)  # Cargar indicadores
            self.cargarDatosDuros(comuna)  # Cargar datos duros
        return 200


class INEEducacionCuatroPuntoUno:
    def __init__(self):
        self.nombre = "Censo 2017 INE 4.1 Education"
        self.datosExtraidos = None
        self.valor = 0

    def __str__(self):
        return str(self.nombre)

    def extraerDatos(self, file):
        self.datosExtraidos = pd.read_excel(file, sheet_name="Comuna", header=2)

    def transformarDatos(self, comuna):
        gdf = self.datosExtraidos[
            self.datosExtraidos["CÓDIGO COMUNA"] == str(comuna["CUT "])
        ]
        media = (
            gdf[gdf["CURSO MÁS ALTO APROBADO"] == "Total Científico - Humanista"][
                "TOTAL POBLACIÓN EFECTIVAMENTE CENSADA"
            ].iloc[0]
            + gdf[gdf["CURSO MÁS ALTO APROBADO"] == "Total Técnica Profesional"][
                "TOTAL POBLACIÓN EFECTIVAMENTE CENSADA"
            ].iloc[0]
            + gdf[gdf["CURSO MÁS ALTO APROBADO"] == "Total Humanidades"][
                "TOTAL POBLACIÓN EFECTIVAMENTE CENSADA"
            ].iloc[0]
            + gdf[
                gdf["CURSO MÁS ALTO APROBADO"]
                == "Total Técnica Comercial, Industrial/Normalista"
            ]["TOTAL POBLACIÓN EFECTIVAMENTE CENSADA"].iloc[0]
            + gdf[gdf["CURSO MÁS ALTO APROBADO"] == "Total Técnico Superior"][
                "TOTAL POBLACIÓN EFECTIVAMENTE CENSADA"
            ].iloc[0]
            + gdf[gdf["CURSO MÁS ALTO APROBADO"] == "Total Profesional"][
                "TOTAL POBLACIÓN EFECTIVAMENTE CENSADA"
            ].iloc[0]
            + gdf[gdf["CURSO MÁS ALTO APROBADO"] == "Total Magíster"][
                "TOTAL POBLACIÓN EFECTIVAMENTE CENSADA"
            ].iloc[0]
            + gdf[gdf["CURSO MÁS ALTO APROBADO"] == "Total Doctorado"][
                "TOTAL POBLACIÓN EFECTIVAMENTE CENSADA"
            ].iloc[0]
        )
        poblacion = gdf[gdf["CURSO MÁS ALTO APROBADO"] == "Total Comuna"][
            "TOTAL POBLACIÓN EFECTIVAMENTE CENSADA"
        ].iloc[0]
        self.valor = media / poblacion * 100

    def cargarIndicadores(self, comuna, user="admin"):
        indicador_id = "D-SCM-SO1_6726-T-64"  # Identificador de Education
        indicador = Indicador(indicador_id, Comuna=comuna["Nombre"].upper())
        indicador.guardarIndicadorEnBaseDeDatos(self.valor)

    def cargarDatosDuros(self, comuna, user="admin"):
        gdf = self.datosExtraidos[
            self.datosExtraidos["CÓDIGO COMUNA"] == str(comuna["CUT "])
        ]
        datosDuros = DatosDurosDeFuente(
            nombreFuente="Censo 2017 INE 4.1 Education",
            nombreLocalidad=comuna["Nombre"].upper(),
            datos=gdf.to_dict("records"),
        )
        datosDuros.guardarDatosDuros()

    def EtlDeDatos(self, file):
        self.extraerDatos(file)  # Extraer
        for comuna in Localidades.obtenerComunas():
            self.transformarDatos(comuna)  # Transformar
            self.cargarIndicadores(comuna)  # Cargar indicadores
            self.cargarDatosDuros(comuna)  # Cargar datos duros
        return 200


class INEViviendaSietePuntoTres:
    def __init__(self):
        self.nombre = "Censo 2017 INE 7.3 Vivienda"
        self.datosExtraidos = None
        self.valor = 0

    def __str__(self):
        return str(self.nombre)

    def extraerDatos(self, file):
        self.datosExtraidos = pd.read_excel(file, sheet_name="Comuna", header=2)

    def transformarDatos(self, comuna):
        gdf = self.datosExtraidos[self.datosExtraidos["ÁREA"] == "Total Comuna"]
        totalViviendas = gdf[gdf["CÓDIGO COMUNA"] == str(comuna["CUT "])][
            "TOTAL VIVIENDAS PARTICULARES CON MORADORES PRESENTES"
        ]
        camion = gdf[gdf["CÓDIGO COMUNA"] == str(comuna["CUT "])]["CAMIÓN ALJIBE"]
        ignorado = gdf[gdf["CÓDIGO COMUNA"] == str(comuna["CUT "])][
            "ORIGEN DE AGUA IGNORADO"
        ]
        self.valor = (totalViviendas - camion - ignorado) / totalViviendas

    def cargarIndicadores(self, comuna, user="admin"):
        indicador_id = "D-FIS-AL2_4124-T-38"  # Identificador del indicador "Indicador Access to clean water"
        indicador = Indicador(indicador_id, Comuna=comuna["Nombre"].upper())
        indicador.guardarIndicadorEnBaseDeDatos(float(self.valor.iloc[0]))

    def cargarDatosDuros(self, comuna, user="admin"):
        gdf = self.datosExtraidos[self.datosExtraidos["ÁREA"] == "Total Comuna"]
        datosDuros = DatosDurosDeFuente(
            nombreFuente="Censo 2017 INE 7.3 Vivienda",
            nombreLocalidad=comuna["Nombre"].upper(),
            datos=gdf.to_dict("records"),
        )
        datosDuros.guardarDatosDuros()

    def EtlDeDatos(self, file):
        self.extraerDatos(file)  # Extraer
        for comuna in Localidades.obtenerComunas():
            self.transformarDatos(comuna)  # Transformar
            self.cargarIndicadores(comuna)  # Cargar indicadores
            self.cargarDatosDuros(comuna)  # Cargar datos duros
        return 200


class INEHogarOchoPuntoUno:
    def __init__(self):
        self.nombre = "Censo 2017 INE 8.1 Hogar"
        self.datosExtraidos = None
        self.valor = 0

    def __str__(self):
        return str(self.nombre)

    def extraerDatos(self, file):
        self.datosExtraidos = pd.read_excel(file, sheet_name="Comuna", header=2)

    def transformarDatos(self, comuna):
        gdf = self.datosExtraidos
        hogares = gdf[gdf["CÓDIGO COMUNA"] == str(comuna["CUT "])]["TOTAL DE HOGARES"]
        unipersonal = gdf[gdf["CÓDIGO COMUNA"] == str(comuna["CUT "])][
            "HOGAR UNIPERSONAL"
        ]
        self.valor = (unipersonal / hogares) * 100

    def cargarIndicadores(self, comuna, user="admin"):
        indicador_id = (
            "D-FIS-HI4_3775-T-7"  # Identificador del indicador "Indicador Housing type"
        )
        indicador = Indicador(indicador_id, Comuna=comuna["Nombre"].upper())
        indicador.guardarIndicadorEnBaseDeDatos(float(self.valor.iloc[0]))

    def cargarDatosDuros(self, comuna, user="admin"):
        gdf = self.datosExtraidos
        datosDuros = DatosDurosDeFuente(
            nombreFuente="Censo 2017 INE 8.1 Hogar",
            nombreLocalidad=comuna["Nombre"].upper(),
            datos=gdf[gdf["CÓDIGO COMUNA"] == str(comuna["CUT "])].to_dict("records"),
        )

        datosDuros.guardarDatosDuros()

    def EtlDeDatos(self, file):
        self.extraerDatos(file)  # Extraer
        for comuna in Localidades.obtenerComunas():
            self.transformarDatos(comuna)  # Transformar
            self.cargarIndicadores(comuna)  # Cargar indicadores
            self.cargarDatosDuros(comuna)  # Cargar datos duros
        return 200


class INEViviendaSietePuntoSeis:
    def __init__(self):
        self.nombre = "Censo 2017 INE 7.6 Vivienda"
        self.datosExtraidos = None
        self.valor = 0

    def __str__(self):
        return str(self.nombre)

    def extraerDatos(self, file):
        self.datosExtraidos = pd.read_excel(file, sheet_name="Comuna", header=2)

    def transformarDatos(self, comuna):
        gdf = self.datosExtraidos
        totalViviendas = gdf[gdf["CÓDIGO COMUNA"] == str(comuna["CUT "])][
            "TOTAL VIVIENDAS PARTICULARES OCUPADAS CON MORADORES PRESENTES"
        ]
        aceptableViviendas = gdf[gdf["CÓDIGO COMUNA"] == str(comuna["CUT "])][
            "VIVIENDAS CON ÍNDICE DE MATERIALIDAD ACEPTABLE"
        ]
        self.valor = aceptableViviendas / totalViviendas

    def cargarIndicadores(self, comuna, user="admin"):
        indicador_id = "D-FIS-HI4_3775-T-6"  # Identificador del indicador "Indicador Housing stock construction quality"
        indicador = Indicador(indicador_id, Comuna=comuna["Nombre"].upper())
        indicador.guardarIndicadorEnBaseDeDatos(float(self.valor.iloc[0]))

    def cargarDatosDuros(self, comuna, user="admin"):
        gdf = self.datosExtraidos
        datosDuros = DatosDurosDeFuente(
            nombreFuente="Censo 2017 INE 7.6 Vivienda",
            nombreLocalidad=comuna["Nombre"].upper(),
            datos=gdf[gdf["CÓDIGO COMUNA"] == str(comuna["CUT "])].to_dict("records"),
        )
        datosDuros.guardarDatosDuros()

    def EtlDeDatos(self, file):
        self.extraerDatos(file)  # Extraer
        for comuna in Localidades.obtenerComunas():
            self.transformarDatos(comuna)  # Transformar
            self.cargarIndicadores(comuna)  # Cargar indicadores
            self.cargarDatosDuros(comuna)  # Cargar datos duros
        return 200


class SIIEstadisticasDeEmpresa:
    def __init__(self):
        self.nombre = "SII: Estadísticas de Empresa por comuna"
        self.datosExtraidos = None
        self.valor_empresasPerCapita = 0

    def __str__(self):
        return str(self.nombre)

    def extraerDatos(self, file):
        self.datosExtraidos = pd.read_excel(file, header=4)

    def transformarDatos(self, comuna, conflict_comunas):
        # Number of business per càpita
        ultimos_datos = self.datosExtraidos[
            self.datosExtraidos["Año Comercial"] == 2021
        ]
        self.datos = ultimos_datos[
            ultimos_datos["Comuna del domicilio o casa matriz"].str.contains(
                comuna["Nombre"]
            )
        ]
        if len(self.datos) == 0:
            self.datos = ultimos_datos[
                ultimos_datos["Comuna del domicilio o casa matriz"].str.contains(
                    conflict_comunas[comuna["Nombre"]]
                )
            ]
            if len(self.datos) == 0:
                self.datos = ultimos_datos[
                    ultimos_datos["Comuna del domicilio o casa matriz"].str.contains(
                        conflict_comunas[comuna["Nombre"] + "2"]
                    )
                ]
                if len(self.datos) == 0:
                    raise NameError(comuna["Nombre"])

        numero_de_negocios_per_capita = self.datos["Número de empresas"].iloc[0]
        self.valor_empresasPerCapita = (
            numero_de_negocios_per_capita / comuna["Población"]
        )

    def cargarIndicadores(self, comuna, user="admin"):
        indicador_id = "D-ENM-IV9_2123-T-157"  # Number of business per càpita
        indicador = Indicador(indicador_id, Comuna=comuna["Nombre"].upper())
        indicador.guardarIndicadorEnBaseDeDatos(self.valor_empresasPerCapita)

    def cargarDatosDuros(self, comuna, user="admin"):
        datosDuros = DatosDurosDeFuente(
            nombreFuente="SII: Estadísticas de Empresa por comuna",
            nombreLocalidad=comuna["Nombre"].upper(),
            datos=self.datos.to_dict("records"),
        )
        datosDuros.guardarDatosDuros()

    def EtlDeDatos(self, file):
        self.extraerDatos(file)  # Extraer
        for comuna in Localidades.obtenerComunas():
            self.transformarDatos(comuna, conflict_comunas)  # Transformar
            self.cargarIndicadores(comuna)  # Cargar indicadores
            self.cargarDatosDuros(comuna)  # Cargar datos duros
        return 200


class BiodiversityMari:
    def __init__(self):
        self.nombre = "Biodiversidad por ciudades"
        self.datosExtraidos = None
        self.valor = 0
        self.datos = None  # Variable para almacenar los datos específicos de la comuna

    def __str__(self):
        return str(self.nombre)

    def extraerDatos(self, file):
        self.datosExtraidos = pd.read_excel(
            file, sheet_name="Biodiversidad por ciudades"
        )

    def transformarDatos(self, comuna):
        if comuna["Nombre"] in Localidades.obtenerComunasConflictoBiodiversity():
            comuna_buscar = Localidades.obtenerComunasConflictoBiodiversity()[
                comuna["Nombre"]
            ]
        else:
            comuna_buscar = comuna["Nombre"]
        city_values = self.datosExtraidos[self.datosExtraidos["city"] == comuna_buscar]

        if not city_values.empty:
            self.datos = city_values
            value = city_values["occurrence"]
            self.valor = float(value.values[0])
        else:
            self.valor = float(0)

    def cargarIndicadores(self, comuna, user="admin"):
        indicador_id = (
            "D-ELG-BI2_3323-T-46"  # Identificador del indicador "Indicador Poverty"
        )
        indicador = Indicador(indicador_id, Comuna=comuna["Nombre"].upper())
        indicador.guardarIndicadorEnBaseDeDatos(self.valor)

    def cargarDatosDuros(self, comuna, user="admin"):
        datosDuros = DatosDurosDeFuente(
            nombreFuente=self.nombre,
            nombreLocalidad=comuna["Nombre"].upper(),
            datos=self.datos.to_dict("records"),
        )
        datosDuros.guardarDatosDuros()

    def EtlDeDatos(self, file):
        self.extraerDatos(file)  # Extraer
        for comuna in Localidades.obtenerComunas():
            self.transformarDatos(comuna)  # Transformar
            self.cargarIndicadores(comuna)  # Cargar indicadores
            self.cargarDatosDuros(comuna)  # Cargar datos duros
        return 200


class ErosionRate:
    def __init__(self):
        self.nombre = "Erosion regiones"
        self.datosExtraidos = None
        self.valor = 0
        self.datos = None  # Variable para almacenar los datos específicos de la comuna

    def __str__(self):
        return str(self.nombre)

    def extraerDatos(self, file):
        self.datosExtraidos = pd.read_excel(file, sheet_name="COMUNAS", header=0)

    def transformarDatos(self, comuna):
        gdp = self.datosExtraidos
        x = gdp[gdp["CÓDIGO COMUNA"] == comuna["CUT "]]
        if not x.empty and x["TOTAL GENERAL"].iloc[0] != 0:
            self.datos = x
            self.valor = (
                x["SUELOS EROSIONADOS"].iloc[0] / x["TOTAL GENERAL"].iloc[0]
            ) * 100
        else:
            self.valor = 0

    def cargarIndicadores(self, comuna, user="admin"):
        indicador_id = (
            "D-ELG-CI8_9785-T-48"  # Identificador del indicador "Indicador Poverty"
        )
        indicador = Indicador(indicador_id, Comuna=comuna["Nombre"].upper())
        indicador.guardarIndicadorEnBaseDeDatos(self.valor)

    def cargarDatosDuros(self, comuna, user="admin"):
        datosDuros = DatosDurosDeFuente(
            nombreFuente=self.nombre,
            nombreLocalidad=comuna["Nombre"].upper(),
            datos=self.datos.to_dict("records"),
        )
        datosDuros.guardarDatosDuros()

    def EtlDeDatos(self, file):
        self.extraerDatos(file)  # Extraer
        for comuna in Localidades.obtenerComunas():
            self.transformarDatos(comuna)  # Transformar
            self.cargarIndicadores(comuna)  # Cargar indicadores
            self.cargarDatosDuros(comuna)  # Cargar datos duros
        return 200


class NumeroUPA:
    def __init__(self):
        self.nombre = "numero-de-upa-y-superficie-por-categoría-de-uso-del-suelo"
        self.datosExtraidos = None
        self.datosProcesados = []
        self.valor = 0
        self.datos = None  # Variable para almacenar los datos específicos de la comuna
        self.extract = False

    def __str__(self):
        return str(self.nombre)

    def extraerDatos(self, file):
        self.datosExtraidos = pd.read_excel(file, sheet_name="05", header=[4, 5])

    def datosExcel(self):
        return [
            " Comuna 6, 7, 8",
            "Cereales",
            "Leguminosas y tubérculos",
            "Cultivos industriales",
            "Hortalizas, hongos, aromáticas, medicinales y condimentarias",
            "Frutales",
            "Vides para vinificación y uvas pisqueras",
        ]

    def calculate(self, df, commune):
        filas = self.datosExcel().copy()
        accumulative = 0
        for fila in filas[1:]:
            accumulative += df[fila]["Número de UPA"]
        return {"commune": commune, "value": accumulative}

    def transformarDatos(self, comuna):
        if self.extract:
            self.datos = self.datosProcesados.get(comuna["Nombre"].lower(), 0)
            self.valor = self.datos
        else:
            result = []
            grupo1 = [
                "Santiago",
                "La florida",
                "La reina",
                "Las condes",
                "Lo barnechea",
                "Ñuñoa",
                "Peñalolén",
                "Vitacura",
                "Providencia",
            ]
            grupo2 = [
                "Cerrillos",
                "Cerro Navia",
                "Conchalí",
                "Estación Central",
                "Huechuraba",
                "Lo prado",
                "Pedro Aguirre Cerda",
                "Quilicura",
                "Renca",
            ]
            grupo3 = ["el bosque", "La Pintana", "san ramón"]
            problematics = {
                "Paihuano": "paiguano",
                "La Calera": "calera",
                "Llay-Llay": "llaillay",
                "Concón": " concón",
                "Til Til": "tiltil",
            }
            comunas_nombre = [
                problematics[i["Nombre"]].lower()
                if i["Nombre"] in problematics
                else i["Nombre"].lower()
                for i in Localidades.obtenerComunas()
            ]
            df_filtrado = self.datosExtraidos[self.datosExcel()]
            df_sin_nan = df_filtrado.dropna()

            for index, row in df_sin_nan.iterrows():
                commune_to_revised = row[" Comuna 6, 7, 8"]["Unnamed: 2_level_1"]
                comunas_split = commune_to_revised.split("/")

                for comunas_separated in comunas_split:
                    comuna_lower = comunas_separated.lower()
                    if comuna_lower == "g1 santiago":
                        for i in grupo1:
                            if i.lower() in comunas_nombre:
                                result.append(self.calculate(row, i))
                    elif comuna_lower == "g2 santiago":
                        for i in grupo2:
                            if i.lower() in comunas_nombre:
                                result.append(self.calculate(row, i))
                    elif comuna_lower == "g3 santiago":
                        for i in grupo3:
                            if i.lower() in comunas_nombre:
                                result.append(self.calculate(row, i))
                    elif comuna_lower in comunas_nombre:
                        result.append(self.calculate(row, comuna_lower))
            self.datosProcesados = {item["commune"]: item["value"] for item in result}
            self.datos = self.datosProcesados.get(comuna["Nombre"].lower(), 0)
            self.valor = self.datos
            self.extract = True

    def cargarIndicadores(self, comuna, user="admin"):
        indicador_id = (
            "D-FIS-AL8_2890-T-43"  # Identificador del indicador "Indicador Poverty"
        )
        indicador = Indicador(indicador_id, Comuna=comuna["Nombre"].upper())
        indicador.guardarIndicadorEnBaseDeDatos(self.valor)

    def cargarDatosDuros(self, comuna, user="admin"):
        datosDuros = DatosDurosDeFuente(
            nombreFuente=self.nombre,
            nombreLocalidad=comuna["Nombre"].upper(),
            datos=self.datos,
        )
        datosDuros.guardarDatosDuros()

    def EtlDeDatos(self, file):
        self.extraerDatos(file)  # Extraer
        for comuna in Localidades.obtenerComunas():
            self.transformarDatos(comuna)  # Transformar
            self.cargarIndicadores(comuna)  # Cargar indicadores
            self.cargarDatosDuros(comuna)  # Cargar datos duros
        return 200
