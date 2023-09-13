import pandas as pd
import geopandas as gpd
import zipfile
import tempfile
import shutil
temp_dir = tempfile.mkdtemp()


# CUT - Legal
_dbComuna = pd.DataFrame(columns= ['CUT', 'Población_comuna', 'Polygono_comuna','Nombre_comuna'])
_dbRegion = pd.DataFrame(columns= ['CUT', 'Polygono_region','Nombre_region'])



with zipfile.ZipFile('./archivos/comunas.zip', 'r') as zip_ref:
    zip_ref.extractall(temp_dir)

df = gpd.read_file(temp_dir + '/comunas.shp')
df = df[['Comuna', 'cod_comuna', 'geometry']]
_dbComuna['CUT'] = df['cod_comuna']
_dbComuna['Nombre_comuna'] = df['Comuna']
_dbComuna['Polygono_comuna'] = df['geometry']


conflict_names = {
            "Los Angeles" : "Los Ángeles",
            "Marchigüe" : "Marchihue",
            "Los Alamos" : "Los Álamos",
            "Paihuano": "Paiguano",
            "Ranquil": "Raiquil"
        }



df_poblacion = pd.read_excel('./archivos/Cantidad-de-Personas-por-Sexo-y-Edad.xlsx', sheet_name='COMUNAS', header =1)
df_poblacion = df_poblacion[['NOMBRE COMUNA','Código Comuna','TOTAL', 'Edad']]
df_poblacion = df_poblacion[df_poblacion['Edad'] == 'Total Comunal']
df_poblacion = df_poblacion.reset_index(drop=True)

for index, row in _dbComuna.iterrows():
    matching_row = df_poblacion[df_poblacion['NOMBRE COMUNA'].str.contains(row['Nombre_comuna'].upper())]
    
    if matching_row.empty:
        try:
            matching_row = df_poblacion[df_poblacion['NOMBRE COMUNA'].str.contains(conflict_names[row['Nombre_comuna']].upper())]
        except KeyError:
            print(row['Nombre_comuna'])
    
    if not matching_row.empty:
        total_value = matching_row['TOTAL'].values[0]
        _dbComuna.loc[index, 'Población_comuna'] = total_value


_dbComuna.to_csv('comunas_datos.csv', index=False)

shutil.rmtree(temp_dir)





'''
df = pd.read_excel('./cut_2018_v04.xls', header = 0)
df = df[['Código Región', 'Nombre Región','Código Comuna 2018', 'Nombre Comuna']]

_dbComuna['CUT'] = df['Codigo Comuna 2018']
_dbComuna['Nombre']

_dbRegion['Codigo Región'] = df['Codigo Región']
'''


'''
df = pd.read_excel('Cantidad-de-Personas-por-Sexo-y-Edad.xlsx', sheet_name='COMUNAS', header =1)

df = df[['NOMBRE REGIÓN','NOMBRE COMUNA','Código Comuna','Código Región','TOTAL', 'Edad']]
df = df[df['Edad'] == 'Total Comunal']
df = df.reset_index(drop=True)

# Valores regionales
# Agrupar por 'Código Región'.

df_regional = df.groupby('Código Región')['TOTAL'].sum().reset_index()
print(df_regional)
'''

# Export JSON