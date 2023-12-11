
import pandas as pd
import json


# Consigue cod_comuna / comuna y coordenadas
#utf-8 importante para tildes
with open('./files/regiones.json', 'r', encoding='utf-8') as json_file:
    data = json.load(json_file)

data = data['features']

fixedJsonData = []
for feature in data:
    properties = feature.get('properties', {})  
    geometry = feature.get('geometry', {})      
    properties['coordinates'] = geometry


    fixedJsonData.append(properties)



#Consigue población  
# Fuente -> COVID 19 MINSAL
#https://github.com/MinCiencia/Datos-COVID19
df = pd.DataFrame(fixedJsonData)

df = df[['coordinates', 'Region', 'codregion']]

nombresModelo = {
    'codregion': 'CUT',
    'Region': 'nombre',
    'coordinates': 'geometria'
}

df.rename(columns=nombresModelo, inplace=True)

df.to_json('regionesDB.json', orient='records', lines=True, default_handler=str, force_ascii=False)
