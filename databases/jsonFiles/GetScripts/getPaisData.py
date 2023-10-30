
import pandas as pd
import json
import os


# Consigue cod_comuna / comuna y coordenadas
#utf-8 importante para tildes
with open('./databases/jsonfiles/GetScripts/files/country.json', 'r', encoding='utf-8') as json_file:
    data = json.load(json_file)

data = data['features']

fixedJsonData = []
for feature in data:
    properties = feature.get('properties', {})  
    geometry = feature.get('geometry', {})      
    properties['coordinates'] = geometry


    fixedJsonData.append(properties)

df = pd.DataFrame(fixedJsonData)

df = df[['coordinates']]
df['pais_id'] = 1
df['nombre'] = "chile"

df.rename(columns={'coordinates': 'geometria'}, inplace=True)
df.to_json('paisDB.json', orient='records', lines=True, default_handler=str, force_ascii=False)
