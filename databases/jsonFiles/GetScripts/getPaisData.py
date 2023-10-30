
import pandas as pd
import json


# Consigue cod_comuna / comuna y coordenadas
#utf-8 importante para tildes
with open('./files/country.json', 'r', encoding='utf-8') as json_file:
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
df['pais_id'] = 1
df['nombre'] = "chile"


print(df)
df.to_json('paisDB.json', orient='records', lines=True, default_handler=str, force_ascii=False)
