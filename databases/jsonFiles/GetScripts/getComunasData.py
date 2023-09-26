import pandas as pd
import json


# Consigue cod_comuna / comuna y coordenadas
#utf-8 importante para tildes
with open('./files/communes.json', 'r', encoding='utf-8') as json_file:
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
df = df[['cod_comuna', 'Comuna', 'coordinates', 'codregion']]

poblacionDF= pd.read_csv('./files/Covid-19.csv')
poblacionDF= poblacionDF[['Codigo comuna', 'Poblacion']]
resultadoDF = poblacionDF.merge(df, left_on='Codigo comuna', right_on='cod_comuna', how='inner')
resultadoDF.drop("Codigo comuna", axis=1, inplace = True)


nombresModelo = {
    'cod_comuna': 'CUT',
    'Comuna': 'nombre',
    'Poblacion': 'poblacion',
    'coordinates': 'geometria',
    'codregion': 'region_id'
}

resultadoDF.rename(columns=nombresModelo, inplace=True)
resultadoDF.to_json('comunasDB.json', orient='records', lines =True)
