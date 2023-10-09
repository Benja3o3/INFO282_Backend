import os
import json

PATH = os.path.join("../../../databases/jsonFiles", "comunasDB.json")


with open(PATH, "r") as data:
    comunas = json.load(data)

print(comunas)
