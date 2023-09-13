import pandas as pd
import LoadFile
import os

class Transform:
    # Constructor method (initialize object attributes)
    def __init__(self, file, source ):
        self.file = file
        self.source = source
    
    def ejecutar_script_por_id(id):
        # Verificar si el ID proporcionado existe como una carpeta
        script_path = f'./ExtractExcel/{id}.py'

        # Verificar si el script proporcionado existe
        if os.path.exists(script_path):
            # Ejecutar el script
            os.system(f'python {script_path}')
        