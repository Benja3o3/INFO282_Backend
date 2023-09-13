# import mimetypes
import os
import FactoryExtractExcel
import transformData
'''
    Cargar el ultimo archivo dentro del directorio en la nube
    - De momento solo buscara el ultimo archivo en local.
'''





class LoadFile:  
    # Constructor method (initialize object attributes)
    def __init__(self, _id ):
        self._id = _id
        self.fileURL = './Files/' + self._id
        self.fileName = None
        self.getFileName()  # Consigue el ultimo archivo subido a esa carpeta en especifico

    def getFileName(self):
        files = os.listdir(self.fileURL)
        files = [file for file in files if os.path.isfile(os.path.join(self.fileURL, file))]
        files.sort(key=lambda x: os.path.getmtime(os.path.join(self.fileURL, x)), reverse=True)
        if files:
            self.fileName = files[0]
    
    def getDirectionFile(self):
        return self.fileURL + self.fileName
    
