# import mimetypes
import os
import FactoryExtractExcel


EXTENSION_EXCEL = {
    "xlsx",
    "xlsm",
    "xlsb",
    "xltx",
    "xltm",
    "xls",
    "xlt",
    "xml",
    "xlam",
    "xlw",
}
EXTENSION_FLAT = {"csv", "txt"}
EXTENSION_JSON = {"json"}
EXTENSION_SHP = {"zip"}

extractExcel = FactoryExtractExcel()
#proJson = FactoryProcessorJson()
#proFlat = FactoryProcessorFlat()
#proShp = FactoryProcessorShp()

class LoadFile:
    # Constructor method (initialize object attributes)
    def __init__(self, _id ):
        self._id = _id
        self.fileURL = './Files/' + self._id
        self.fileName = None
        self.getFileName()
        self.getFileExtension()


    def getFileName(self):
        files = os.listdir(self.fileURL)
        files = [file for file in files if os.path.isfile(os.path.join(self.fileURL, file))]
        files.sort(key=lambda x: os.path.getmtime(os.path.join(self.fileURL, x)), reverse=True)
        if files:
            self.fileName = files[0]

    def getFileExtension(self):
        typeFile = self.file.rsplit(".", 1)[1].lower()

        if typeFile in EXTENSION_EXCEL:
            return extractExcel.extractData(self.fileURL)
        else:
            return {"error": "Invalid type"}    

            
    '''
        if typeFile in EXTENSION_JSON:
            return proJson.saveData(self.fileURL)
        elif typeFile in EXTENSION_FLAT:
            return proFlat.saveData(self.fileURL)
        elif typeFile in EXTENSION_EXCEL:
            return proExcel.saveData(self.fileURL)
        elif typeFile in EXTENSION_SHP:
            return proShp.saveData(self.fileURL)
        else:
            return {"error": "Invalid type"}    
    '''



_loadfile = LoadFile("IND_COLEGIO_01")
