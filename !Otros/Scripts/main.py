import LoadFile as lf
import transformData as td

class Main:
    def __init__(self):
        pass

    def run(self):
        _loadfile = lf.LoadFile("IND_COLEGIO_01")
        print(_loadfile.getDirectionFile())



if __name__ == "__main__":
    programa = Main()
    programa.run()



