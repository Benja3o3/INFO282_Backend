import os
import psycopg2
from sqlalchemy import create_engine

class database:
    def __init__(self, database):
        #load_dotenv()
        self.conn = None
        self.database = database
        self.host = os.environ.get('HOST')
        self.port = os.environ.get('PORT')
        self.user = os.environ.get('USER')
        self.password = os.environ.get('PASSWORD')
        self.conection()

    def conection(self):
        # # Parámetros de conexión
        parametros = {
            "host": self.host,
            "port": self.port,
            "user": self.user,
            "password": self.password,
            "database": self.database,
        }
        self.conn = psycopg2.connect(**parametros)

    def create_sqlalchemy_engine(self):
            db_uri = f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"
            engine = create_engine(db_uri)
            return engine