import geopandas as gpd
import psycopg2
def get_cursor():
    conn = psycopg2.connect(
        host = 'localhost',
        user = 'postgres',
        password = '123',
        database = 'BarometroBienestar'
    )
    cursor = conn.cursor()
    return cursor


'''
try:
    connection = psycopg2.connect(
        host = 'localhost',
        user = 'postgres',
        password = '123',
        database = 'BarometroBienestar'
    )
    print("[ x ] Conexion exitosa")
    cursor = connection.cursor()
except Exception as ex:
    print(ex)

'''

## Construct 