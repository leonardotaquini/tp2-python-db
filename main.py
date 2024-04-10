import os
import csv
import MySQLdb
from dotenv import load_dotenv

load_dotenv()


def conectar_mysql(host, user, password, database):
    try:
        connection= MySQLdb.connect(host, user, password, database)
        return connection
    except Exception as e:
        print(f"Error al conectar a la base de datos: {e}")
        return None


def verificar_y_eliminar_tabla(cursor_param, tabla_param):
    try:
        cursor_param.execute("SHOW TABLES LIKE %s", (tabla_param,))
        existe_tabla = cursor_param.fetchone()
        
        if existe_tabla:
            cursor_param.execute(f"DROP TABLE {tabla_param}")
            print(f"La tabla '{tabla_param}' ha sido eliminada correctamente")
    except Exception as e:
        print(f"Error al verificar y eliminar la tabla: {e}")


def crear_tabla_desde_csv(cursor_param, archivo_csv_param, tabla_param):
    with open(archivo_csv_param, 'r', encoding='utf-8') as file:
        reader = csv.reader(file)
        columnas = next(reader)
        crear_tabla_query = f"CREATE TABLE { tabla_param } ({', '.join([f'{ columna } VARCHAR(255)' for columna in columnas])})"
        cursor_param.execute(crear_tabla_query)        


def cargar_datos_desde_csv(cursor_param, archivo_csv_param, tabla_param):
    with open(archivo_csv_param, 'r', encoding='utf-8') as file:
        reader = csv.reader(file)
        next(reader)
        for fila in reader:
            marcadores = ','.join(['%s'] * len(fila))
            consulta = f'INSERT INTO {tabla} VALUES ({ marcadores })'
            cursor_param.execute(consulta, fila)



host = os.environ["DB_HOST"]
user = os.environ["DB_USERNAME"] 
password = os.environ["DB_PASSWORD"] 
database = os.environ["DB_DATABASE"] 
archivo_csv = os.path.join(os.path.dirname(__file__), 'localidades.csv')
tabla = "localidades"


conn = conectar_mysql(host, user, password, database)
cursor = conn.cursor()


verificar_y_eliminar_tabla(cursor, tabla)

crear_tabla_desde_csv(cursor, archivo_csv, tabla)

cargar_datos_desde_csv(cursor, archivo_csv, tabla)

conn.commit()

conn.close()


     


