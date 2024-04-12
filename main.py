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


def listar_localidades(cursor_param):
    cursor_param.execute("SELECT * FROM localidades")
    resultado = cursor_param.fetchall()
    return resultado         


def agrupar_provincias_con_id(lista_localidades):
    provincias = {}
    for localidad in lista_localidades:
        provincia = localidad[0].lower()
        if provincia not in provincias:
            provincias[ localidad[0] ] = localidad[4]
    return provincias     

def remover_repetidos( lista ):
    finally_dict = {}
    for item, val in lista.items():
        finally_dict[ item ] = set( val )
    return finally_dict        


def agrupar_provincia_localidad( localidades_arg ):
    provincias = agrupar_provincias_con_id( localidades_arg )
    provincia_por_localidad = {}
    for localidad in localidades_arg:
        id_prov = localidad[4]
        nombre_prov = localidad[0]
        nombre_localidad = localidad[2]

        if id_prov == provincias.get( nombre_prov ):
            if nombre_prov not in provincia_por_localidad:
                provincia_por_localidad[ nombre_prov ] = [ nombre_localidad ]
            else:
                provincia_por_localidad[ nombre_prov ].append( nombre_localidad )
    provincia_por_localidad = remover_repetidos( provincia_por_localidad )
    return provincia_por_localidad


carpeta_provincia = 'provincias'
if not os.path.exists(carpeta_provincia):
    os.makedirs(carpeta_provincia)

# def escribir_csv_provincia(provincia, localidades):
#     nombre_archivo = os.path.join(carpeta_provincia, f'{provincia}.csv')
#     with open(nombre_archivo, 'w', newline='', encoding='utf-8') as archivo_csv:
#         escritor_csv = csv.writer(archivo_csv)
#         escritor_csv.writerow(['Localidades'])
#         print(localidades)
#         escritor_csv.writerows(localidades)

def escribir_csv_provincia(provincia, localidades):
    nombre_archivo = os.path.join(carpeta_provincia, f'{provincia}.csv')
    with open(nombre_archivo, 'w', newline='', encoding='utf-8') as archivo_csv:
        escritor_csv = csv.writer(archivo_csv)
        escritor_csv.writerow(['Localidades'])
        for localidad in localidades:
            escritor_csv.writerow([localidad])

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

localidades = listar_localidades(cursor)


provincias = agrupar_provincia_localidad(localidades)


for provincia, localidades in provincias.items():
    localidadess = list(localidades)
    escribir_csv_provincia(provincia, localidadess)


conn.commit()

conn.close()


     


