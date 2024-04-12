
from main import host, user, password, database, conectar_mysql

def get_localidades():
    try:
        conn = conectar_mysql( host, user, password, database )
        cursor = conn.cursor()
    except Exception as e:
        print(f"Error al conectar a la base de datos: {e}")
        return None
    try:
        cursor.execute("SELECT * FROM localidades")
        localidades = cursor.fetchall()
        print(localidades)
        return localidades
    except Exception as e:
        print(f"Error al obtener las localidades: {e}")
        return None
    finally:
        cursor.close()
        conn.close()    


        get_localidades();