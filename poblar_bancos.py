import pandas as pd
import pymysql
import pyodbc # Para SQL Server
import json

# 1. Cargar llaves y configuración
with open('asfi_keys.json', 'r') as f:  
    keys = json.load(f)

# --- FUNCIONES DE CIFRADO CLÁSICO ---
def enc_cesar(txt, s=5):
    return "".join([chr(ord(c) + s) for c in str(txt)])

def enc_atbash(txt):
    chars = "0123456789.ABCDEFGHIJKLM"
    rev = chars[::-1]
    return str(txt).translate(str.maketrans(chars, rev))

def enc_vigenere(txt, key="BOL"):
    txt = str(txt)
    return "".join([chr(ord(txt[i]) + ord(key[i % len(key)])) for i in range(len(txt))])

# --- PROCESO DE CARGA ---
def cargar_relacionales():
    print("📂 Leyendo dataset...")
    df = pd.read_csv('datos (2)')
    
    batch_sql_server = [] # Bancos 1, 2, 3
    batch_mysql = []      # Bancos 4, 5, 6

    print("🔐 Cifrando datos de Bancos 1 al 6...")
    for _, row in df.iterrows():
        bid = str(row['IdBanco'])
        saldo = str(row['Saldo'])
        
        # Solo procesamos los que van a SQL Server y MySQL
        if bid == "1": cifrado = enc_cesar(saldo, keys[bid]['shift'])
        elif bid == "2": cifrado = enc_atbash(saldo)
        elif bid == "3": cifrado = enc_vigenere(saldo, keys[bid]['key'])
        elif bid in ["4", "5", "6"]: cifrado = enc_cesar(saldo, 10) # Ejemplo rápido
        else: continue # El resto (Mongo/Firebase) los haremos en otro script

        registro = (row['Nro'], row['IdBanco'], row['Identificacion'], row['Nombres'], row['Apellidos'], row['NroCuenta'], cifrado)

        if bid in ["1", "2", "3"]: batch_sql_server.append(registro)
        else: batch_mysql.append(registro)

    # --- INYECTAR EN SQL SERVER ---
    try:
        print("🚀 Inyectando a SQL Server (Bancos 1, 2, 3)...")
        conn_str = 'DRIVER={ODBC Driver 17 for SQL Server};SERVER=localhost;DATABASE=ASFI_Bancos_Relacionales;UID=sa;PWD=YourStrong@Passw0rd'
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()
            cursor.executemany("INSERT INTO Cuentas (CuentaId, BancoId, Identificacion, Nombres, Apellidos, NroCuenta, SaldoCifrado) VALUES (?,?,?,?,?,?,?)", batch_sql_server)
        print("✅ SQL Server poblado con éxito.")
    except Exception as e: print(f"❌ Error SQL Server: {e}")

    # --- INYECTAR EN MYSQL ---
    try:
        print("🚀 Inyectando a MySQL (Bancos 4, 5, 6)...")
        conn = pymysql.connect(host='localhost', port=3307, user='root', password='root_pass', db='Banco_Relacional_2')
        with conn.cursor() as cursor:
            cursor.executemany("INSERT INTO Cuentas (CuentaId, BancoId, Identificacion, Nombres, Apellidos, NroCuenta, SaldoCifrado) VALUES (%s,%s,%s,%s,%s,%s,%s)", batch_mysql)
        conn.commit()
        conn.close()
        print("✅ MySQL poblado con éxito.")
    except Exception as e: print(f"❌ Error MySQL: {e}")

if __name__ == "__main__":
    cargar_relacionales()