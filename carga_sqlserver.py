import pandas as pd
import pyodbc
import json
from concurrent.futures import ThreadPoolExecutor

# --- 1. CARGAR LLAVES ---
try:
    with open('asfi_keys.json', 'r') as f:
        keys = json.load(f)
except FileNotFoundError:
    print("Ojo: No encuentro el asfi_keys.json. Usando valores por defecto.")
    keys = {"1": {"shift": 5}, "3": {"key": "BOLIVIA"}}

# --- 2. ALGORITMOS DE CIFRADO ---
def enc_cesar(txt, shift=5):
    return "".join([chr(ord(c) + shift) for c in str(txt)])

def enc_atbash(txt):
    chars = "0123456789.-ABCDEFGHIJKL"
    rev = chars[::-1]
    try:
        return str(txt).translate(str.maketrans(chars, rev))
    except Exception:
        return str(txt)

def enc_vigenere(txt, key="BOLIVIA"):
    txt = str(txt)
    return "".join([chr((ord(txt[i]) + ord(key[i % len(key)])) % 256) for i in range(len(txt))])

# --- 3. PROCESAMIENTO ---
def procesar_registro_sql(row):
    bid = str(row['IdBanco'])
    saldo = str(row['Saldo'])
    
    try:
        if bid == "1":
            cifrado = enc_cesar(saldo, keys.get("1", {}).get("shift", 5))
        elif bid == "2":
            cifrado = enc_atbash(saldo)
        elif bid == "3":
            cifrado = enc_vigenere(saldo, keys.get("3", {}).get("key", "BOLIVIA"))
        else:
            cifrado = saldo
    except Exception:
        cifrado = "ERR_CIFRADO"

    # Ahora devolvemos también el 'Nro' (CuentaId) al principio
    return (row['Nro'], row['NroCuenta'], row['IdBanco'], row['Identificacion'], row['Nombres'], row['Apellidos'], cifrado)

def cargar_sql_server():
    print("📂 Leyendo dataset para SQL Server...")
    df = pd.read_csv('01 - Practica 2 Dataset (1).csv', sep=';') 
    if len(df.columns) == 1:
        df = pd.read_csv('01 - Practica 2 Dataset (1).csv', sep=',')

    # LIMPIEZA MÁGICA
    df.fillna({'Identificacion': 0, 'Nombres': 'Desconocido', 'Apellidos': 'Desconocido', 'Saldo': 0}, inplace=True)
    
    # VOLAMOS DUPLICADOS POR SI ACASO
    df.drop_duplicates(subset=['Nro'], inplace=True)

    df_sql = df[df['IdBanco'].isin([1, 2, 3])]
    print(f"📊 Procesando {len(df_sql)} cuentas para SQL Server...")

    records = df_sql.to_dict('records')

    with ThreadPoolExecutor(max_workers=8) as executor:
        batch_sql = list(executor.map(procesar_registro_sql, records))

    print("🚀 Inyectando lotes en SQL Server...")
    conn_str = 'DRIVER={ODBC Driver 17 for SQL Server};SERVER=localhost,1433;DATABASE=master;UID=sa;PWD=SuperStrongPass123!'
    
    try:
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()
            
            # Borramos la tabla vieja si existe y creamos la nueva con el esquema correcto
            cursor.execute("""
                IF OBJECT_ID('CuentasBancarias', 'U') IS NOT NULL DROP TABLE CuentasBancarias;
                CREATE TABLE CuentasBancarias (
                    CuentaId BIGINT PRIMARY KEY, 
                    NroCuenta BIGINT, 
                    IdBanco INT, 
                    Identificacion BIGINT,
                    Nombres VARCHAR(100), 
                    Apellidos VARCHAR(100), 
                    SaldoCifrado VARCHAR(255)
                )
            """)
            conn.commit()

            # Actualizamos el INSERT para que reciba los 7 datos
            sql_insert = "INSERT INTO CuentasBancarias (CuentaId, NroCuenta, IdBanco, Identificacion, Nombres, Apellidos, SaldoCifrado) VALUES (?, ?, ?, ?, ?, ?, ?)"
            cursor.fast_executemany = True 
            cursor.executemany(sql_insert, batch_sql)
            conn.commit()
            
        print("✅ ¡Bum! SQL Server poblado a la perfección bro.")
    except Exception as e:
        print(f"❌ Error SQL Server: {e}")

if __name__ == "__main__":
    cargar_sql_server()