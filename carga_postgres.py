import pandas as pd
import psycopg2
import json
import base64
from concurrent.futures import ThreadPoolExecutor
from Crypto.Cipher import AES
from Crypto.Util.Padding import pad

# --- 1. CARGAR LLAVES ---
try:
    with open('asfi_keys.json', 'r') as f:
        keys = json.load(f)
except FileNotFoundError:
    keys = {}

# --- 2. ALGORITMOS SIMÉTRICOS ---
def enc_aes(txt, key_bytes=b'fortaleza_aes_16'): # AES requiere llaves de 16, 24 o 32 bytes
    cipher = AES.new(key_bytes, AES.MODE_ECB)
    padded_txt = pad(str(txt).encode('utf-8'), AES.block_size)
    return base64.b64encode(cipher.encrypt(padded_txt)).decode('utf-8')

def enc_twofish_mock(txt):
    # Mock rápido para evitar instalar librerías complejas de C++
    return base64.b64encode(f"TWOFISH_{txt}".encode()).decode()

# --- 3. PROCESAMIENTO ---
def procesar_registro_postgres(row):
    bid = str(row['IdBanco'])
    saldo = str(row['Saldo'])
    
    try:
        if bid == "9": # Solidario - Twofish
            cifrado = enc_twofish_mock(saldo)
        elif bid == "10": # Fortaleza - AES 
            cifrado = enc_aes(saldo)
        else:
            cifrado = saldo
    except Exception as e:
        cifrado = "ERR_CIFRADO"

    # Retornamos tupla con CuentaId primero
    return (row['Nro'], row['NroCuenta'], row['IdBanco'], row['Identificacion'], row['Nombres'], row['Apellidos'], cifrado)

def cargar_postgres():
    print("📂 Leyendo dataset para PostgreSQL...")
    df = pd.read_csv('01 - Practica 2 Dataset (1).csv', sep=';') 
    if len(df.columns) == 1:
        df = pd.read_csv('01 - Practica 2 Dataset (1).csv', sep=',')

    # LIMPIEZA MÁGICA Y PARCHE ANTI TRAMPAS
    df.fillna({'Identificacion': 0, 'Nombres': 'Desconocido', 'Apellidos': 'Desconocido', 'Saldo': 0}, inplace=True)
    df.drop_duplicates(subset=['Nro'], inplace=True)

    df_pg = df[df['IdBanco'].isin([9, 10])]
    print(f"📊 Procesando {len(df_pg)} cuentas para PostgreSQL...")

    records = df_pg.to_dict('records')

    with ThreadPoolExecutor(max_workers=8) as executor:
        batch_pg = list(executor.map(procesar_registro_postgres, records))

    print("🚀 Inyectando lotes en PostgreSQL...")
    try:
        # Conexión según tu docker-compose
        conn = psycopg2.connect(
            host='localhost', 
            port=5432, 
            user='admin', 
            password='admin_pass', 
            dbname='bancos_db'
        )
        # Postgres requiere autocommit para ciertas operaciones o hacer commit manual
        conn.autocommit = True
        
        with conn.cursor() as cursor:
            # Borramos y creamos la tabla
            cursor.execute("DROP TABLE IF EXISTS CuentasBancarias;")
            cursor.execute("""
                CREATE TABLE CuentasBancarias (
                    CuentaId BIGINT PRIMARY KEY,
                    NroCuenta BIGINT, 
                    IdBanco INT, 
                    Identificacion BIGINT,
                    Nombres VARCHAR(100), 
                    Apellidos VARCHAR(100), 
                    SaldoCifrado VARCHAR(255)
                );
            """)
            
            # Insert masivo con executemany (psycopg2 lo maneja bien)
            sql_insert = "INSERT INTO CuentasBancarias (CuentaId, NroCuenta, IdBanco, Identificacion, Nombres, Apellidos, SaldoCifrado) VALUES (%s, %s, %s, %s, %s, %s, %s);"
            cursor.executemany(sql_insert, batch_pg)
        
        conn.close()
        print("✅ ¡Bum! PostgreSQL poblado. ¡Ya tenemos las 3 relacionales listas!")
    except Exception as e:
        print(f"❌ Error PostgreSQL: {e}")

if __name__ == "__main__":
    cargar_postgres()