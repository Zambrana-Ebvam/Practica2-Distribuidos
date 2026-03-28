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
def enc_aes(txt, key_bytes=b'fortaleza_aes_16'): 
    cipher = AES.new(key_bytes, AES.MODE_ECB)
    padded_txt = pad(str(txt).encode('utf-8'), AES.block_size)
    return base64.b64encode(cipher.encrypt(padded_txt)).decode('utf-8')

def enc_twofish_mock(txt):
    return base64.b64encode(f"TWOFISH_{txt}".encode()).decode()

# --- 3. PROCESAMIENTO ---
def procesar_registro_postgres(row):
    bid = str(int(float(row['IdBanco'])))
    saldo = str(row['Saldo'])
    
    try:
        if bid == "9": 
            cifrado = enc_twofish_mock(saldo)
        elif bid == "10": 
            cifrado = enc_aes(saldo)
        else:
            cifrado = saldo
    except Exception as e:
        cifrado = "ERR_CIFRADO"

    return (row['Nro'], str(row['NroCuenta']), row['IdBanco'], str(row['Identificacion']), row['Nombres'], row['Apellidos'], cifrado)

def cargar_postgres():
    print("📂 Leyendo dataset para PostgreSQL...")
    df = pd.read_csv('datos (2).csv', sep=',') 
    
    df.fillna({'Identificacion': 0, 'Nombres': 'Desconocido', 'Apellidos': 'Desconocido', 'Saldo': 0}, inplace=True)
    df.dropna(subset=['IdBanco'], inplace=True)
    df.drop_duplicates(inplace=True)

    df_pg = df[df['IdBanco'].isin([9, 10])]
    print(f"📊 Procesando {len(df_pg)} cuentas para PostgreSQL...")

    records = df_pg.to_dict('records')

    with ThreadPoolExecutor(max_workers=8) as executor:
        batch_pg = list(executor.map(procesar_registro_postgres, records))

    print("🚀 Inyectando lotes en PostgreSQL...")
    try:
        conn = psycopg2.connect(
            host='localhost', 
            port=5432, 
            user='admin', 
            password='admin_pass', 
            dbname='bancos_db'
        )
        conn.autocommit = True
        
        with conn.cursor() as cursor:
            cursor.execute("DROP TABLE IF EXISTS CuentasBancarias;")
            cursor.execute("""
                CREATE TABLE CuentasBancarias (
                    CuentaId BIGINT PRIMARY KEY,
                    NroCuenta VARCHAR(100), 
                    IdBanco INT, 
                    Identificacion VARCHAR(100),
                    Nombres VARCHAR(100), 
                    Apellidos VARCHAR(100), 
                    SaldoCifrado VARCHAR(255),
                    SaldoBs DECIMAL(18,4) NULL,
                    CodigoVerificacion VARCHAR(8) NULL
                );
            """)
            
            sql_insert = "INSERT INTO CuentasBancarias (CuentaId, NroCuenta, IdBanco, Identificacion, Nombres, Apellidos, SaldoCifrado) VALUES (%s, %s, %s, %s, %s, %s, %s);"
            cursor.executemany(sql_insert, batch_pg)
        
        conn.close()
        print("✅ PostgreSQL poblado.")
    except Exception as e:
        print(f"❌ Error PostgreSQL: {e}")

if __name__ == "__main__":
    cargar_postgres()