import pandas as pd
import pymysql
import json
import base64
from concurrent.futures import ThreadPoolExecutor
from Crypto.Cipher import DES, DES3, Blowfish
from Crypto.Util.Padding import pad

# --- 1. CARGAR LLAVES ---
try:
    with open('asfi_keys.json', 'r') as f:
        keys = json.load(f)
except FileNotFoundError:
    keys = {}

# --- 2. ALGORITMOS SIMÉTRICOS ---
def enc_bloque(txt, key_bytes, cipher_module):
    cipher = cipher_module.new(key_bytes, cipher_module.MODE_ECB)
    padded_txt = pad(str(txt).encode('utf-8'), cipher_module.block_size)
    return base64.b64encode(cipher.encrypt(padded_txt)).decode('utf-8')

def enc_playfair_mock(txt):
    return base64.b64encode(str(txt).encode()).decode()

def enc_hill_mock(txt):
    return base64.b64encode(str(txt)[::-1].encode()).decode()

# --- 3. PROCESAMIENTO ---
def procesar_registro_mysql(row):
    bid = str(int(float(row['IdBanco'])))
    saldo = str(row['Saldo'])
    
    try:
        if bid == "4": 
            cifrado = enc_playfair_mock(saldo)
        elif bid == "5": 
            cifrado = enc_hill_mock(saldo)
        elif bid == "6": 
            cifrado = enc_bloque(saldo, b'8bytekey', DES)
        elif bid == "7": 
            cifrado = enc_bloque(saldo, b'16bytekey_3des__', DES3)
        elif bid == "8": 
            cifrado = enc_bloque(saldo, b'secret_key_prodem', Blowfish)
        else:
            cifrado = saldo
    except Exception as e:
        cifrado = "ERR_CIFRADO"

    return (row['Nro'], str(row['NroCuenta']), row['IdBanco'], str(row['Identificacion']), row['Nombres'], row['Apellidos'], cifrado)

def cargar_mysql():
    print("📂 Leyendo dataset para MySQL...")
    df = pd.read_csv('datos (2).csv', sep=',') 
    
    df.fillna({'Identificacion': 0, 'Nombres': 'Desconocido', 'Apellidos': 'Desconocido', 'Saldo': 0}, inplace=True)
    df.dropna(subset=['IdBanco'], inplace=True)
    df.drop_duplicates(inplace=True)

    df_mysql = df[df['IdBanco'].isin([4, 5, 6, 7, 8])]
    print(f"📊 Procesando {len(df_mysql)} cuentas para MySQL...")

    records = df_mysql.to_dict('records')

    with ThreadPoolExecutor(max_workers=8) as executor:
        batch_mysql = list(executor.map(procesar_registro_mysql, records))

    print("🚀 Inyectando lotes en MySQL...")
    try:
        conn = pymysql.connect(host='localhost', port=3307, user='root', password='root_pass')
        with conn.cursor() as cursor:
            cursor.execute("CREATE DATABASE IF NOT EXISTS bancos_db")
            cursor.execute("USE bancos_db")
            
            cursor.execute("DROP TABLE IF EXISTS CuentasBancarias")
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
                )
            """)
            
            sql_insert = "INSERT INTO CuentasBancarias (CuentaId, NroCuenta, IdBanco, Identificacion, Nombres, Apellidos, SaldoCifrado) VALUES (%s, %s, %s, %s, %s, %s, %s)"
            cursor.executemany(sql_insert, batch_mysql)
        
        conn.commit()
        conn.close()
        print("✅ MySQL poblado.")
    except Exception as e:
        print(f"❌ Error MySQL: {e}")

if __name__ == "__main__":
    cargar_mysql()