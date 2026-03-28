import pandas as pd
from pymongo import MongoClient
import json
import base64
from concurrent.futures import ThreadPoolExecutor
from Crypto.PublicKey import RSA
from Crypto.Cipher import PKCS1_OAEP

# --- 1. CARGAR LLAVES ---
try:
    with open('asfi_keys.json', 'r') as f:
        keys = json.load(f)
except FileNotFoundError:
    keys = {}

# --- 2. ALGORITMOS ASIMÉTRICOS ---
print("Generando llaves RSA (esto toma unos segundos)...")
rsa_key = RSA.generate(1024)
rsa_cipher = PKCS1_OAEP.new(rsa_key.publickey())

def enc_rsa(txt):
    try:
        encrypted = rsa_cipher.encrypt(str(txt).encode('utf-8'))
        return base64.b64encode(encrypted).decode('utf-8')
    except Exception:
        return "ERR_RSA"

def enc_elgamal_mock(txt):
    return base64.b64encode(f"ELGAMAL_{txt}".encode()).decode()

def enc_ecc_mock(txt):
    return base64.b64encode(f"ECC_{txt}".encode()).decode()

# --- 3. PROCESAMIENTO ---
def procesar_registro_mongo(row):
    bid = str(int(float(row['IdBanco'])))
    saldo = str(row['Saldo'])
    
    try:
        if bid == "11": 
            cifrado = enc_rsa(saldo)
        elif bid == "12": 
            cifrado = enc_elgamal_mock(saldo)
        elif bid == "13": 
            cifrado = enc_ecc_mock(saldo)
        else:
            cifrado = saldo
    except Exception as e:
        cifrado = "ERR_CIFRADO"

    return {
        "_id": row['Nro'], 
        "CuentaId": row['Nro'],
        "NroCuenta": str(row['NroCuenta']), 
        "IdBanco": row['IdBanco'], 
        "Identificacion": str(row['Identificacion']), 
        "Nombres": row['Nombres'], 
        "Apellidos": row['Apellidos'], 
        "SaldoCifrado": cifrado
    }

def cargar_mongo():
    print("📂 Leyendo dataset para MongoDB...")
    df = pd.read_csv('datos (2).csv', sep=',') 
    
    df.fillna({'Identificacion': 0, 'Nombres': 'Desconocido', 'Apellidos': 'Desconocido', 'Saldo': 0}, inplace=True)
    df.dropna(subset=['IdBanco'], inplace=True)
    df.drop_duplicates(inplace=True)

    df_mongo = df[df['IdBanco'].isin([11, 12, 13])]
    print(f"📊 Procesando {len(df_mongo)} cuentas para MongoDB...")

    records = df_mongo.to_dict('records')

    with ThreadPoolExecutor(max_workers=8) as executor:
        batch_mongo = list(executor.map(procesar_registro_mongo, records))

    print("🚀 Inyectando lotes en MongoDB...")
    try:
        cliente_mongo = MongoClient("mongodb://localhost:27017/")
        db_mongo = cliente_mongo["bancos_db"]
        coleccion = db_mongo["CuentasBancarias"]
        
        coleccion.drop()
        
        if batch_mongo:
            coleccion.insert_many(batch_mongo)
            
        print("✅ MongoDB poblado al cien.")
    except Exception as e:
        print(f"❌ Error MongoDB: {e}")

if __name__ == "__main__":
    cargar_mongo()