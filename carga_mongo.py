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
# Generamos la llave una sola vez para no fundir el procesador en el bucle
rsa_key = RSA.generate(1024)
rsa_cipher = PKCS1_OAEP.new(rsa_key.publickey())

def enc_rsa(txt):
    try:
        encrypted = rsa_cipher.encrypt(str(txt).encode('utf-8'))
        return base64.b64encode(encrypted).decode('utf-8')
    except Exception:
        return "ERR_RSA"

def enc_elgamal_mock(txt):
    # Simulador rápido para ElGamal
    return base64.b64encode(f"ELGAMAL_{txt}".encode()).decode()

def enc_ecc_mock(txt):
    # Simulador rápido para ECC
    return base64.b64encode(f"ECC_{txt}".encode()).decode()

# --- 3. PROCESAMIENTO ---
def procesar_registro_mongo(row):
    bid = str(row['IdBanco'])
    saldo = str(row['Saldo'])
    
    try:
        if bid == "11": # FIE - RSA
            cifrado = enc_rsa(saldo)
        elif bid == "12": # Comunidad - ElGamal
            cifrado = enc_elgamal_mock(saldo)
        elif bid == "13": # Desarrollo Productivo - ECC
            cifrado = enc_ecc_mock(saldo)
        else:
            cifrado = saldo
    except Exception as e:
        cifrado = "ERR_CIFRADO"

    # En Mongo insertamos documentos (diccionarios), así que armamos el JSON directo
    return {
        "_id": row['Nro'], # Usamos Nro como _id principal para evitar duplicados en Mongo
        "CuentaId": row['Nro'],
        "NroCuenta": row['NroCuenta'], 
        "IdBanco": row['IdBanco'], 
        "Identificacion": row['Identificacion'], 
        "Nombres": row['Nombres'], 
        "Apellidos": row['Apellidos'], 
        "SaldoCifrado": cifrado
    }

def cargar_mongo():
    print("📂 Leyendo dataset para MongoDB...")
    df = pd.read_csv('01 - Practica 2 Dataset (1).csv', sep=';') 
    if len(df.columns) == 1:
        df = pd.read_csv('01 - Practica 2 Dataset (1).csv', sep=',')

    # LIMPIEZA MÁGICA Y PARCHE ANTI TRAMPAS
    df.fillna({'Identificacion': 0, 'Nombres': 'Desconocido', 'Apellidos': 'Desconocido', 'Saldo': 0}, inplace=True)
    df.drop_duplicates(subset=['Nro'], inplace=True)

    df_mongo = df[df['IdBanco'].isin([11, 12, 13])]
    print(f"📊 Procesando {len(df_mongo)} cuentas para MongoDB...")

    records = df_mongo.to_dict('records')

    with ThreadPoolExecutor(max_workers=8) as executor:
        batch_mongo = list(executor.map(procesar_registro_mongo, records))

    print("🚀 Inyectando lotes en MongoDB...")
    try:
        # Nos conectamos al contenedor local (puerto 27017 por defecto en tu docker-compose)
        cliente_mongo = MongoClient("mongodb://localhost:27017/")
        db_mongo = cliente_mongo["bancos_db"]
        coleccion = db_mongo["CuentasBancarias"]
        
        # Limpiamos la colección por si la corres varias veces
        coleccion.drop()
        
        # Inserción masiva. Es extremadamente rápido en NoSQL.
        if batch_mongo:
            coleccion.insert_many(batch_mongo)
            
        print("✅ MongoDB poblado al cien. Ya tenemos los no relacionales dominados.")
    except Exception as e:
        print(f"❌ Error MongoDB: {e}")

if __name__ == "__main__":
    cargar_mongo()