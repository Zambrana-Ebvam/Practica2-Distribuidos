import pandas as pd
from neo4j import GraphDatabase
import json
import base64
from concurrent.futures import ThreadPoolExecutor
from Crypto.Cipher import ChaCha20

# --- 1. CARGAR LLAVES ---
try:
    with open('asfi_keys.json', 'r') as f:
        keys = json.load(f)
except FileNotFoundError:
    keys = {}

# --- 2. ALGORITMO CHACHA20 ---
def enc_chacha20(txt, key_string="nacion_arg_chacha20_32bytes_key"):
    # ChaCha20 exige una llave exacta de 32 bytes (256 bits)
    key_bytes = key_string.encode('utf-8')
    if len(key_bytes) < 32:
        key_bytes = key_bytes.ljust(32, b'0') # Rellenamos con ceros si falta
    elif len(key_bytes) > 32:
        key_bytes = key_bytes[:32] # Cortamos si sobra
    
    # ChaCha20 usa un 'nonce' (número usado una sola vez) que necesitamos para descifrar
    cipher = ChaCha20.new(key=key_bytes)
    ciphertext = cipher.encrypt(str(txt).encode('utf-8'))
    
    # Juntamos el nonce y el texto cifrado para poder descifrarlo en la ASFI después
    encrypted_data = cipher.nonce + ciphertext
    return base64.b64encode(encrypted_data).decode('utf-8')

# --- 3. PROCESAMIENTO ---
def procesar_registro_neo4j(row):
    saldo = str(row['Saldo'])
    
    try:
        # Banco 14 - Nacion Arg usa ChaCha20
        llave_json = keys.get("14", {}).get("key", "nacion_arg_chacha20_32bytes_key")
        cifrado = enc_chacha20(saldo, llave_json)
    except Exception as e:
        cifrado = "ERR_CIFRADO"

    # Retornamos un diccionario listo para el Query de Grafos
    return {
        "CuentaId": row['Nro'],
        "NroCuenta": row['NroCuenta'], 
        "IdBanco": row['IdBanco'], 
        "Identificacion": row['Identificacion'], 
        "Nombres": row['Nombres'], 
        "Apellidos": row['Apellidos'], 
        "SaldoCifrado": cifrado
    }

def cargar_neo4j():
    print("📂 Leyendo dataset para Neo4j (Grafos)...")
    df = pd.read_csv('01 - Practica 2 Dataset (1).csv', sep=';') 
    if len(df.columns) == 1:
        df = pd.read_csv('01 - Practica 2 Dataset (1).csv', sep=',')

    # LIMPIEZA MÁGICA
    df.fillna({'Identificacion': 0, 'Nombres': 'Desconocido', 'Apellidos': 'Desconocido', 'Saldo': 0}, inplace=True)
    df.drop_duplicates(subset=['Nro'], inplace=True)

    df_neo = df[df['IdBanco'] == 14]
    print(f"📊 Procesando {len(df_neo)} cuentas para Neo4j...")

    records = df_neo.to_dict('records')

    with ThreadPoolExecutor(max_workers=8) as executor:
        batch_neo = list(executor.map(procesar_registro_neo4j, records))

    print("🚀 Inyectando nodos y relaciones en Neo4j...")
    
    # Conexión al contenedor local. Ojo con la contraseña que pusiste en el docker-compose
    uri = "bolt://localhost:7687"
    user = "neo4j"
    password = "password123"
    
    try:
        driver = GraphDatabase.driver(uri, auth=(user, password))
        
        # Cypher Query: UNWIND procesa toda la lista de un solo golpe (Bulk Insert)
        # MERGE crea el nodo solo si no existe, ideal para no duplicar clientes
        query = """
        UNWIND $batch AS row
        MERGE (c:Cliente {identificacion: row.Identificacion})
        ON CREATE SET c.nombres = row.Nombres, c.apellidos = row.Apellidos
        
        MERGE (cta:Cuenta {nroCuenta: row.NroCuenta})
        ON CREATE SET cta.cuentaId = row.CuentaId, cta.bancoId = row.IdBanco, cta.saldoCifrado = row.SaldoCifrado
        
        MERGE (c)-[:TIENE_CUENTA]->(cta)
        """
        
        with driver.session() as session:
            # Borramos todo antes por si necesitas correrlo varias veces
            session.run("MATCH (n) DETACH DELETE n")
            # Ejecutamos la inyección masiva
            session.run(query, batch=batch_neo)
            
        driver.close()
        print("✅ ¡Brutal! Neo4j poblado. Los nodos Cliente y Cuenta están conectados.")
    except Exception as e:
        print(f"❌ Error Neo4j: {e}")

if __name__ == "__main__":
    cargar_neo4j()