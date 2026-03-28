import pandas as pd
from neo4j import GraphDatabase
from concurrent.futures import ThreadPoolExecutor

# --- 1. PROCESAMIENTO SIN CRIPTOGRAFÍA ---
def procesar_registro_neo4j(row):
    saldo_real = str(row['Saldo']) 

    return {
        "CuentaId": row['Nro'],
        "NroCuenta": str(row['NroCuenta']), 
        "IdBanco": row['IdBanco'], 
        "Identificacion": str(row['Identificacion']), 
        "Nombres": row['Nombres'], 
        "Apellidos": row['Apellidos'], 
        "SaldoCifrado": saldo_real 
    }

def cargar_neo4j():
    print("📂 Leyendo dataset para Neo4j (Grafos - Texto Plano)...")
    df = pd.read_csv('datos (2).csv', sep=',') 
    
    df.fillna({'Identificacion': 0, 'Nombres': 'Desconocido', 'Apellidos': 'Desconocido', 'Saldo': 0}, inplace=True)
    df.dropna(subset=['IdBanco'], inplace=True)
    df.drop_duplicates(inplace=True)

    df_neo = df[df['IdBanco'] == 14]
    print(f"📊 Procesando {len(df_neo)} cuentas para Neo4j...")

    records = df_neo.to_dict('records')

    with ThreadPoolExecutor(max_workers=8) as executor:
        batch_neo = list(executor.map(procesar_registro_neo4j, records))

    print("🚀 Inyectando nodos y relaciones en Neo4j sin encriptar...")
    
    uri = "bolt://localhost:7687"
    user = "neo4j"
    password = "password123"
    
    try:
        driver = GraphDatabase.driver(uri, auth=(user, password))
        
        query = """
        UNWIND $batch AS row
        MERGE (c:Cliente {identificacion: row.Identificacion})
        ON CREATE SET c.nombres = row.Nombres, c.apellidos = row.Apellidos
        
        MERGE (cta:Cuenta {nroCuenta: row.NroCuenta})
        ON CREATE SET cta.cuentaId = row.CuentaId, cta.bancoId = row.IdBanco, cta.saldoCifrado = row.SaldoCifrado
        
        MERGE (c)-[:TIENE_CUENTA]->(cta)
        """
        
        with driver.session() as session:
            session.run("MATCH (n) DETACH DELETE n") 
            session.run(query, batch=batch_neo) 
            
        driver.close()
        print("✅ Neo4j poblado en texto plano.")
    except Exception as e:
        print(f"❌ Error Neo4j: {e}")

if __name__ == "__main__":
    cargar_neo4j()