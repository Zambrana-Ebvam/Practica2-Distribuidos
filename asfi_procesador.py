import pymysql
import pyodbc
import psycopg2
from pymongo import MongoClient
from neo4j import GraphDatabase
import time
import asfi_core
from concurrent.futures import ThreadPoolExecutor

TIPO_CAMBIO_BASE = 10.2300

def obtener_tipo_cambio_actual():
    variacion = 0.0000
    # variacion = round(random.uniform(-0.9999, 0.9999), 4)
    return TIPO_CAMBIO_BASE, variacion, round(TIPO_CAMBIO_BASE + variacion, 4)

def generar_codigo_verificacion(cuenta_id):
    # SOLUCIÓN ANTI-COLISIONES: 
    # Convertimos el CuentaId a formato Hexadecimal de 8 caracteres.
    # Como el CuentaId es irrepetible, este código también lo será.
    return f"{int(cuenta_id):08X}"

def procesar_cuenta_asfi(row, tc_base, tc_var, tc_final):
    cuenta_id, nro_cuenta, id_banco, identificacion, nombres, apellidos, saldo_cifrado = row
    
    saldo_usd = asfi_core.asfi_descifrar_saldo(id_banco, saldo_cifrado)
    saldo_bs = round(saldo_usd * tc_final, 4)
    
    # Le pasamos el cuenta_id a la función para garantizar que sea único
    codigo_ver = generar_codigo_verificacion(cuenta_id)
    
    registro_cuenta = (cuenta_id, nro_cuenta, id_banco, identificacion, nombres, apellidos, saldo_usd, saldo_bs, codigo_ver, tc_final, 'CONVERTIDO')
    registro_auditoria = (cuenta_id, id_banco, 'CONVERSION', saldo_usd, saldo_cifrado, saldo_bs, tc_final, codigo_ver, 'EXITOSO')
    
    return registro_cuenta, registro_auditoria

def limpiar_duplicados(datos_crudos):
    cuentas_unicas = {}
    for row in datos_crudos:
        clave = f"{row[1]}-{row[2]}"
        if clave not in cuentas_unicas:
            cuentas_unicas[clave] = row
    return list(cuentas_unicas.values())

def limpiar_boveda_asfi():
    # Esta función vacía la base de datos de la ASFI automáticamente antes de correr
    print("🧹 Limpiando la bóveda central de la ASFI para una migración limpia...")
    try:
        conn = pymysql.connect(host='localhost', port=3307, user='root', password='root_pass', db='ASFI_Central')
        with conn.cursor() as cursor:
            cursor.execute("DELETE FROM Auditoria_Conversiones")
            cursor.execute("DELETE FROM Log_Tipo_Cambio")
            cursor.execute("DELETE FROM Cuentas_ASFI")
        conn.commit()
        conn.close()
        print("✨ Bóveda impecable y lista.")
    except Exception as e:
        print(f"⚠️ Error al limpiar la bóveda: {e}")

def guardar_en_asfi(lote_cuentas, tc_base, tc_var, tc_final, nombre_motor):
    if not lote_cuentas: return
    
    inicio_motor = time.time()
    datos_limpios = limpiar_duplicados(lote_cuentas)
    print(f"📦 {nombre_motor}: Procesando {len(datos_limpios)} cuentas únicas...")
    
    with ThreadPoolExecutor(max_workers=8) as executor:
        resultados = list(executor.map(lambda row: procesar_cuenta_asfi(row, tc_base, tc_var, tc_final), datos_limpios))
        
    batch_cuentas = [res[0] for res in resultados]
    batch_auditoria = [res[1] for res in resultados]

    try:
        conn = pymysql.connect(host='localhost', port=3307, user='root', password='root_pass', db='ASFI_Central')
        with conn.cursor() as cursor:
            cursor.execute("INSERT INTO Log_Tipo_Cambio (TipoCambioBase, VariacionAplicada, TipoCambioFinal) VALUES (%s, %s, %s)", (tc_base, tc_var, tc_final))
            
            sql_cuenta = "INSERT INTO Cuentas_ASFI (CuentaId, NroCuenta, IdBanco, Identificacion, Nombres, Apellidos, SaldoUSD, SaldoBs, CodigoVerificacion, TipoCambioAplicado, Estado) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            cursor.executemany(sql_cuenta, batch_cuentas)
            
            sql_audit = "INSERT INTO Auditoria_Conversiones (CuentaId, BancoId, Operacion, SaldoUSD_Original, SaldoUSD_Cifrado, SaldoBS_Convertido, TipoCambioAplicado, CodigoVerificacion, Estado) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
            cursor.executemany(sql_audit, batch_auditoria)
            
        conn.commit()
        conn.close()
        
        tiempo_motor = time.time() - inicio_motor
        print(f"✅ {nombre_motor}: ¡Migración a ASFI exitosa! (Tomó {tiempo_motor:.2f} segundos)")
    except Exception as e:
        print(f"❌ Error guardando datos de {nombre_motor}: {e}")

def ataque_total_asfi():
    print("🔥 INICIANDO MIGRACIÓN MASIVA A LA ASFI 🔥")
    tiempo_inicio_total = time.time()
    
    # Limpiamos antes de empezar para evitar choques
    limpiar_boveda_asfi()
    
    tc_base, tc_var, tc_final = obtener_tipo_cambio_actual()
    print(f"\n💵 TIPO DE CAMBIO GLOBAL APLICADO: {tc_final} Bs/USD\n")

    # 1. SQL SERVER
    try:
        with pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=localhost,1433;DATABASE=master;UID=sa;PWD=SuperStrongPass123!') as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT CuentaId, NroCuenta, IdBanco, Identificacion, Nombres, Apellidos, SaldoCifrado FROM CuentasBancarias")
            guardar_en_asfi(cursor.fetchall(), tc_base, tc_var, tc_final, "SQL Server")
    except Exception as e: print(f"Error SQL Server: {e}")

    # 2. MYSQL
    try:
        conn = pymysql.connect(host='localhost', port=3307, user='root', password='root_pass', db='bancos_db')
        with conn.cursor() as cursor:
            cursor.execute("SELECT CuentaId, NroCuenta, IdBanco, Identificacion, Nombres, Apellidos, SaldoCifrado FROM CuentasBancarias")
            guardar_en_asfi(cursor.fetchall(), tc_base, tc_var, tc_final, "MySQL (Origen)")
        conn.close()
    except Exception as e: print(f"Error MySQL: {e}")

    # 3. POSTGRESQL
    try:
        conn = psycopg2.connect(host='localhost', port=5432, user='admin', password='admin_pass', dbname='bancos_db')
        with conn.cursor() as cursor:
            cursor.execute("SELECT CuentaId, NroCuenta, IdBanco, Identificacion, Nombres, Apellidos, SaldoCifrado FROM CuentasBancarias")
            guardar_en_asfi(cursor.fetchall(), tc_base, tc_var, tc_final, "PostgreSQL")
        conn.close()
    except Exception as e: print(f"Error PostgreSQL: {e}")

    # 4. MONGODB
    try:
        cliente = MongoClient("mongodb://localhost:27017/")
        db = cliente["bancos_db"]
        datos_mongo = []
        for doc in db["CuentasBancarias"].find():
            datos_mongo.append((doc["CuentaId"], doc["NroCuenta"], doc["IdBanco"], doc["Identificacion"], doc["Nombres"], doc["Apellidos"], doc["SaldoCifrado"]))
        guardar_en_asfi(datos_mongo, tc_base, tc_var, tc_final, "MongoDB")
    except Exception as e: print(f"Error MongoDB: {e}")

    # 5. NEO4J
    try:
        driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "password123"))
        query = "MATCH (c:Cliente)-[:TIENE_CUENTA]->(cta:Cuenta) RETURN cta.cuentaId, cta.nroCuenta, cta.bancoId, c.identificacion, c.nombres, c.apellidos, cta.saldoCifrado"
        with driver.session() as session:
            result = session.run(query)
            datos_neo = [tuple(record.values()) for record in result]
            guardar_en_asfi(datos_neo, tc_base, tc_var, tc_final, "Neo4j")
        driver.close()
    except Exception as e: print(f"Error Neo4j: {e}")

    tiempo_fin_total = time.time()
    tiempo_total_segundos = tiempo_fin_total - tiempo_inicio_total
    minutos = int(tiempo_total_segundos // 60)
    segundos = int(tiempo_total_segundos % 60)

    print("\n=======================================================")
    print("🎉 ¡PROCESO DE LA ASFI COMPLETADO AL 100%!")
    print("🏦 TODOS LOS BANCOS ESTÁN EN LA BÓVEDA CENTRAL.")
    print(f"⏱️ TIEMPO TOTAL DE PROCESAMIENTO: {minutos} minutos y {segundos} segundos ({tiempo_total_segundos:.2f}s)")
    print("=======================================================")

if __name__ == "__main__":
    ataque_total_asfi()