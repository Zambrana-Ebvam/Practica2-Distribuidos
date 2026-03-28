import pymysql
import requests
import time
import random
import threading
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
import asfi_core

MAPA_BANCOS = {
    1: "http://localhost:8001", 2: "http://localhost:8001", 3: "http://localhost:8001",
    4: "http://localhost:8002", 5: "http://localhost:8002", 6: "http://localhost:8002", 7: "http://localhost:8002", 8: "http://localhost:8002",
    9: "http://localhost:8003", 10: "http://localhost:8003",
    11: "http://localhost:8004", 12: "http://localhost:8004", 13: "http://localhost:8004",
    14: "http://localhost:8005"
}

TIPO_CAMBIO_BASE = 6.9600
TIEMPO_ULTIMA_ACTUALIZACION = 0
TC_BASE, TC_VAR, TC_FINAL = 0, 0, 0
bloqueo_bcb = threading.Lock() 

def obtener_tipo_cambio_dinamico():
    global TIEMPO_ULTIMA_ACTUALIZACION, TC_BASE, TC_VAR, TC_FINAL
    with bloqueo_bcb:
        if TC_FINAL == 0 or (time.time() - TIEMPO_ULTIMA_ACTUALIZACION) > 180:
            TC_VAR = round(random.uniform(-0.9999, 0.9999), 4)
            TC_BASE = TIPO_CAMBIO_BASE
            TC_FINAL = round(TC_BASE + TC_VAR, 4)
            TIEMPO_ULTIMA_ACTUALIZACION = time.time()
            print(f"\n🔄 [BCB] ¡Fluctuación de mercado! Nuevo TC: {TC_FINAL} Bs/USD\n")
        return TC_BASE, TC_VAR, TC_FINAL

def escribir_log_fisico(cuenta_id, banco_id, tc_aplicado):
    with open("asfi_auditoria.log", "a", encoding="utf-8") as f:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        f.write(f"[{timestamp}] - BANCO_ID: {banco_id} | CUENTA_ID: {cuenta_id} | TC_APLICADO: {tc_aplicado}\n")

def limpiar_boveda_asfi():
    try:
        conn = pymysql.connect(host='localhost', port=3307, user='root', password='root_pass', db='ASFI_Central')
        with conn.cursor() as cursor:
            cursor.execute("DELETE FROM Auditoria_Conversiones")
            cursor.execute("DELETE FROM Log_Tipo_Cambio")
            cursor.execute("DELETE FROM Cuentas_ASFI")
        conn.commit()
        conn.close()
    except Exception:
        pass

def limpiar_duplicados_api(cuentas_json):
    cuentas_unicas = {}
    for c in cuentas_json:
        clave = f"{c['nro_cuenta']}-{c['id_banco']}"
        if clave not in cuentas_unicas:
            cuentas_unicas[clave] = c
    return list(cuentas_unicas.values())

def procesar_cuenta_via_api(cuenta):
    tc_base, tc_var, tc_final = obtener_tipo_cambio_dinamico()
    
    saldo_usd = asfi_core.asfi_descifrar_saldo(cuenta['id_banco'], cuenta['saldo_cifrado'])
    saldo_bs = round(saldo_usd * tc_final, 4)
    codigo_ver = f"{int(cuenta['cuenta_id']):08X}"
    
    escribir_log_fisico(cuenta['cuenta_id'], cuenta['id_banco'], tc_final)
    
    # ¡YA NO HAY REQUESTS.PUT AQUÍ! Todo se guarda en la RAM
    datos_cierre = {"cuenta_id": cuenta['cuenta_id'], "saldo_bs": saldo_bs, "codigo_ver": codigo_ver}
    registro_cuenta = (cuenta['cuenta_id'], cuenta['nro_cuenta'], cuenta['id_banco'], cuenta['identificacion'], cuenta['nombres'], cuenta['apellidos'], saldo_usd, saldo_bs, codigo_ver, tc_final, 'CONVERTIDO')
    registro_auditoria = (cuenta['cuenta_id'], cuenta['id_banco'], 'CONVERSION', saldo_usd, cuenta['saldo_cifrado'], saldo_bs, tc_final, codigo_ver, 'EXITOSO')
    
    return registro_cuenta, registro_auditoria, datos_cierre

def procesar_banco_microservicio(item):
    id_banco, url_api = item
    inicio = time.time()
    try:
        respuesta = requests.get(f"{url_api}/api/banco/{id_banco}/cuentas")
        if respuesta.status_code != 200: return [], []
        
        cuentas_limpias = limpiar_duplicados_api(respuesta.json()['data'])
        
        with ThreadPoolExecutor(max_workers=8) as executor:
            resultados = list(executor.map(procesar_cuenta_via_api, cuentas_limpias))
            
        batch_cuentas = [res[0] for res in resultados]
        batch_auditoria = [res[1] for res in resultados]
        lote_cierres = [res[2] for res in resultados]
        
        # 🔥 EL ATAQUE BATCH: Mandamos los miles de registros en UN SOLO VIAJE
        try:
            requests.put(f"{url_api}/api/banco/{id_banco}/cierre_masivo", json=lote_cierres)
        except Exception as e:
            print(f"⚠️ Error actualizando banco {id_banco}: {e}")
            
        print(f"✅ Banco {id_banco} completado full batch en {time.time() - inicio:.2f}s")
        return batch_cuentas, batch_auditoria
    except Exception as e:
        print(f"❌ Falló conexión con Banco {id_banco}: {e}")
        return [], []

def orquestador_supremo_asfi():
    print("🔥 INICIANDO ORQUESTADOR ASFI (MODO FERRARI BATCH) 🔥")
    tiempo_inicio = time.time()
    
    limpiar_boveda_asfi()
    open("asfi_auditoria.log", "w", encoding="utf-8").close()
    
    todas_cuentas = []
    todas_auditorias = []

    print("\n🚀 Disparando peticiones simultáneas a las 5 APIs...\n")
    with ThreadPoolExecutor(max_workers=14) as macro_executor:
        resultados_bancos = list(macro_executor.map(procesar_banco_microservicio, MAPA_BANCOS.items()))

    for cuentas, auditorias in resultados_bancos:
        todas_cuentas.extend(cuentas)
        todas_auditorias.extend(auditorias)
        
    print("\n🚀 Guardando en Meruvia (ASFI Central)...")
    try:
        conn = pymysql.connect(host='localhost', port=3307, user='root', password='root_pass', db='ASFI_Central')
        with conn.cursor() as cursor:
            tc_base, tc_var, tc_final = obtener_tipo_cambio_dinamico()
            cursor.execute("INSERT INTO Log_Tipo_Cambio (TipoCambioBase, VariacionAplicada, TipoCambioFinal) VALUES (%s, %s, %s)", (tc_base, tc_var, tc_final))
            
            cursor.executemany("INSERT INTO Cuentas_ASFI (CuentaId, NroCuenta, IdBanco, Identificacion, Nombres, Apellidos, SaldoUSD, SaldoBs, CodigoVerificacion, TipoCambioAplicado, Estado) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", todas_cuentas)
            cursor.executemany("INSERT INTO Auditoria_Conversiones (CuentaId, BancoId, Operacion, SaldoUSD_Original, SaldoUSD_Cifrado, SaldoBS_Convertido, TipoCambioAplicado, CodigoVerificacion, Estado) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)", todas_auditorias)
            
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"❌ Error ASFI Central: {e}")

    mins, segs = divmod(time.time() - tiempo_inicio, 60)
    print("\n=======================================================")
    print("🎉 ¡PROYECTO DISTRIBUIDO MODO BATCH COMPLETADO!")
    print(f"🏦 14 Bancos Procesados en tiempo récord.")
    print(f"⏱️ TIEMPO TOTAL: {int(mins)} minutos y {segs:.2f} segundos")
    print("=======================================================")

if __name__ == "__main__":
    orquestador_supremo_asfi()