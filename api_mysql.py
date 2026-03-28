from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import pymysql
from typing import List

app = FastAPI(title="API Gateway - Bancos MySQL")

class ActualizacionCierre(BaseModel):
    cuenta_id: int
    saldo_bs: float
    codigo_ver: str

def get_db_connection():
    return pymysql.connect(host='localhost', port=3307, user='root', password='root_pass', db='bancos_db')

@app.get("/api/banco/{id_banco}/cuentas")
def obtener_cuentas(id_banco: int):
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            cursor.execute("SELECT CuentaId, NroCuenta, IdBanco, Identificacion, Nombres, Apellidos, SaldoCifrado FROM CuentasBancarias WHERE IdBanco = %s", (id_banco,))
            cuentas = [{"cuenta_id": r[0], "nro_cuenta": r[1], "id_banco": r[2], "identificacion": r[3], "nombres": r[4], "apellidos": r[5], "saldo_cifrado": r[6]} for r in cursor.fetchall()]
        conn.close()
        return {"status": "success", "data": cuentas}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.put("/api/banco/{id_banco}/cierre_masivo")
def actualizar_saldos_asfi(id_banco: int, data: List[ActualizacionCierre]):
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            lote = [(d.saldo_bs, d.codigo_ver, d.cuenta_id, id_banco) for d in data]
            cursor.executemany(
                "UPDATE CuentasBancarias SET SaldoBs = %s, CodigoVerificacion = %s WHERE CuentaId = %s AND IdBanco = %s",
                lote
            )
        conn.commit()
        conn.close()
        return {"status": "success"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) 