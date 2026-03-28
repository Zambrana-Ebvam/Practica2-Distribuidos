from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import pyodbc
from typing import List

app = FastAPI(title="API Gateway - Bancos SQL Server (1, 2, 3)")

class ActualizacionCierre(BaseModel):
    cuenta_id: int
    saldo_bs: float
    codigo_ver: str

def get_db_connection():
    return pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=localhost,1433;DATABASE=master;UID=sa;PWD=SuperStrongPass123!')

@app.get("/api/banco/{id_banco}/cuentas")
def obtener_cuentas(id_banco: int):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT CuentaId, NroCuenta, IdBanco, Identificacion, Nombres, Apellidos, SaldoCifrado FROM CuentasBancarias WHERE IdBanco = ?", (id_banco,))
        
        cuentas = []
        for row in cursor.fetchall():
            cuentas.append({
                "cuenta_id": row[0], "nro_cuenta": row[1], "id_banco": row[2],
                "identificacion": row[3], "nombres": row[4], "apellidos": row[5], "saldo_cifrado": row[6]
            })
        conn.close()
        return {"status": "success", "data": cuentas}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.put("/api/banco/{id_banco}/cierre_masivo")
def actualizar_saldos_asfi(id_banco: int, data: List[ActualizacionCierre]):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        lote = [(d.saldo_bs, d.codigo_ver, d.cuenta_id, id_banco) for d in data]
        cursor.executemany(
            "UPDATE CuentasBancarias SET SaldoBs = ?, CodigoVerificacion = ? WHERE CuentaId = ? AND IdBanco = ?",
            lote
        )
        conn.commit()
        conn.close()
        return {"status": "success"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))