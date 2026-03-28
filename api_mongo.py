from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from pymongo import MongoClient, UpdateOne
from typing import List

app = FastAPI(title="API Gateway - Bancos MongoDB")

class ActualizacionCierre(BaseModel):
    cuenta_id: int
    saldo_bs: float
    codigo_ver: str

def get_db():
    cliente = MongoClient("mongodb://localhost:27017/")
    return cliente["bancos_db"]

@app.get("/api/banco/{id_banco}/cuentas")
def obtener_cuentas(id_banco: int):
    try:
        db = get_db()
        cuentas = []
        for doc in db["CuentasBancarias"].find({"IdBanco": id_banco}):
            cuentas.append({
                "cuenta_id": doc["CuentaId"], "nro_cuenta": doc["NroCuenta"], "id_banco": doc["IdBanco"],
                "identificacion": doc["Identificacion"], "nombres": doc["Nombres"], "apellidos": doc["Apellidos"], "saldo_cifrado": doc["SaldoCifrado"]
            })
        return {"status": "success", "data": cuentas}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.put("/api/banco/{id_banco}/cierre_masivo")
def actualizar_saldos_asfi(id_banco: int, data: List[ActualizacionCierre]):
    try:
        db = get_db()
        operaciones = [
            UpdateOne({"CuentaId": d.cuenta_id, "IdBanco": id_banco}, 
                      {"$set": {"SaldoBs": d.saldo_bs, "CodigoVerificacion": d.codigo_ver}}) 
            for d in data
        ]
        db["CuentasBancarias"].bulk_write(operaciones)
        return {"status": "success"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))