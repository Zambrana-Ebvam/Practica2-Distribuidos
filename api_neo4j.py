from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from neo4j import GraphDatabase
from typing import List

app = FastAPI(title="API Gateway - Banco Neo4j")

class ActualizacionCierre(BaseModel):
    cuenta_id: int
    saldo_bs: float
    codigo_ver: str

def get_db_driver():
    return GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "password123"))

@app.get("/api/banco/{id_banco}/cuentas")
def obtener_cuentas(id_banco: int):
    try:
        driver = get_db_driver()
        query = "MATCH (c:Cliente)-[:TIENE_CUENTA]->(cta:Cuenta {bancoId: $id_banco}) RETURN cta.cuentaId, cta.nroCuenta, cta.bancoId, c.identificacion, c.nombres, c.apellidos, cta.saldoCifrado"
        cuentas = []
        with driver.session() as session:
            result = session.run(query, id_banco=id_banco)
            for record in result:
                cuentas.append({
                    "cuenta_id": record[0], "nro_cuenta": record[1], "id_banco": record[2],
                    "identificacion": record[3], "nombres": record[4], "apellidos": record[5], "saldo_cifrado": record[6]
                })
        driver.close()
        return {"status": "success", "data": cuentas}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.put("/api/banco/{id_banco}/cierre_masivo")
def actualizar_saldos_asfi(id_banco: int, data: List[ActualizacionCierre]):
    try:
        driver = get_db_driver()
        lote = [{"cuenta_id": d.cuenta_id, "saldo_bs": d.saldo_bs, "codigo_ver": d.codigo_ver} for d in data]
        query = """
        UNWIND $lote AS row
        MATCH (cta:Cuenta {cuentaId: row.cuenta_id, bancoId: $id_banco})
        SET cta.saldoBs = row.saldo_bs, cta.codigoVerificacion = row.codigo_ver
        """
        with driver.session() as session:
            session.run(query, id_banco=id_banco, lote=lote)
        driver.close()
        return {"status": "success"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))