# Aplicación principal

from typing import List, Dict
from icontract import require, ensure
from datetime import datetime

# Paso 1: Simular datos crudos
def extract() -> List[Dict]:
    return [
        {"cliente_id": 123, "monto": 250.5, "fecha": "2024-12-01", "producto": "Laptop"},
        {"cliente_id": -1, "monto": 0, "fecha": "no_fecha", "producto": ""},
        # Agregá más casos con y sin errores
    ]

# Paso 2: Validar y transformar
@require(lambda registro: isinstance(registro["cliente_id"], int) and registro["cliente_id"] > 0,
         "cliente_id inválido")
@require(lambda registro: registro["monto"] > 0, "monto debe ser mayor a 0")
@require(lambda registro: isinstance(registro["producto"], str) and len(registro["producto"]) > 0,
         "producto no puede ser vacío")
@require(lambda registro: _fecha_valida(registro["fecha"]), "fecha inválida")
@ensure(lambda result: isinstance(result, dict), "Debe devolver un diccionario")
def transformar(registro: Dict) -> Dict:
    registro["fecha"] = datetime.strptime(registro["fecha"], "%Y-%m-%d")
    return registro

def _fecha_valida(fecha_str: str) -> bool:
    try:
        datetime.strptime(fecha_str, "%Y-%m-%d")
        return True
    except ValueError:
        return False

# Paso 3: Cargar (simulado)
def load(data_limpia: List[Dict]):
    for registro in data_limpia:
        print(registro)

# Orquestador ETL
def ejecutar_etl():
    crudos = extract()
    procesados = []
    for reg in crudos:
        try:
            limpio = transformar(reg)
            procesados.append(limpio)
        except Exception as e:
            print(f"Registro descartado: {reg} - Error: {e}")
    load(procesados)

if __name__ == "__main__":
    ejecutar_etl()
