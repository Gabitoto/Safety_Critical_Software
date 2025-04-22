import csv
from typing import List, Dict
from datetime import datetime
from icontract import require, ensure

class ETLPipeline:
    def __init__(self, ruta_csv_entrada: str, ruta_csv_salida: str):
        self.ruta_csv_entrada = ruta_csv_entrada
        self.ruta_csv_salida = ruta_csv_salida
        self.registros_crudos: List[Dict] = []
        self.registros_limpios: List[Dict] = []

    def extract(self):
        """Lee datos crudos del archivo CSV."""
        with open(self.ruta_csv_entrada, newline='', encoding='utf-8') as archivo:
            lector = csv.DictReader(archivo)
            for fila in lector:
                try:
                    self.registros_crudos.append({
                        "cliente_id": int(fila["cliente_id"]),
                        "monto": float(fila["monto"]),
                        "fecha": fila["fecha"],
                        "producto": fila["producto"]
                    })
                except Exception as e:
                    print(f"[EXTRACT ERROR] Fila malformada: {fila} - {e}")

    def transformar_todo(self):
        """Valida y transforma todos los registros."""
        for registro in self.registros_crudos:
            try:
                limpio = self.transformar(registro)
                self.registros_limpios.append(limpio)
            except Exception as e:
                print(f"[TRANSFORM ERROR] Registro descartado: {registro} - {e}")

    def es_entero_y_positivo(registro):
        return isinstance(registro["cliente_id"], int) and registro["cliente_id"] > 0

    @require(lambda registro: ETLPipeline.es_entero_y_positivo(registro),
             "cliente_id inválido")
    @require(lambda registro: registro["monto"] > 0, "monto debe ser mayor a 0")
    @require(lambda registro: isinstance(registro["producto"], str) and len(registro["producto"]) > 0,
             "producto no puede ser vacío")
    @require(lambda registro: ETLPipeline._fecha_valida(registro["fecha"]), "fecha inválida")
    @ensure(lambda result: isinstance(result, dict), "Debe devolver un diccionario")
    def transformar(self, registro: Dict) -> Dict:
        registro["fecha"] = datetime.strptime(registro["fecha"], "%Y-%m-%d")
        return registro

    def load(self):
        """Escribe los datos limpios a un nuevo archivo CSV."""
        with open(self.ruta_csv_salida, mode='w', newline='', encoding='utf-8') as archivo:
            campos = ["cliente_id", "monto", "fecha", "producto"]
            writer = csv.DictWriter(archivo, fieldnames=campos)
            writer.writeheader()
            for registro in self.registros_limpios:
                writer.writerow({
                    "cliente_id": registro["cliente_id"],
                    "monto": registro["monto"],
                    "fecha": registro["fecha"].strftime("%Y-%m-%d"),
                    "producto": registro["producto"]
                })

    @staticmethod
    def _fecha_valida(fecha_str: str) -> bool:
        try:
            datetime.strptime(fecha_str, "%Y-%m-%d")
            return True
        except ValueError:
            return False
