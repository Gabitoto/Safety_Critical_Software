# Aplicación principal

from modules.funciones import ETLPipeline

if __name__ == "__main__":
    pipeline = ETLPipeline("data\data_cruda.csv", "data\datos_limpios.csv")
    pipeline.extract()
    pipeline.transformar_todo()
    pipeline.load()
    print("✅ ETL terminado. Datos limpios exportados a 'datos_limpios.csv'.")