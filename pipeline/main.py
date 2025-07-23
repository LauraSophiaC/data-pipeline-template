from pipeline.ingestion import load_data
from pipeline.preprocessing import clean_data
from pipeline.quality_checks import validate_schema
from utils.logger import get_logger
import os

def main():
    logger = get_logger()
    logger.info("Inicio del pipeline")

    try:
        df = load_data("data/raw/personas.csv")
        logger.info(f"Datos cargados correctamente: {df.shape[0]} filas")
    except Exception as e:
        logger.error(f"Error al cargar los datos: {e}")
        return

    try:
        df = clean_data(df)
        logger.info(f"Datos limpiados: {df.shape[0]} filas")
    except Exception as e:
        logger.error(f"Error en limpieza: {e}")
        return

    try:
        df = validate_schema(df)
        logger.info("Datos validados correctamente")
    except Exception as e:
        logger.error(f"Error en validación: {e}")
        return

    os.makedirs("data/processed", exist_ok=True)
    df.to_csv("data/processed/personas_limpias.csv", index=False)
    logger.info("Datos guardados exitosamente en data/processed")

if __name__ == "__main__":
    main()

