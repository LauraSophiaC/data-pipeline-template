from pipeline.ingestion import run_ingestion
from pipeline.preprocessing import run_preprocessing
from pipeline.transformation import run_transformation
from utils.logger import get_logger

logger = get_logger("main")

if __name__ == "__main__":
    logger.info("------Pipeline MercadoPago iniciado--------")
    try:
        logger.info(">>> INGESTA")
        run_ingestion()
        logger.info(">>> PREPROCESAMIENTO")
        run_preprocessing()
        logger.info(">>> TRANSFORMACIÓN")
        run_transformation()
        logger.info("------Pipeline MercadoPago finalizado--------")
    except Exception as e:
        logger.error(f"Error en la ejecución del pipeline: {e}")        


