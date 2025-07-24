from pipeline.ingestion import run_ingestion
from pipeline.preprocessing import run_preprocessing
from pipeline.transformation import run_transformation



if __name__ == "__main__":
    print(">>> INGESTA")
    run_ingestion()

    print(">>> PREPROCESAMIENTO")
    run_preprocessing()

    print(">>> TRANSFORMACIÓN")
    run_transformation()


