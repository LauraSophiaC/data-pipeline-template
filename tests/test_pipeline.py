import glob
import os
import pandas as pd
from pipeline.preprocessing import preprocess_prints
from pipeline.transformation import run_transformation

def test_preprocess_prints_columns():
    df = preprocess_prints()
    expected_columns = {"user_id", "value_prop", "timestamp", "position"}
    assert expected_columns.issubset(set(df.columns)), "Faltan columnas esperadas en prints"

def get_latest_gold_file():
    files = glob.glob("data/medallion/gold/*.parquet")
    assert files, "No se encontró ningún archivo .parquet en gold/"
    return max(files, key=os.path.getctime)

def test_transformation_output():
    run_transformation()
    files = glob.glob("data/gold/*.parquet")
    assert len(files) > 0, "El dataset generado está vacío"

