import pandas as pd
from datetime import datetime
import os
from utils.logger import get_logger
logger = get_logger(__name__)




def load_prints (path="data/raw/prints.json"):
    df = pd.read_json(path, lines=True)
    df["ingestion_date"]=datetime.now()
    return df
def load_taps (path="data/raw/taps.json"):
    df = pd.read_json(path, lines=True)
    df["ingestion_date"]=datetime.now()
    return df
def load_pays (path="data/raw/pays.csv"):
    df = pd.read_csv(path)
    df["ingestion_date"]=datetime.now()
    return df
def save_to_bronze (df: pd.DataFrame, name:str):
    os.makedirs("data/medallion/bronze", exist_ok=True)
    df.to_parquet(f"data/medallion/bronze/{name}.parquet", index=False)
    print(f"{name}.parquet guardato en data/medallion/bronze")

def run_ingestion():
    print(" ----- INICIANDO INGESTA HACIA CAPA BRONZE ----- ")
    prints_df = load_prints()
    taps_df = load_taps()
    pays_df = load_pays()

    save_to_bronze(prints_df, "prints")
    save_to_bronze(taps_df, "taps")
    save_to_bronze(pays_df, "pays")

    print(" ----- INGESTA FINALIZADA ----- ")