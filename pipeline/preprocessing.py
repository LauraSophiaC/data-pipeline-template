import pandas as pd
import os
from datetime import datetime
from utils.logger import get_logger
from utils.logger import get_logger

logger = get_logger("preprocessing")


def clean_columns_names(df):
    df.columns = df.columns.str.lower().str.strip().str.replace(' ', '_')
    return df

def parse_dates(df, column_name):
    df[column_name] = pd.to_datetime(df[column_name], errors='coerce')
    return df

def preprocess_prints():
    df = pd.read_parquet("data/medallion/bronze/prints.parquet")

    # Normalizar columnas anidadas (event_data)
    df = pd.concat(
        [df.drop("event_data", axis=1), df["event_data"].apply(pd.Series)], axis=1
    )

    df = clean_columns_names(df)

    # Convertir 'day' a timestamp y renombrar
    df["timestamp"] = pd.to_datetime(df["day"], errors="coerce")
    df.drop(columns=["day"], inplace=True)

    # Validar columnas clave
    df = df.dropna(subset=["user_id", "value_prop", "timestamp"])
    return df


def preprocess_taps():
    df = pd.read_parquet("data/medallion/bronze/taps.parquet")

    df = pd.concat(
        [df.drop("event_data", axis=1), df["event_data"].apply(pd.Series)], axis=1
    )

    df = clean_columns_names(df)
    df["timestamp"] = pd.to_datetime(df["day"], errors="coerce")
    df.drop(columns=["day"], inplace=True)

    df = df.dropna(subset=["user_id", "value_prop", "timestamp"])
    return df


def preprocess_pays():
    df = pd.read_parquet("data/medallion/bronze/pays.parquet")

    df = clean_columns_names(df)
    df.rename(columns={"pay_date": "timestamp", "total": "amount"}, inplace=True)

    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce").fillna(0)

    df = df.dropna(subset=["user_id", "value_prop", "timestamp"])
    return df


def save_to_silver(df: pd.DataFrame, name: str):
    os.makedirs("data/medallion/silver", exist_ok=True)
    df.to_parquet(f"data/medallion/silver/{name}_clean.parquet", index=False)
    logger.info(f"{name}_clean.parquet guardado en data/medallion/silver/")


def run_preprocessing():
    logger.info(" ----- INICIANDO PREPROCESAMIENTO HACIA CAPA SILVER ----- ")
    
    prints_df = preprocess_prints()
    taps_df = preprocess_taps()
    pays_df = preprocess_pays()

    save_to_silver(prints_df, "prints")
    save_to_silver(taps_df, "taps")
    save_to_silver(pays_df, "pays")

    logger.info(" ----- PREPROCESAMIENTO FINALIZADO ----- ")    