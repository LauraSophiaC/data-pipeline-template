import pandas as pd
from datetime import datetime, timedelta
from utils.logger import get_logger
logger = get_logger(__name__)


def run_transformation():
    #Cargar datos silver
    prints_df = pd.read_parquet("data/medallion/silver/prints_clean.parquet")
    taps_df = pd.read_parquet("data/medallion/silver/taps_clean.parquet")
    pays_df = pd.read_parquet("data/medallion/silver/pays_clean.parquet")

    print('----- INICIANDO TRANSFORMACION HACIA CAPA GOLD ----- ')

    #Definir ventanas temporales
    max_date = prints_df['timestamp'].max()
    one_week_ago =max_date - timedelta(days=7)
    four_weeks_ago = max_date - timedelta(days=28)
    three_weeks_ago = max_date - timedelta(days=21)
    week_3_start = four_weeks_ago
    week_3_end = max_date - timedelta(days=8)

    # 1. FILTRAR PRINTS DE LA ULTIMA SEMANA
    recent_prints = prints_df[prints_df['timestamp'] >= one_week_ago].copy()

    # 2. MARCAR SI HIZO CLICK
    taps_key = taps_df[['user_id','value_prop' ,'timestamp']].copy()
    taps_key['clicked'] = 1

    # joinear para detectar clicks s
    recent_prints = recent_prints.merge(taps_key, on=['user_id', 'value_prop', 'timestamp'], how='left')
    recent_prints['clicked'] = recent_prints['clicked'].fillna(0).astype(int)

    # 3. METRICAS HISTORICAS 3 SEMANAS ANTES DEL PRINT
    # filtros para cada archivo
    past_prints = prints_df[(prints_df['timestamp'] >= week_3_start) & (prints_df['timestamp'] < week_3_end)]
    past_taps = taps_df[(taps_df['timestamp'] >= week_3_start) & (taps_df['timestamp'] < week_3_end)]
    past_pays = pays_df[(pays_df['timestamp'] >= week_3_start) & (pays_df['timestamp'] < week_3_end)]

    # 4. AGREGACIONES POR USUARIO Y VALUE_PROP
    agg_prints = past_prints.groupby(['user_id', 'value_prop']).size().reset_index(name='past_prints')
    agg_taps = past_taps.groupby(['user_id', 'value_prop']).size().reset_index(name='past_taps')
    agg_pays = past_pays.groupby(['user_id', 'value_prop']).agg(
        past_pays_count=pd.NamedAgg(column='timestamp', aggfunc='count'),
        past_pays_total=pd.NamedAgg(column='amount', aggfunc='sum')
    ).reset_index()

    # 5 UNIR TODO A RECENT_PRINTS
    df = recent_prints.merge(agg_prints, on=['user_id', 'value_prop'], how='left')
    df = df.merge(agg_taps, on=['user_id', 'value_prop'], how='left')
    df = df.merge(agg_pays, on=['user_id', 'value_prop'], how='left')   

    #6 RELLENAR NULOS
    df['past_prints'] =df['past_prints'].fillna(0).astype(int)
    df['past_taps'] = df['past_taps'].fillna(0).astype(int)
    df['past_pays_count'] = df['past_pays_count'].fillna(0).astype(int)
    df['past_pays_total'] = df['past_pays_total'].fillna(0)

    # 4. GUARDAR EN GOLG

    date = datetime.now().strftime('%Y-%m-%d')
    df.to_parquet("data/medallion/gold/dataset_training_{date}.parquet", index=False)
    print("Dataset guardado en data/medallion/gold/dataset_training.parquet")
    print(" ----- TRANSFORMACION FINALIZADA ----- ")
    
