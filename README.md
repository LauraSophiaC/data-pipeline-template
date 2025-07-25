# MercadoPago – DE Challenge

Este proyecto implementa un pipeline de datos en Python para preparar un dataset final listo para entrenamiento de modelos de Machine Learning. Utiliza una arquitectura de medallas (Bronze → Silver → Gold) y procesa tres fuentes de datos: prints.json, taps.json y pays.csv.

---

## Cómo ejecutar

1. Clonar el repositorio:

   git clone (https://github.com/LauraSophiaC/data-pipeline-template.git)
   cd data-pipeline-mercadopago

2. Instalar dependencias:

   pip install -r requirements.txt

3. Ejecutar el pipeline completo:

   python -m pipeline.main

---

## Capas del pipeline


─ ingestion.py       -> Carga archivos raw y genera bronze
─ preprocessing.py   ->Limpia, desanida, convierte fechas
─ transformation.py  -> Cálculos de agregados + output final
─ main.py            -> Orquesta el flujo completo


- data/raw/      ->  Archivos originales (.json, .csv)
- data/bronze/   ->  Datos crudos serializados en .parquet
- data/silver/   -> Datos limpios, desanidados y estandarizados
- data/gold/    ->  Dataset final: prints recientes + métricas históricas

---

## Output

Archivo generado:  data/gold/dataset_training_20250723.parquet

Contiene columnas como:

- clicked: 1/0 según si hubo tap
- past_prints: cantidad de veces que se mostró la value prop en 3 semanas previas
- past_taps: cantidad de taps en 3 semanas previas
- past_pays_count: número de pagos realizados
- past_pays_total: monto total pagado

---
