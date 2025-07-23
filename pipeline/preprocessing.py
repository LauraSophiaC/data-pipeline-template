def clean_data(df):
    df.columns = df.columns.str.lower().str.strip()
    df = df.dropna(subset=["edad"])  # Elimina filas sin edad
    df["edad"] = df["edad"].astype(int)
    return df
