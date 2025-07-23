import pandas as pd
from pipeline.preprocessing import clean_data


def test_clean_data():
    df = pd.DataFrame({"nombre_columna_clave": ["valor", None], "otra": [1, 2]})
    result = clean_data(df)
    assert result.shape[0] == 1


# CORRER LOS TEST CON pytest tests/
