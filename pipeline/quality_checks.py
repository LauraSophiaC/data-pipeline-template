
import pandera as pa
from pandera import Column, DataFrameSchema, Check

def validate_schema(df):
    schema = DataFrameSchema({
        "nombre": Column(str, Check.str_length(min_value=1)),
        "edad": Column(int, Check.ge(0)),
        "ciudad": Column(str, Check.str_length(min_value=1)),
    })
    return schema.validate(df)

