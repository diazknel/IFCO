from pyspark.sql import DataFrame

def validate_data(df: DataFrame, expected_columns: list):
    actual_columns = df.columns
    missing_columns = [col for col in expected_columns if col not in actual_columns]
    assert len(missing_columns) == 0, f"Faltan columnas: {missing_columns}"
