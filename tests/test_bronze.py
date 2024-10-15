import pytest
from pyspark.sql import SparkSession
from src.bronze import load_bronze_orders, load_bronze_invoincing

@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.appName("Test").getOrCreate()

def test_load_bronze_data(spark):
    # Cargar los datos de la capa Bronce
    orders_df = load_bronze_orders(spark)
    invoicing_df = load_bronze_invoincing(spark)

    # Verificar que los DataFrames no estén vacíos
    assert orders_df.count() > 0, "El DataFrame de órdenes está vacío"
    assert invoicing_df.count() > 0, "El DataFrame de facturación está vacío"
    
    # Verificar que las columnas esperadas estén presentes
    expected_columns_orders = ['order_id','date','company_id','company_name','crate_type','contact_data','salesowners']
    for col in expected_columns_orders:
        assert col in orders_df.columns, f"Falta la columna {col} en el DataFrame de órdenes"
