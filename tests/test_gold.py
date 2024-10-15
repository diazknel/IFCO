import pytest
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions as F
from src.gold import (
    distribution_of_crate_types,
    salesowners_per_company,
    full_contact_name,
    full_address,
    calculate_sales_commissions
)

@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.appName("TestGoldLayer").getOrCreate()

# Test para distribution_of_crate_types
def test_distribution_of_crate_types(spark):
    data = [
        ("CompanyA", "1", "TypeA"),
        ("CompanyA", "1", "TypeB"),
        ("CompanyB", "2", "TypeA")
    ]
    
    df = spark.createDataFrame(data, ["company_name_clean", "company_id", "crate_type"])
    
    result_df = distribution_of_crate_types(df)
    
    result = result_df.collect()
    
    assert result[0]["company_name_clean"] == "CompanyA", "Error in distribution of CompanyA"
    assert result[0]["order_count"] == 1, "Error in counting TypeA of CompanyA"
    assert result[1]["crate_type"] == "TypeB", "Error in CompanyA cash type"
    assert result[2]["company_name_clean"] == "CompanyB", "Error in CompanyB distribution∫"

# Test para salesowners_per_company
def test_salesowners_per_company(spark):
    data = [
        ("CompanyA", "1", "John, Alice"),
        ("CompanyB", "2", "Alice, Bob"),
        ("CompanyA", "1", "Alice, Bob")
    ]
    
    df = spark.createDataFrame(data, ["company_name_clean", "company_id", "salesowners"])
    
    result_df = salesowners_per_company(df)
    
    result = result_df.collect()
    
    assert result[0]["company_name_clean"] == "CompanyA", "Error en la agrupación de CompanyA"
    assert result[0]["list_salesowners"] == "Alice, Bob, John", "Error en la lista de vendedores para CompanyA"
    assert result[1]["company_name_clean"] == "CompanyB", "Error en la agrupación de CompanyB"
    assert result[1]["list_salesowners"] == "Alice, Bob", "Error en la lista de vendedores para CompanyB"

# Test para full_contact_name
def test_full_contact_name(spark):
    data = [
        ("1", "John Doe"),
        ("2", "Jane Smith")
    ]
    
    df = spark.createDataFrame(data, ["order_id", "contact_full_name"])
    
    result_df = full_contact_name(df)
    
    result = result_df.collect()
    
    assert result[0]["contact_full_name"] == "John Doe", "Error en el nombre completo del contacto"
    assert result[1]["contact_full_name"] == "Jane Smith", "Error en el nombre completo del contacto"

# Test para full_address
def test_full_address(spark):
    data = [
        ("1", "New York, 10001"),
        ("2", "San Francisco, 94107")
    ]
    
    df = spark.createDataFrame(data, ["order_id", "contact_address"])
    
    result_df = full_address(df)
    
    result = result_df.collect()
    
    assert result[0]["contact_address"] == "New York, 10001", "Error en la dirección del contacto para order_id 1"
    assert result[1]["contact_address"] == "San Francisco, 94107", "Error en la dirección del contacto para order_id 2"

# Test para calculate_sales_commissions
def test_calculate_sales_commissions(spark):
    data = [
        ("1", "John, Alice", 10000),  # 100 euros
        ("2", "Alice, Bob", 20000),   # 200 euros
        ("3", "Charlie", 50000)       # 500 euros
    ]
    
    df = spark.createDataFrame(data, ["order_id", "salesowners", "net_invoiced_value"])
    
    result_df = calculate_sales_commissions(df)
    
    result = result_df.collect()
    
    # Verificar las comisiones correctas
    john_commission = [row["total_commission"] for row in result if row["salesowner"] == "John"][0]
    alice_commission = [row["total_commission"] for row in result if row["salesowner"] == "Alice"][0]
    bob_commission = [row["total_commission"] for row in result if row["salesowner"] == "Bob"][0]
    charlie_commission = [row["total_commission"] for row in result if row["salesowner"] == "Charlie"][0]
    
    assert john_commission == 6.0, "Error en la comisión de John"
    assert alice_commission == 14.5, "Error en la comisión de Alice"
    assert bob_commission == 5.0, "Error en la comisión de Bob"
    assert charlie_commission == 30.0, "Error en la comisión de Charlie"
