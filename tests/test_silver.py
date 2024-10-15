import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, ArrayType
from src.silver import (
    clean_company_name,
    transform_to_valid_json,
    create_contact_full_name,
    create_contact_address,
    combine_orders_invoices
)

@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.appName("TestSilverLayer").getOrCreate()

# Test for clean_company_name
def test_clean_company_name(spark):
    data = [
        ("1", "Company @ Inc."),
        ("2", "Example_Company")
    ]
    
    df = spark.createDataFrame(data, ["order_id", "company_name"])
    
    result_df = clean_company_name(df)
    
    result = result_df.select("company_name_clean").collect()
    
    assert result[0]["company_name_clean"] == "COMPANY  INC", "Error cleaning company_name"
    assert result[1]["company_name_clean"] == "EXAMPLE COMPANY", "Error cleaning underscores in company_name"

# Test for transform_to_valid_json
def test_transform_to_valid_json(spark):
    data = [
        (1, '""{"contact_name": "John", "contact_surname": "Doe"}""'),
        (2, '""{"contact_name": "Jane", "contact_surname": "Smith"}""'),
    ]
    schema = StructType([
        StructField("order_id", IntegerType(), True),
        StructField("contact_data", StringType(), True)
    ])

    df = spark.createDataFrame(data, schema)

    df_transformed = transform_to_valid_json(df, "contact_data")

    expected_data = [
        (1, '""{"contact_name": "John", "contact_surname": "Doe"}""', '{"contact_name": "John", "contact_surname": "Doe"}'),
        (2, '""{"contact_name": "Jane", "contact_surname": "Smith"}""', '{"contact_name": "Jane", "contact_surname": "Smith"}'),
    ]
    expected_df = spark.createDataFrame(expected_data, ["order_id", "contact_data", "contact_data_clean"])

    assert df_transformed.collect() == expected_df.collect(), "Error transforming JSON strings"

# Test for create_contact_full_name
def test_create_contact_full_name(spark):
    data = [
        (1, '[{"contact_name": "Abel", "contact_surname": "Sanchez", "city":"Valencia", "cp": "12345"}]'),
        (2, '[{"contact_name": "Jane", "contact_surname": "Smith", "city":"Barcelona", "cp": "08024"}]'),
        (3, None),
        (4, '[{"contact_name": "Casandra", "contact_surname": "Clarke", "city":"Jersey", "cp": "1678"}]'),
        (5, '[{"contact_name": "Brandon", "contact_surname": "Sanderson", "city":"Utah", "cp": "0928"}]')
    ]
    schema = StructType([
        StructField("order_id", IntegerType(), True),
        StructField("contact_data_clean", StringType(), True)
    ])

    df = spark.createDataFrame(data, schema)

    df_result = create_contact_full_name(df)
    df_full_name = df_result.select("order_id", "contact_full_name")


    expected_data = [
        (1, "Abel Sanchez"),
        (2, "Jane Smith"),
        (3, "John Doe"),
        (4, "Casandra Clarke"),
        (5, "Brandon Sanderson")
    ]
    expected_schema = StructType([
        StructField("order_id", IntegerType(), True),
        StructField("contact_full_name", StringType(), True)
    ])
    expected_df = spark.createDataFrame(expected_data, expected_schema)

    assert df_full_name.collect() == expected_df.collect(), "Error creating contact_full_name"

# Test for create_contact_address
def test_create_contact_address(spark):
    data = [
        ("1", '{"city": "New York", "cp": "10001"}'),
        ("2", '{"city": null, "cp": null}')
    ]
    
    df = spark.createDataFrame(data, ["order_id", "contact_data_clean"])
    
    result_df = create_contact_address(df)
    
    result = result_df.select("contact_address").collect()
    
    assert result[0]["contact_address"] == "New York, 10001", "Error creating contact_address"
    assert result[1]["contact_address"] == "Unknown, UNK00", "Error assigning default values ​​for city and zip"

# Test for combine_orders_invoices
def test_combine_orders_invoices(spark):
    orders_data = [
        ("1", "CompanyA", 10),
        ("2", "CompanyB", 2)
    ]
    
    invoices_data = [
        ("1", 1200),
        ("2", 2300)
    ]
    
    df_orders = spark.createDataFrame(orders_data, ["order_id", "company_name", "vat"])
    df_invoices = spark.createDataFrame(invoices_data, ["order_id", "gross_value"])

    df_invoices.show()
    df_orders.show()
    
    result_df = combine_orders_invoices(df_orders, df_invoices)
    
    result = result_df.select("order_id", "net_invoiced_value").collect()
    
    assert result[0]["net_invoiced_value"] == 1200, "Error in combining orders and invoices"
    assert result[1]["net_invoiced_value"] == 2300, "Error in combining orders and invoices"
