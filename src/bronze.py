from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def load_bronze_orders(spark):
    
    # Load raw data from CSV
    orders_df = spark.read.csv('data/orders.csv', header=True, inferSchema=True, sep=';')
    
    return orders_df

def load_bronze_invoincing(spark):

    df_invoices = spark.read.option("multiline", "true").json('data/invoicing_data.json')

    # Flatten the nested structure
    df_invoices_flat = df_invoices.select(F.explode("data.invoices").alias("invoice"))

    #  Select the columns of interest
    df_invoices_flat = df_invoices_flat.select(
        F.col("invoice.id").alias("invoice_id"),
        F.col("invoice.orderId").alias("order_id"),
        F.col("invoice.companyId").alias("company_id"),
        F.col("invoice.grossValue").alias("gross_value"),
        F.col("invoice.vat").alias("vat")
    )
    
    # Convert gross_value to integer
    df_invoices_flat = df_invoices_flat.withColumn("gross_value", F.col("gross_value").cast("int"))
    
    return df_invoices_flat    

