from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def load_bronze_orders(spark):
    
    # Cargar los datos crudos
    orders_df = spark.read.csv('data/orders.csv', header=True, inferSchema=True, sep=';')
    
    return orders_df

def load_bronze_invoincing(spark):

    df_invoices = spark.read.option("multiline", "true").json('data/invoicing_data.json')

    # Aplanar la estructura del JSON utilizando explode∫
    df_invoices_flat = df_invoices.select(F.explode("data.invoices").alias("invoice"))

    # Seleccionar las columnas relevantes
    df_invoices_flat = df_invoices_flat.select(
        F.col("invoice.id").alias("invoice_id"),
        F.col("invoice.orderId").alias("order_id"),
        F.col("invoice.companyId").alias("company_id"),
        F.col("invoice.grossValue").alias("gross_value"),
        F.col("invoice.vat").alias("vat")
    )
    
    # Convertir 'gross_value' de string a entero
    df_invoices_flat = df_invoices_flat.withColumn("gross_value", F.col("gross_value").cast("int"))
    
    return df_invoices_flat    

