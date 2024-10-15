from pyspark.sql.types import StructType, StructField, StringType, ArrayType
from pyspark.sql.functions import upper, trim, regexp_replace, col
from pyspark.sql import functions as F

def clean_company_name(df):
    """
    Esta función transforma el campo 'company_name' en:
    - Mayúsculas.
    - Elimina los espacios adicionales.
    - Elimina los caracteres especiales.
    Además, selecciona las columnas importantes.
    """
    # Convertir todos los nombres a mayúsculas
    df = df.withColumn('company_name_clean', upper(col('company_name')))
    
    # Eliminar caracteres especiales
    df = df.withColumn('company_name_clean', regexp_replace(col('company_name_clean'), r'[^\w\s]', ''))

    # Eliminar guiones bajos
    df = df.withColumn('company_name_clean', regexp_replace(col('company_name_clean'), r'[_]', ' '))

    # Eliminar espacios adicionales
    df = df.withColumn('company_name_clean', trim(col('company_name_clean')))
    
    return df

def transform_to_valid_json(df, column_name):
    """
    Transforma un campo de un DataFrame en un JSON válido.
    
    :param df: El DataFrame original.
    :param column_name: El nombre del campo que contiene el JSON no válido.
    :return: Un nuevo DataFrame con el campo transformado en un JSON válido.
    """
    # Paso 1: Eliminar las comillas dobles duplicadas (si existen)
    df = df.withColumn(f"{column_name}_clean", F.regexp_replace(F.col(column_name), '""', '"'))
    
    # Paso 2: Eliminar las comillas exteriores que envuelven todo el JSON
    df = df.withColumn(f"{column_name}_clean", F.expr(f"substring({column_name}_clean, 2, length({column_name}_clean) - 2)"))

    return df

def create_contact_full_name(df):
    """
    Extrae el nombre completo del contacto desde la columna contact_data,
    o usa 'John Doe' si la información no está disponible.
    """
    # Definir el esquema de JSON para analizar la lista en 'contact_data'
    contact_schema = ArrayType(StructType([
        StructField("contact_name", StringType(), True),
        StructField("contact_surname", StringType(), True),
        StructField("city", StringType(), True),
        StructField("cp", StringType(), True)
    ]))
    
    # Analizar el JSON en 'contact_data' si existe
    df = df.withColumn("contact_info", F.from_json(F.col("contact_data_clean"), contact_schema))

    # Extraer el primer elemento de la lista (contact_name y contact_surname)
    df = df.withColumn("contact_name", F.col("contact_info")[0]["contact_name"]) \
           .withColumn("contact_surname", F.col("contact_info")[0]["contact_surname"])

    # Concatenar el nombre y apellido, y usar 'John Doe' si contact_data es null o no tiene nombre y apellido
    df = df.withColumn(
        'contact_full_name',
        F.when(
            F.col('contact_data_clean').isNull(), "John Doe"  # Si contact_data es nulo
        ).otherwise(
            F.when(
                (F.col('contact_name').isNull()) | (F.col('contact_surname').isNull()), "John Doe"
            ).otherwise(
                F.concat_ws(" ", F.col('contact_name'), F.col('contact_surname'))  # Concatenar nombre y apellido
            )
        )
    )

    return df

def create_contact_address(df):
    """
    Crea un DataFrame con las columnas order_id y contact_address.
    El campo contact_address sigue el formato 'city name, postal code'.
    Si la ciudad no está disponible, se usa 'Unknown'.
    Si el código postal no está disponible, se usa 'UNK00'.
    """

    # Definir el esquema de JSON para analizar la lista en 'contact_data'
    contact_schema = ArrayType(StructType([
        StructField("contact_name", StringType(), True),
        StructField("contact_surname", StringType(), True),
        StructField("city", StringType(), True),
        StructField("cp", StringType(), True)
    ]))


    # Analizar el JSON en 'contact_data' para extraer city y cp
    df = df.withColumn("contact_info", F.from_json(F.col("contact_data_clean"), contact_schema))

    # Extraer los campos city y cp del JSON
    df = df.withColumn("city", F.col("contact_info")[0]["city"]) \
           .withColumn("cp", F.col("contact_info")[0]["cp"])

    # Formatear el campo contact_address
    df = df.withColumn(
        "contact_address",
        F.concat_ws(
            ", ",
            F.when(F.col("city").isNull(), "Unknown").otherwise(F.col("city")),
            F.when(F.col("cp").isNull(), "UNK00").otherwise(F.col("cp").cast(StringType()))
        )
    )

    return df

def combine_orders_invoices(df_orders, df_invoices):
    """
    Combina los DataFrames de órdenes y facturas utilizando 'order_id' como clave.
    
    :param df_orders: DataFrame con los datos de las órdenes.
    :param df_invoices: DataFrame con los datos de las facturas.
    :return: DataFrame combinado.
    """
    # Unir los DataFrames en el campo 'order_id'
    df_combined = df_orders.join(df_invoices, "order_id", "inner")

    # Añadir una columna 'net_invoiced_value' a partir de 'gross_value'
    df_combined = df_combined.withColumn("net_invoiced_value", F.col("gross_value"))
    
    return df_combined

def transform_to_silver(orders_df):

    orders_clean_df = clean_company_name(orders_df)

    valid_json_df = transform_to_valid_json(orders_clean_df, "contact_data")

    contact_full_name_df = create_contact_full_name(valid_json_df)

    contact_address_df = create_contact_address(contact_full_name_df)
    
    return contact_address_df
