from pyspark.sql.types import StructType, StructField, StringType, ArrayType
from pyspark.sql.functions import upper, trim, regexp_replace, col
from pyspark.sql import functions as F

def clean_company_name(df):
    """
        This function transforms the 'company_name' field into:
        - Uppercase.
        - Removes extra spaces.
        - Removes special characters.
        Also, selects important columns.
    """
    # Convert to uppercase
    df = df.withColumn('company_name_clean', upper(col('company_name')))
    
    # Delete special characters
    df = df.withColumn('company_name_clean', regexp_replace(col('company_name_clean'), r'[^\w\s]', ''))

    # Delete underscores
    df = df.withColumn('company_name_clean', regexp_replace(col('company_name_clean'), r'[_]', ' '))

    # Delete leading and trailing spaces
    df = df.withColumn('company_name_clean', trim(col('company_name_clean')))
    
    return df

def transform_to_valid_json(df, column_name):
    """
        Transforms a field in a DataFrame into valid JSON.

        :param df: The original DataFrame.
        :param column_name: The name of the field containing the invalid JSON.
        :return: A new DataFrame with the field transformed into valid JSON.
    """
    # Remove the extra quotes from the JSON string
    df = df.withColumn(f"{column_name}_clean", F.regexp_replace(F.col(column_name), '""', '"'))
    
    # Remove the leading and trailing quotes
    df = df.withColumn(f"{column_name}_clean", F.expr(f"substring({column_name}_clean, 2, length({column_name}_clean) - 2)"))

    return df

def create_contact_full_name(df):
    """
        Extract the contact's full name from the contact_data column,
        or use 'John Doe' if the information is not available.
    """
    # Define the JSON schema to parse the list in 'contact_data'
    contact_schema = ArrayType(StructType([
        StructField("contact_name", StringType(), True),
        StructField("contact_surname", StringType(), True),
        StructField("city", StringType(), True),
        StructField("cp", StringType(), True)
    ]))
    
    # Analyze the JSON in 'contact_data' to extract contact_name and contact_surname
    df = df.withColumn("contact_info", F.from_json(F.col("contact_data_clean"), contact_schema))

    # Extraer el primer elemento de la lista (contact_name y contact_surname)
    df = df.withColumn("contact_name", F.col("contact_info")[0]["contact_name"]) \
           .withColumn("contact_surname", F.col("contact_info")[0]["contact_surname"])

    # Concat the contact_name and contact_surname
    df = df.withColumn(
        'contact_full_name',
        F.when(
            F.col('contact_data_clean').isNull(), "John Doe"  
        ).otherwise(
            F.when(
                (F.col('contact_name').isNull()) | (F.col('contact_surname').isNull()), "John Doe"
            ).otherwise(
                F.concat_ws(" ", F.col('contact_name'), F.col('contact_surname'))  
            )
        )
    )

    return df

def create_contact_address(df):
    """
        Create a DataFrame with columns order_id and contact_address.
        The contact_address field follows the format 'city name, postal code'.
        If the city is not available, 'Unknown' is used.
        If the postal code is not available, 'UNK00' is used.
    """

    # Define the JSON schema to parse the list in 'contact_data'
    contact_schema = ArrayType(StructType([
        StructField("contact_name", StringType(), True),
        StructField("contact_surname", StringType(), True),
        StructField("city", StringType(), True),
        StructField("cp", StringType(), True)
    ]))


    # Analyze the JSON in 'contact_data' to extract city and cp
    df = df.withColumn("contact_info", F.from_json(F.col("contact_data_clean"), contact_schema))

    # Extract the city and cp from the first element of the list
    df = df.withColumn("city", F.col("contact_info")[0]["city"]) \
           .withColumn("cp", F.col("contact_info")[0]["cp"])

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
        Combines the Orders and Invoices DataFrames using 'order_id' as the key.

        :param df_orders: DataFrame with the order data.
        :param df_invoices: DataFrame with the invoice data.
        :return: Combined DataFrame.
    """
    df_combined = df_orders.join(df_invoices, "order_id", "inner")

    df_combined = df_combined.withColumn("net_invoiced_value", F.col("gross_value"))
    
    return df_combined

def transform_to_silver(orders_df):

    orders_clean_df = clean_company_name(orders_df)

    valid_json_df = transform_to_valid_json(orders_clean_df, "contact_data")

    contact_full_name_df = create_contact_full_name(valid_json_df)

    contact_address_df = create_contact_address(contact_full_name_df)
    
    return contact_address_df
