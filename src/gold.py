from pyspark.sql.functions import count
from pyspark.sql import functions as F

def distribution_of_crate_types(orders_df):
    """
    Calcula la distribución de los tipos de cajas por compañía (número de pedidos por tipo).
    """
    return orders_df.groupBy("company_name_clean", "company_id", "crate_type") \
    .agg(count("*").alias("order_count")) \
    .orderBy("company_name_clean")

def salesowners_per_company(df):
    """
    Proporciona un DataFrame que contiene las columnas company_id, company_name y una lista ordenada y única de salesowners.
    
    :param df: DataFrame original con los datos de órdenes y vendedores.
    :return: DataFrame con la lista de salesowners única y ordenada para cada empresa.
    """
    # Dividir el campo 'salesowners' en una lista de vendedores
    df = df.withColumn("salesowners_list", F.split(F.col("salesowners"), ", "))

    # Aplanar la lista de vendedores y eliminar duplicados utilizando collect_set
    df = df.withColumn("salesowner", F.explode(F.col("salesowners_list")))

    # Agrupar por company_id y company_name, y colectar la lista única de salesowners
    df_grouped = df.groupBy("company_name_clean") \
               .agg(F.collect_set("salesowner").alias("unique_salesowners"),
                    F.collect_set("company_id").alias("unique_company_ids"))

    # Ordenar la lista de salesowners por nombre
    df_result = df_grouped.withColumn("list_salesowners", F.array_sort(F.col("unique_salesowners"))) \
                          .withColumn("list_salesowners", F.expr("array_join(list_salesowners, ', ')"))

    return df_result.select("company_name_clean", "list_salesowners", "unique_company_ids") 

def full_contact_name(df_orders):
    return df_orders.select("order_id", "contact_full_name")

def full_address(df_orders):
    return df_orders.select("order_id", "contact_address")

def calculate_sales_commissions(df):
    """
    Calcula las comisiones para los vendedores basadas en su posición en el campo 'salesowners'.
    Si hay solo 1 vendedor, se calcula solo la comisión del primer vendedor.
    Si hay 2 vendedores, se calcula la comisión del primer y segundo vendedor.
    Si hay 3 vendedores, se calcula la comisión para todos.
    
    :param df: DataFrame combinado con las órdenes y las facturas.
    :return: DataFrame con las comisiones por vendedor.
    """
    # Convertir el valor neto facturado de centavos a euros
    df = df.withColumn("net_value_euros", F.col("net_invoiced_value") / 100)

    # Dividir el campo 'salesowners' en una lista de vendedores
    df = df.withColumn("salesowners_list", F.split(F.col("salesowners"), ", "))

    # Calcular la comisión del Main Owner (primer vendedor en la lista)
    df_main_owner = df.withColumn("salesowner", F.col("salesowners_list")[0]) \
                      .withColumn("commission", F.round(F.col("net_value_euros") * 0.06, 2)) \
                      .select("order_id", "salesowner", "commission")

    # Calcular la comisión del Co-owner 1 (segundo vendedor en la lista) solo si existe
    df_co_owner1 = df.filter(F.size(F.col("salesowners_list")) >= 2) \
                     .withColumn("salesowner", F.col("salesowners_list")[1]) \
                     .withColumn("commission", F.round(F.col("net_value_euros") * 0.025, 2)) \
                     .select("order_id", "salesowner", "commission")

    # Calcular la comisión del Co-owner 2 (tercer vendedor en la lista) solo si existe
    df_co_owner2 = df.filter(F.size(F.col("salesowners_list")) >= 3) \
                     .withColumn("salesowner", F.col("salesowners_list")[2]) \
                     .withColumn("commission", F.round(F.col("net_value_euros") * 0.0095, 2)) \
                     .select("order_id", "salesowner", "commission")

    # Unir todas las comisiones en un solo DataFrame
    df_commissions = df_main_owner.unionByName(df_co_owner1, allowMissingColumns=True) \
                                  .unionByName(df_co_owner2, allowMissingColumns=True)

    # Agrupar por vendedor y sumar sus comisiones
    df_commissions = df_commissions.groupBy("salesowner") \
                                   .agg(F.round(F.sum("commission"), 2).alias("total_commission"))

    # Ordenar por el total de comisiones en orden descendente
    df_commissions = df_commissions.orderBy(F.col("total_commission").desc())

    return df_commissions
