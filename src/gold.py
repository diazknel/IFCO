from pyspark.sql.functions import count
from pyspark.sql import functions as F

def distribution_of_crate_types(orders_df):
    """
    Calculate the distribution of box types by company (number of orders by type).
    """
    return orders_df.groupBy("company_name_clean", "company_id", "crate_type") \
    .agg(count("*").alias("order_count")) \
    .orderBy("company_name_clean")

def salesowners_per_company(df):
    """
        Provides a DataFrame containing company_id, company_name columns and a unique, ordered list of salesowners.

        :param df: Original DataFrame with the order and salesperson data.
        :return: DataFrame with the unique, ordered list of salesowners for each company.
    """
    # Split the salesowners string into a list
    df = df.withColumn("salesowners_list", F.split(F.col("salesowners"), ", "))

    # Flatten the list of salesowners
    df = df.withColumn("salesowner", F.explode(F.col("salesowners_list")))

    # Group by company_name_clean and collect unique salesowners and company_ids
    df_grouped = df.groupBy("company_name_clean") \
               .agg(F.collect_set("salesowner").alias("unique_salesowners"),
                    F.collect_set("company_id").alias("unique_company_ids"))

    # Order the list of salesowners and convert it to a string
    df_result = df_grouped.withColumn("list_salesowners", F.array_sort(F.col("unique_salesowners"))) \
                          .withColumn("list_salesowners", F.expr("array_join(list_salesowners, ', ')"))

    return df_result.select("company_name_clean", "list_salesowners", "unique_company_ids") 

def full_contact_name(df_orders):
    return df_orders.select("order_id", "contact_full_name")

def full_address(df_orders):
    return df_orders.select("order_id", "contact_address")

def calculate_sales_commissions(df):
    """
        Calculates commissions for salespeople based on their position in the 'salesowners' field.
        If there is only 1 salesperson, only the commission for the first salesperson is calculated.
        If there are 2 salespeople, the commission for the first and second salespeople is calculated.
        If there are 3 salespeople, the commission for all is calculated.

        :param df: DataFrame combined with orders and invoices.
        :return: DataFrame with commissions per salesperson.
    """
    # Convert into Euros
    df = df.withColumn("net_value_euros", F.col("net_invoiced_value") / 100)

    # Split the salesowners string into a list
    df = df.withColumn("salesowners_list", F.split(F.col("salesowners"), ", "))

    # Calculate the commission for the main owner (first seller in the list)
    df_main_owner = df.withColumn("salesowner", F.col("salesowners_list")[0]) \
                      .withColumn("commission", F.round(F.col("net_value_euros") * 0.06, 2)) \
                      .select("order_id", "salesowner", "commission")

    # Calculate the commission for Co-owner 1 (second seller in the list) if it exists
    df_co_owner1 = df.filter(F.size(F.col("salesowners_list")) >= 2) \
                     .withColumn("salesowner", F.col("salesowners_list")[1]) \
                     .withColumn("commission", F.round(F.col("net_value_euros") * 0.025, 2)) \
                     .select("order_id", "salesowner", "commission")

    # Calculate the commission for Co-owner 2 (third seller in the list) if it exists
    df_co_owner2 = df.filter(F.size(F.col("salesowners_list")) >= 3) \
                     .withColumn("salesowner", F.col("salesowners_list")[2]) \
                     .withColumn("commission", F.round(F.col("net_value_euros") * 0.0095, 2)) \
                     .select("order_id", "salesowner", "commission")

    df_commissions = df_main_owner.unionByName(df_co_owner1, allowMissingColumns=True) \
                                  .unionByName(df_co_owner2, allowMissingColumns=True)

    df_commissions = df_commissions.groupBy("salesowner") \
                                   .agg(F.round(F.sum("commission"), 2).alias("total_commission"))

    df_commissions = df_commissions.orderBy(F.col("total_commission").desc())

    return df_commissions
