from pyspark.sql import SparkSession
from src.bronze import load_bronze_invoincing, load_bronze_orders
from src.silver import transform_to_silver, combine_orders_invoices
from src.gold import distribution_of_crate_types, full_address, full_contact_name, calculate_sales_commissions, salesowners_per_company
from src.utils.data_loader import validate_data

def main():
    spark = SparkSession.builder.appName("IFCODataAnalysis").getOrCreate()
    
    try:
        # Cargar datos crudos desde la capa Bronce
        orders_df = load_bronze_orders(spark)
        invoicing_df = load_bronze_invoincing(spark)
        
        # Validar que los datos contengan las columnas esperadas
        validate_data(orders_df, ['order_id','date','company_id','company_name','crate_type','contact_data','salesowners'])
        
        # Transformar a capa Plata (limpieza y estructuración)
        orders_df_silver = transform_to_silver(orders_df)
        combined_df = combine_orders_invoices(orders_df_silver, invoicing_df) 
        
        # Ejecutar análisis desde la capa Oro
        distribution_df = distribution_of_crate_types(orders_df_silver)
        df_1 = full_contact_name(orders_df_silver)
        df_2 = full_address(orders_df_silver)
        df_commisions = calculate_sales_commissions(combined_df)
        df_3 = salesowners_per_company(orders_df_silver)
        
        # Mostrar resultados
        print("Test 1: Distribution of Crate Type per Company:")
        distribution_df.show()

        print("Test 2: DataFrame of Orders with Full Name of the Contact:")
        df_1.show()

        print("Test 3: DataFrame of Orders with Contact Address:")
        df_2.show()

        print("Test 4: Calculation of Sales Team Commissions")
        df_commisions.show()

        print("Test 5: DataFrame of Companies with Sales Owners")
        df_3.show(truncate=False)
    
    finally:
        spark.stop()

if __name__ == '__main__':
    main()
