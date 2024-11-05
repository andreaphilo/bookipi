from part_1.airflow.data_warehouse.database_connection import database

spark = database()
subscription_payment_df = spark.read.format("mongo").option("collection", "subscription_payment").load()

def subscription_payment_data(subscription_payment_df):
    subscription_payment_data = subscription_payment_df
    return subscription_payment_data