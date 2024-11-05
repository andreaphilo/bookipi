from part_1.airflow.data_warehouse.database_connection import database

spark = database()
subscription_df = spark.read.format("mongo").option("collection", "subscription").load()

def subscription_data(subscription_df):
    subscription_data = subscription_df
    return subscription_data