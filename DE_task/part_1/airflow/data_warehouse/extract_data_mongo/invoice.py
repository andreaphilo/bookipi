from part_1.airflow.data_warehouse.database_connection import database
from pyspark.sql.functions import explode
from pyspark.sql.functions import col

spark = database()
invoice_df = spark.read.format("mongo").option("collection", "invoice").load()

def invoice_item_data(invoice_df):
    invoice_item_data = invoice_df.select("ui", explode("it").alias("item")) \
                                .select("ui", col("item.name").alias("item_name"),
                                        col("item.quantity").alias("item_quantity"),
                                        col("item.price").alias("item_price"))
    return invoice_item_data

def invoice_payments_data(invoice_df):
    invoice_payments_data = invoice_df.select("ui", explode("pm").alias("payment")) \
                                    .select("ui", col("payment.amount").alias("payment_amount"),
                                            col("payment.date").alias("payment_date"))
    return invoice_payments_data
