from pyspark.sql.functions import split
from pyspark.sql.functions import col
from part_2.database_connection import database

spark = database
subscription_payment_df = spark.read.format("mongo").option("collection", "subscription_payment").load()

def subscription_payment_data():
    subscription_payment_data = subscription_payment_df.withColumn("start_date", split(col("billingPeriod"), "-").getItem(0)) \
                                                        .withColumn("end_date", split(col("billingPeriod"), "-").getItem(1)) \
                                                        .drop("billingPeriod")
    return subscription_payment_data