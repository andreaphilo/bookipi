from part_1.airflow.data_warehouse.database_connection import database

spark = database()
user_df = spark.read.format("mongo").option("collection", "user").load()


def user_data(user_df):
    user_data = user_df.withColumnRenamed("e", "email") \
                    .withColumnRenamed("dci", "default_company_id")\
                    .withColumnRenamed("f", "first_name")\
                    .withColumnRenamed("l", "last_name")

    return user_data
