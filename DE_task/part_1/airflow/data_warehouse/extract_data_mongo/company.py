from part_1.airflow.data_warehouse.database_connection import database

spark = database()
company_df = spark.read.format("mongo").option("collection", "user").load()

def company_data(company_df):
    company_data = company_df.withColumnRenamed("c", "company_name") \
                        .withColumnRenamed("co", "country") \
                        .withColumnRenamed("st", "state") \
                        .withColumnRenamed("cr", "currency") \
                        .withColumnRenamed("e", "email")
    return company_data