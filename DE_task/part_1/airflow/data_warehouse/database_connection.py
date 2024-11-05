from pyspark.sql import SparkSession

def database():
    spark = SparkSession.builder \
        .appName("MongoDBToBigQuery") \
        .config("spark.mongodb.input.uri", "mongodb://localhost:27017/your_database.user") \
        .getOrCreate()
    return spark


def project():
    project_id = "datawarehouse"
    return project_id

def dataset():
    dataset = "staging"
    return dataset

