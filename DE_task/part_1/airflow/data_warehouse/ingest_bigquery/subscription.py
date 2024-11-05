from data_warehouse.extract_data_mongo.subscription import subscription_data
from google.cloud import bigquery
from part_1.airflow.data_warehouse.database_connection import project, dataset
import datetime

subscription = subscription_data()
subscription['ingest_date'] = datetime.today().strftime('%Y-%m-%d')
project_id = project()
dataset_id = dataset()

subscription.write.format("bigquery").option("table", f"{project_id}.{dataset_id}.subscription").save()