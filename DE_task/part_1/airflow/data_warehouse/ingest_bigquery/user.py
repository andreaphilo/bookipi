from data_warehouse.extract_data_mongo.user import user_data
from google.cloud import bigquery
from part_1.airflow.data_warehouse.database_connection import project, dataset
import datetime

user = user_data()
user['ingest_date'] = datetime.today().strftime('%Y-%m-%d')
project_id = project()
dataset_id = dataset()

user.write.format("bigquery").option("table", f"{project_id}.{dataset_id}.user").save()

