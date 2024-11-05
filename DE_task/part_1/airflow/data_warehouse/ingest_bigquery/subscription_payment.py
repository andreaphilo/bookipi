from data_warehouse.extract_data_mongo.subscription_payment import subscription_payment_data
from google.cloud import bigquery
from part_1.airflow.data_warehouse.database_connection import project, dataset
import datetime


subscription_payment = subscription_payment_data()
subscription_payment['ingest_date'] = datetime.today().strftime('%Y-%m-%d')
project_id = project()
dataset_id = dataset()

subscription_payment.write.format("bigquery").option("table", f"{project_id}.{dataset_id}.subscription_payment").save()