from data_warehouse.extract_data_mongo.invoice import invoice_item_data, invoice_payments_data
from google.cloud import bigquery
from part_1.airflow.data_warehouse.database_connection import project, dataset
import datetime

invoice_item = invoice_item_data()
invoice_item['ingest_date'] = datetime.today().strftime('%Y-%m-%d')
invoice_payments = invoice_payments_data()
invoice_payments['ingest_date'] = datetime.today().strftime('%Y-%m-%d')

project_id = project()
dataset_id = dataset()

invoice_item.write.format("bigquery").option("table", f"{project_id}.{dataset_id}.invoice_item").save()

invoice_payments.write.format("bigquery").option("table", f"{project_id}.{dataset_id}.invoice_payments").save()