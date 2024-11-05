from data_warehouse.extract_data_mongo.company import company_data
from google.cloud import bigquery
from part_1.airflow.data_warehouse.database_connection import project, dataset
import datetime

company = company_data()
company['ingest_date'] = datetime.today().strftime('%Y-%m-%d')
project_id = project()
dataset_id = dataset()

company.write.format("bigquery").option("table", f"{project_id}.{dataset_id}.company").save()