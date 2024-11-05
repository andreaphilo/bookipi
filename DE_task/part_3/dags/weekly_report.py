from datetime import datetime, timezone
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow import models
from airflow.models import Variable
import pendulum


timestamp_default = datetime.now(tz=timezone.utc)
local_tz = pendulum.timezone("Australia/NSW")
timestamp = pendulum.instance(timestamp_default).in_tz(local_tz)

project_id = Variable.get("PROJECT_ID_REPORTING")
dataset = Variable.get("DATASET_REPORTING")
table_name  = "weekly_churn_analysis"

default_args = {
    'owner'           : 'data_team@gmail.com',
    "depends_on_past" : False,
    'start_date'      : datetime(2024, 11, 3, tzinfo=local_tz),
    'retries'         : None,
    'trigger_rule'    : 'all_success'
}

dag = models.DAG(
    'weekly_churn_report',
    default_args      = default_args,
    description       = 'weekly churn report pipeline',
    schedule_interval = '1 0 * * 0',
    catchup=False,
)

weekly_churn_report_task = BigQueryInsertJobOperator(
    task_id='copy-table-1',
    configuration={
        'query': {
            'query': 'home/airflow/sql_files/weekly_churn_analysis.sql',
            'destinationTable': {
                'projectId': project_id,
                'datasetId': dataset,
                'tableId': table_name
            },
            'useLegacySql': False,
            'allowLargeResults': True,
            'writeDisposition': 'APPEND' #or you can change to 'replace' or 'truncate' if you want the destination table only contains current week report.
        }
    },
    dag=dag
)

#you can add more weekly report task in here, so this dags will specialize to manage weekly report

weekly_churn_report_task

#and in here you can set all the weekly reports task run in parallel or sequences