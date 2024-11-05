from datetime import datetime, timezone
from airflow.operators.bash_operator import BashOperator
from airflow.providers.dbt.operators.dbt import DbtRunOperator
from airflow import models
import pendulum


timestamp_default = datetime.now(tz=timezone.utc)
local_tz = pendulum.timezone("Australia/NSW")
timestamp = pendulum.instance(timestamp_default).in_tz(local_tz)

default_args = {
    'owner'           : 'data_team@gmail.com',
    "depends_on_past" : False,
    'start_date'      : datetime(2024, 11, 3, tzinfo=local_tz),
    'retries'         : None,
    'trigger_rule'    : 'all_success'
}

dag = models.DAG(
    'MongotoBigquery',
    default_args      = default_args,
    description       = 'Mongo to Bigquery Pipeline',
    schedule_interval = '1 0 * * *',
    catchup=False,
)


user_pipeline = BashOperator(
    task_id='user_pipeline_task',
    bash_command='python /home/airflow/datawarehouse/ingest_bigquery/user.py',
    dag=dag,
)

company_pipeline = BashOperator(
    task_id='company_pipeline_task',
    bash_command='python /home/airflow/datawarehouse/ingest_bigquery/company.py',
    dag=dag,
)

invoice_pipeline = BashOperator(
    task_id='invoice_pipeline_task',
    bash_command='python /home/airflow/datawarehouse/ingest_bigquery/invoice.py',
    dag=dag,
)

subscription_pipeline = BashOperator(
    task_id='subscription_pipeline_task',
    bash_command='python /home/airflow/datawarehouse/ingest_bigquery/subscription.py',
    dag=dag,
)

subscription_payment_pipeline = BashOperator(
    task_id='subscription_payment_pipeline_task',
    bash_command='python /home/airflow/datawarehouse/ingest_bigquery/subscription_payment.py',
    dag=dag,
)

transform_user = DbtRunOperator(
    task_id='transform_user_task',
    models = 'user',
    dbt_bin='dbt run --project-dir /path/to/your/dbt/project --profiles-dir /path/to/your/dbt',
    dag=dag,
)

transform_company = DbtRunOperator(
    task_id='transform_company_task',
    models = 'company',
    dbt_bin='dbt run --project-dir /path/to/your/dbt/project --profiles-dir /path/to/your/dbt',
    dag=dag,
)

transform_invoice_item = DbtRunOperator(
    task_id='transform_invoice_item_task',
    models = 'invoice_item',
    dbt_bin='dbt run --project-dir /path/to/your/dbt/project --profiles-dir /path/to/your/dbt',
    dag=dag,
)

transform_invoice_payments = DbtRunOperator(
    task_id='transform_invoice_payments_task',
    models = 'invoice_payments',
    dbt_bin='dbt run --project-dir /path/to/your/dbt/project --profiles-dir /path/to/your/dbt',
    dag=dag,
)

transform_subscription = DbtRunOperator(
    task_id='transform_subscription_task',
    models = 'subscription',
    dbt_bin='dbt run --project-dir /path/to/your/dbt/project --profiles-dir /path/to/your/dbt',
    dag=dag,
)

transform_subscription_payment = DbtRunOperator(
    task_id='transform_subscription_payment_task',
    models = 'subscription_payment',
    dbt_bin='dbt run --project-dir /path/to/your/dbt/project --profiles-dir /path/to/your/dbt',
    dag=dag,
)


user_pipeline >> transform_user
company_pipeline >> transform_company
invoice_pipeline >> [transform_invoice_item, transform_invoice_payments]
subscription_pipeline >> transform_subscription
subscription_payment_pipeline >> transform_subscription_payment

