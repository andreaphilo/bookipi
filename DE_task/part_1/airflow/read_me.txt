In this case i will using airflow for the orchestration tools, but its not mandatory we can use virtual machine too and add cronjob to schedule a job,
or maybe kafka for streaming but it depends on the needs. So for this one i will choose airflow.


End to end process will be using airflow.
and dbt part can be optional too.

the dags mongotobigquery pipeline will be scheduled daily but can be change depends on needs.

the flow will be

the 1st task of airflow will 
connect into mongoDB > extract data per object (user, company, invoice, subsription, and subscription_payment) then ingest it into bigquery as raw (staging layer)

the next task
will be run dbt based on models to do transformation into another layer of bigquery data warehouse