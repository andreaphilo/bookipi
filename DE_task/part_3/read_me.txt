to create weekly churn analysis report we need another dedicated dags.
this dags will triggered weekly, you can adjust the time.
so the dags will be execute the query file then store it into bigquery table in weekly basis.
you can add any other weekly report in this dags. so this dags will be specialize to do weekly job only. and it easier to maintain without risking other pipeline,
and you can easily change the sql without changing the dags itself (if the logic needs to change).