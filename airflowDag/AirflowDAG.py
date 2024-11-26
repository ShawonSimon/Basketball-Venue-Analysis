from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
from basketball_pipeline import extract_wikipedia_data, transform_wikipedia_data, write_wikipedia_data

dag = DAG(
    dag_id='basketball_wiki',
    default_args={
        "owner":"Shawon Simon",
        "start_date": datetime.now(),
    },
    schedule_interval=None,
    catchup=False
    )

#Extraction
extract_data_from_wiki = PythonOperator(
    task_id = "extract_wikipedia_data", 
    python_callable=extract_wikipedia_data,
    provide_context=True,
    op_kwargs={"url": "https://en.wikipedia.org/wiki/List_of_basketball_arenas"},
    dag=dag
)

#transform 
transform_wikipedia_data = PythonOperator(
    task_id='transform_wikipedia_data',
    provide_context=True,
    python_callable=transform_wikipedia_data,
    dag=dag
)

#write
write_to_adls = PythonOperator(
    task_id='write_to_adls',
    python_callable=write_wikipedia_data,
    provide_context=True,
    dag=dag,
)

extract_data_from_wiki >> transform_wikipedia_data >> write_to_adls