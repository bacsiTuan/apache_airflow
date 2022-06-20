from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from demo_cronjob.update_price.main import get_id_need_update, update_price

default_args = {
    'owner': 'tuandc',
    'depends_on_past': False,
    'email': ['tuandao864@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retry_delay': timedelta(minutes=1),

    'start_date': datetime(2022, 5, 1, 0, 0),
    'retries': 1,
}

dag = DAG(
    'update_price',
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    concurrency=32,
    schedule_interval=None,
    render_template_as_native_obj=True,
    tags=["demo_cronjob", "auto", "v0.0.1"]
)

task_get_id_need_update = PythonOperator(
    task_id='get_id_need_update',
    dag=dag,
    python_callable=get_id_need_update
)

task_update_price = PythonOperator(
    task_id='update_price',
    dag=dag,
    python_callable=update_price,
    op_kwargs={
        "list_id": "{{ti.xcom_pull(task_ids='get_id_need_update', key='list_id')}}"
    }
)

task_get_id_need_update >> task_update_price
