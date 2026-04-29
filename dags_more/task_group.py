from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime

def print_task(msg):
    print(msg)

with DAG(
    dag_id="taskgroup_example",
    start_date=datetime(2023,1,1),
    schedule_interval="@daily",
    catchup=False
) as dag:

    start = PythonOperator(
        task_id="start",
        python_callable=print_task,
        op_args=["Start"]
    )

    # TaskGroup starts
    with TaskGroup("transform_tasks") as transform_group:

        t1 = PythonOperator(
            task_id="clean_data",
            python_callable=print_task,
            op_args=["Cleaning data"]
        )

        t2 = PythonOperator(
            task_id="validate_data",
            python_callable=print_task,
            op_args=["Validating data"]
        )

        t3 = PythonOperator(
            task_id="aggregate_data",
            python_callable=print_task,
            op_args=["Aggregating data"]
        )

        t1 >> t2 >> t3

    end = PythonOperator(
        task_id="end",
        python_callable=print_task,
        op_args=["End"]
    )

    start >> transform_group >> end