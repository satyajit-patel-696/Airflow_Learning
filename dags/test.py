from airflow.sdk import dag,task
@dag(dag_id="first_dag")
def first_dag():
    @task
    def task_1():
        print("task1")
    @task
    def task_2():
        print("task2")
    t1=task_1()
    t2=task_2()
    t1>>t2
first_dag_instance=first_dag()