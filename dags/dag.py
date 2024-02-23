from airflow.models.dag import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable

from db import create_engine_instance, create_database_if_not_exists
from extract import main as extract
from transfrom import main as transfrom
from load import main as load

# Define parameters
params = {
    'user': Variable.get("pg-user"),
    'password': Variable.get("pg-password"),
    'host': Variable.get("pg-host"),
    'dbname': Variable.get("pg-database"),
}


# [START]
with DAG(dag_id="Data-pipeline", 
        start_date=days_ago(2), 
        schedule_interval="0 0 * * *",
        tags=["ETL"]) as dag:
    # Task to create database if not exists
    task_create_database = PythonOperator(
        task_id='check_connection_database',
        python_callable=create_database_if_not_exists,
        op_args=[create_engine_instance({**params, 'dbname': 'postgres'}), params['dbname']]
    )
    
    # Task to run data extraction
    task_extract_employee = PythonOperator(
        task_id='extract_employee',
        python_callable=extract,
        op_args=[create_engine_instance(params), './data/employees.csv']
    )

    task_extract_timesheet = PythonOperator(
        task_id='extract_timesheet',
        python_callable=extract,
        op_args=[create_engine_instance(params), './data/timesheets.csv']
    )

    transform_task = PythonOperator(
        task_id='transform_data',
        python_callable=transfrom,
        op_args=[create_engine_instance(params), task_extract_employee.output, task_extract_timesheet.output]
    )

    load_task = PythonOperator(
        task_id='load_data',
        python_callable=load,
        op_args=[create_engine_instance(params), transform_task.output]
    )


    # Set task dependencies
    task_create_database >> [task_extract_employee, task_extract_timesheet] >> transform_task >> load_task
# [END]