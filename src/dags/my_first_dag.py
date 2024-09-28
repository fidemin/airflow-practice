from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

dag = DAG(
    'my_first_dag',
    start_date=datetime(2024, 9, 28),
    schedule_interval=timedelta(minutes=5),
)

echo_bash = BashOperator(
    task_id='echo_bash',
    bash_command='echo "Hello, World from bash {{ execution_date.strftime("%Y-%m-%d %H:%M:%S") }}"',
    dag=dag,
)

def _echo_python():
    print("Hello, World from python")

echo_python = PythonOperator(
    task_id='echo_python',
    python_callable=_echo_python,
    dag=dag,
)

echo_bash >> echo_python