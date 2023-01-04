from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

from datetime import datetime

import os

default_args = {
    "owner": "airflow"
    ,"start_date": datetime(2023, 1, 4)
    , "end_date": datetime(2023, 1, 5)
}

dag = DAG(
    dag_id='process_web_log',
    default_args=default_args,
    schedule_interval='@daily',
    # ,catchup=False
)

def scan_for_log():
    os.chdir('/opt/airflow/')
    files = os.listdir('the_logs/')
    if 'log.txt' not in files:
        raise FileNotFoundError('The file "log.txt" does not exist in directory "the_logs/"')
    else:
        print("File exists.")

t1 = PythonOperator(
    task_id='scan_for_log'
    ,dag=dag
    ,python_callable=scan_for_log
)

def extract_data():
    os.chdir('/opt/airflow/')
    fpath = 'the_logs/log.txt'
    out_fpath = 'the_logs/extracted_data.txt'
    out_lines = []

    with open(fpath, "r") as f:
        for line in f:
            out_line = line.split("-")[0]
            out_line = out_line.strip(" ")
            out_lines.append(out_line+'\n')
    
    with open(out_fpath, "w") as f:
        f.writelines(out_lines)

t2 = PythonOperator(
    task_id='extract_data'
    ,dag=dag
    ,python_callable=extract_data
)

def transform_data():
    os.chdir('/opt/airflow/')
    fpath = 'the_logs/extracted_data.txt'
    out_fpath = 'the_logs/transformed_data.txt'
    out_lines = []

    with open(fpath, "r") as f:
        for line in f:
            if line != "198.46.149.143\n":
                out_lines.append(line)
    
    with open(out_fpath, "w") as f:
        f.writelines(out_lines)

t3 = PythonOperator(
    task_id='transform_data'
    ,dag=dag
    ,python_callable=transform_data
)

t4 = BashOperator(
    task_id='load_data'
    ,dag=dag
    ,cwd='/opt/airflow/'
    ,bash_command='tar -czf the_logs/weblog.tar the_logs/transformed_data.txt'
)

t1 >> t2 
t2 >> t3 
t3 >> t4 
