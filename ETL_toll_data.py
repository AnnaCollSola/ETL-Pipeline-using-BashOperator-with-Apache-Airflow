# Import the libraries
from datetime import datetime, timedelta
# The DAG object; we'll need this to instantiate a DAG
from airflow.models import DAG
# Operators; you need this to write tasks!
from airflow.operators.bash import BashOperator


# Define DAG arguments
default_args = {
    'owner': 'Anna',
    'start_date': datetime(2026,9,18),
    'email': ['anna@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


# Define the DAG
dag = DAG(
    'ETL_toll_data',
    default_args=default_args,
    description='Apache Airflow Final Assignment',
    schedule=timedelta(days=1),
)


# define task to unzip data
unzip_data = BashOperator(
    task_id='extract',
    bash_command='tar -xvf /opt/airflow/dags/finalassignment/tolldata.tgz -C /opt/airflow/dags/finalassignment/',
    dag=dag,
)

# define task to extract data
extract_data_from_csv = BashOperator(
    task_id='extract_data_from_csv',
    bash_command='cut -d"," -f1,2,3,4 /opt/airflow/dags/finalassignment/vehicle-data.csv > /opt/airflow/dags/finalassignment/csv_data.csv',
    dag=dag,
)


extract_data_from_tsv = BashOperator(
    task_id='extract_data_from_tsv',
    bash_command="""
    cut -f5,6,7 /opt/airflow/dags/finalassignment/tollplaza-data.tsv \
    | tr -d '\\r' \
    | tr '\t' ',' \
    > /opt/airflow/dags/finalassignment/tsv_data.csv
    """,
    dag=dag,
)
 
extract_data_from_fixed_width = BashOperator(
    task_id='extract_data_from_fixed_width',
    bash_command="""
    paste -d',' \
    <(cut -c59-61 /opt/airflow/dags/finalassignment/payment-data.txt) \
    <(cut -c63-67 /opt/airflow/dags/finalassignment/payment-data.txt) \
    > /opt/airflow/dags/finalassignment/fixed_width_data.csv
    """,
    dag=dag,
)


consolidate_data = BashOperator(
    task_id='consolidate_data',
    bash_command="""
    paste \
    /opt/airflow/dags/finalassignment/csv_data.csv \
    /opt/airflow/dags/finalassignment/tsv_data.csv \
    /opt/airflow/dags/finalassignment/fixed_width_data.csv \
    | tr '\t' ',' \
    > /opt/airflow/dags/finalassignment/extracted_data.csv
    """,
    dag=dag,
)


transform_data = BashOperator(
    task_id='transform_data',
    bash_command="""
    paste -d',' \
    <(cut -d',' -f1-3 /opt/airflow/dags/finalassignment/extracted_data.csv) \
    <(cut -d',' -f4 /opt/airflow/dags/finalassignment/extracted_data.csv | tr '[:lower:]' '[:upper:]') \
    <(cut -d',' -f5- /opt/airflow/dags/finalassignment/extracted_data.csv) \
    > /opt/airflow/dags/finalassignment/staging/transformed_data.csv
    """,
    dag=dag,
)

unzip_data >> extract_data_from_csv >> extract_data_from_tsv >> extract_data_from_fixed_width >> consolidate_data >> transform_data




