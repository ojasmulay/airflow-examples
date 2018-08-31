
import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.qubole_operator import QuboleOperator
from datetime import timedelta


# these args will get passed on to each operator
# you can override them on a per-task basis during operator initialization
default_args = {
  'owner': 'airflow',
  'depends_on_past': False,
  'start_date': airflow.utils.dates.days_ago(1),
  'email': ['airflow@airflow.com'],
  'email_on_failure': False,
  'email_on_retry': False,
  'retries': 1,
  'retry_delay': timedelta(minutes=5)
}

dag = DAG(
  'QueryFile',
  default_args=default_args,
  description='A simple tutorial DAG',
  schedule_interval=timedelta(days=1))


t1= QuboleOperator(
  task_id='hive_inline',
  command_type='hivecmd',
 # query='show tables',
  script_location='s3://omulay-common/airflow-examples/query_script_path_example/sample_query.sql',
  cluster_label='default',
  tags='aiflow_example_run',  # Attach tags to Qubole command, auto attaches 3 tags - dag_id, task_id, run_id
  qubole_conn_id='qubole_default',  # Connection ID to submit commands inside QDS, if not set **qubole_default** is used
  dag=dag)
