# EXEMPLO DE UMA DAG QUE EXECUTA TAREFAS EM SÉRIE
from airflow import DAG
from datetime import datetime
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.contrib.operators.wasb_delete_blob_operator import WasbDeleteBlobOperator
from airflow.contrib.operators.file_to_wasb import FileToWasbOperator
from airflow.models import Variable
import os
from random import randint
#from projeto_grupo_05.write_tracks_spotify import write_tracks_spotify

os.environ["JAVA_HOME"] = Variable.get('JAVA_HOME')

default_args = {
    'owner': 'grupo_05',
    'start_date': datetime(2023, 5, 7)
}
dag = DAG(
    dag_id='dag_explode_json',
    default_args=default_args,
    schedule_interval='0 3 * * *'
)

start = DummyOperator(
    task_id='start',
    dag=dag)

# task1 = PythonOperator(
#     task_id='write_tracks_spotify',
#     python_callable=write_tracks_spotify,
#     dag=dag
# )

task_bronze = SparkSubmitOperator(
                          task_id=f'task_id_bronze',
                          conn_id='spark_local',
                          jars='/usr/local/airflow/jars/delta-core_2.12-2.0.0.jar,\
                                /usr/local/airflow/jars/delta-storage-2.0.0.jar,\
                                /usr/local/airflow/jars/azure-storage-8.6.4.jar,\
                                /usr/local/airflow/jars/hadoop-azure-3.2.1.jar,\
                                /usr/local/airflow/jars/jetty-util-9.3.25.v20180904.jar,\
                                /usr/local/airflow/jars/jetty-util-ajax-11.0.7.jar'.replace(' ', ''),
                          conf={'spark.driver.host' : 'localhost',
                                'spark.ui.port' : randint(4040, 5050),
                                'spark.executor.extraJavaOptions': '-Djava.security.egd=file:///dev/urandom -Duser.timezone=UTC',
                                'spark.driver.extraJavaOptions': '-Djava.security.egd=file:///dev/urandom -Duser.timezone=UTC'},
                          driver_memory='500m',
                          num_executors=0,
                          executor_memory='100m',
                          name=f'task_id',
                          application='/usr/local/airflow/dags/projeto_grupo_05/BRONZE_TRACKS_SPOTIFY.py',
                          application_args=[],
                          dag=dag
                      )

account_name = "aulafiaead"
account_key = "QDKbVST0U3yAaEI4HN9DFwYTB3jGO6xb4Kk5r59UFYOzXrkrVLESZKmrKzPZ/eEsDLV8Fw5XxybA+ASt4EZ2zA=="
container_name = "grupo5"
directory_name = "landing"

rootPath = "wasbs://" + container_name + "@" + account_name + ".blob.core.windows.net/"
bronzePath = rootPath+"delta/bronze"
silverPath = rootPath+"delta/silver"
landzonePath = rootPath+"landing"

# task_copy = FileToWasbOperator(
#     task_id='copy_file',
#     file_path='',
#     container_name='bronze',
#     blob_name='*',
#     wasb_conn_id = 'wasb_conn',
# #    source_bucket_name='landing',
# #    source_bucket_key='tabela.txt',
# #    dest_bucket_name ='raw',
# #    dest_bucket_key='tabela.txt',
# #    aws_conn_id='minio_s3',
# #    trigger_rule='one_success', ### LISTA COMPLETA DOS TIPOS DE TRIGGER RULES ---> https://airflow.apache.org/docs/apache-airflow/1.10.3/concepts.html?highlight=trigger%20rule
#     dag=dag
# )

# task_delete = WasbDeleteBlobOperator(
#     task_id='delete_raw_file',
#     container_name= 'landing',
#     blob_name='*',
# #    bucket='raw',
# #    prefix='tabela.txt',
#     wasb_conn_id= 'wasb_conn',
# #    aws_conn_id='minio_s3',
#     dag=dag
# )

task_silver = SparkSubmitOperator(
                          task_id=f'task_id_silver',
                          conn_id='spark_local',
                          jars='/usr/local/airflow/jars/delta-core_2.12-2.0.0.jar,\
                                /usr/local/airflow/jars/delta-storage-2.0.0.jar,\
                                /usr/local/airflow/jars/azure-storage-8.6.4.jar,\
                                /usr/local/airflow/jars/hadoop-azure-3.2.1.jar,\
                                /usr/local/airflow/jars/jetty-util-9.3.25.v20180904.jar,\
                                /usr/local/airflow/jars/jetty-util-ajax-11.0.7.jar'.replace(' ', ''),
                          conf={'spark.driver.host' : 'localhost',
                                'spark.ui.port' : randint(4040, 5050),
                                'spark.executor.extraJavaOptions': '-Djava.security.egd=file:///dev/urandom -Duser.timezone=UTC',
                                'spark.driver.extraJavaOptions': '-Djava.security.egd=file:///dev/urandom -Duser.timezone=UTC'},
                          driver_memory='500m',
                          num_executors=0,
                          executor_memory='100m',
                          name=f'task_id',
                          application='/usr/local/airflow/dags/projeto_grupo_05/SILVER_TRACKS_SPOTIFY.py',
                          application_args=[],
                          dag=dag
                      )

finish = DummyOperator(
    task_id='finish',
    dag=dag)

#start >> task_bronze >> task_copy >> task_delete >> task_silver  >> finish
start >> task_bronze >> task_silver  >> finish
