"""
Code that goes along with the Airflow located at:
http://airflow.readthedocs.org/en/latest/tutorial.html
http://gilsondev.in/airflow/dados/2018/06/23/automatizando-fluxo-de-trabalho-com-airflow/
https://medium.com/@gilsondev/automatizando-seu-fluxo-de-trabalho-com-airflow-4dbc1c932dcb
https://towardsdatascience.com/getting-started-with-apache-airflow-df1aa77d7b1b
TO execute: https://medium.com/@lopesdiego12/como-criar-seu-primeiro-data-pipeline-no-apache-airflow-dbe3791a4053

Comandos para executar:
sudo docker pull puckel/docker-airflow
sudo docker-compose -f docker-compose-CeleryExecutor.yml up -d
sudo docker cp Text.py 7ac:/usr/local/airflow/dags
sudo docker cp DagCode.py 7ac:/usr/local/airflow/dags
sudo docker cp exemplo.yml 7ac:/usr/local/airflow/dags/config
"""
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator


def lerArquivo():
    print('Writing in file')
    with open('/home/diogo/Apache-Airflow/arquivo.txt', 'a+', encoding='utf8') as f:
        now = dt.datetime.now()
        t = now.strftime("%Y-%m-%d %H:%M")
        f.write(str(t) + '\n')
    return 'Writed'


def responseMessage():
    return 'Greet Responded Again'

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2020, 11, 15),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

dag = DAG("tutorial", default_args=default_args, schedule_interval=timedelta(1))
templated_command = """
    {% for i in range(5) %}
        echo "{{ ds }}"
        echo "{{ macros.ds_add(ds, 7)}}"
        echo "{{ params.my_param }}"
    {% endfor %}
"""
#tasks
# t1, t2 and t3 are examples of tasks created by instantiating operators
task1 = BashOperator(task_id='say_Hi', bash_command='echo "Teste de escrita de arquivo"', dag=dag)
#Imprime a data na saída padrão
task2 = BashOperator(task_id="print_date", bash_command="date", dag=dag)
#Salve a data em um arquivo texto
task3 = PythonOperator(task_id='greet', python_callable=lerArquivo, dag=dag)
#Faz uma sleep de 5 segundos.
task4 = BashOperator(task_id="sleep", bash_command="sleep 5", retries=3, dag=dag)
task5 = PythonOperator(task_id='respond', python_callable=responseMessage)
task6 = BashOperator(task_id="templated", bash_command=templated_command, params={"my_param": "DAG executado"}, dag=dag)

task2.set_upstream(task1)
task3.set_upstream(task2)
task4.set_upstream(task3)
task5.set_upstream(task2)
task6.set_upstream(task5)
