from datetime import datetime,timedelta
from textwrap import dedent
# Importation le fichier python projetpipeline. On importe la fonction du fichier python main_function projetpipeline.py
from projetpipeline import main_function
from script1 import scriptspark

# importation de DAG pour instancier le DAG sur AirFlow
from airflow import DAG
from airflow.operators.python import PythonOperator

# Création du Dag : "ProjetDataPipelineFinal"
# Utilisation de l'opérateur intégré PythonOperator de Airflow pour faire appel à la fonction main_function importé plus haut 
with DAG("ProjetDataPipelineAirflowFinal",start_date=datetime(2021,1,1),schedule='*/30 * * * *',catchup=False) as dag:

    task = PythonOperator(
    task_id='task',
    python_callable=main_function,
)
    task2 = PythonOperator(
    task_id='task2',
    python_callable=scriptspark,

)

    task >> task2