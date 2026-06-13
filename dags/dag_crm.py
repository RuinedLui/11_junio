from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

from datetime import datetime, timedelta

from src.crm.extract import extraer_clientes
from src.crm.extract import extraer_ordenes
from src.crm.validate import validar_datos
from src.crm.logging_process import registrar_finalizacion

default_args = {
    "owner": "grupo2",
    "retries": 3,
    "retry_delay": timedelta(minutes=5)
}

with DAG(
    dag_id="dag_crm",
    default_args=default_args,
    start_date=datetime(2026, 6, 12),
    schedule="@daily",
    catchup=False,
    tags=["crm"]
) as dag:

    inicio = EmptyOperator(
        task_id="inicio"
    )

    clientes = PythonOperator(
        task_id="extraer_clientes",
        python_callable=extraer_clientes
    )

    ordenes = PythonOperator(
        task_id="extraer_ordenes",
        python_callable=extraer_ordenes
    )

    validacion = PythonOperator(
        task_id="validar_datos",
        python_callable=validar_datos
    )

    log_final = PythonOperator(
        task_id="registrar_finalizacion",
        python_callable=registrar_finalizacion
    )

    fin = EmptyOperator(
        task_id="fin"
    )

    inicio >> clientes >> ordenes >> validacion >> log_final >> fin
    