"""
DAG Orquestador del pipeline de datos.

Coordina la ejecución secuencial de todos los DAGs del proyecto:

    CRM → Producción → Inventario → Transformación → DW/Reporting

Se ejecuta automáticamente cada día (@daily) o manualmente desde la UI.
Cada tarea dispara un DAG hijo sin esperar a que termine (fire-and-forget).
"""

from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'grupo2',
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id='dag_orquestador',
    default_args=default_args,
    description='DAG Orquestador para coordinar la ingesta, transformación y reportes',
    schedule_interval='@daily',
    start_date=datetime(2026, 6, 1),
    catchup=False,
    tags=['orquestacion'],
) as dag:

    # Paso 1: Disparar extracción de CRM
    disparar_crm = TriggerDagRunOperator(
        task_id='disparar_crm',
        trigger_dag_id='dag_crm',
        wait_for_completion=False,
        retries=3,
    )

    # Paso 2: Disparar extracción de Producción
    disparar_produccion = TriggerDagRunOperator(
        task_id='disparar_produccion',
        trigger_dag_id='dag_produccion',
        wait_for_completion=False,
        retries=3,
    )

    # Paso 3: Disparar extracción de Inventario
    disparar_inventario = TriggerDagRunOperator(
        task_id='disparar_inventario',
        trigger_dag_id='dag_inventario',
        wait_for_completion=False,
        retries=3,
    )

    # Paso 4: Disparar transformación de datos
    disparar_transformacion = TriggerDagRunOperator(
        task_id='disparar_transformacion',
        trigger_dag_id='dag_transformacion',
        wait_for_completion=False,
        retries=3,
    )

    # Paso 5: Disparar carga al DW y reportería
    disparar_dw_reporting = TriggerDagRunOperator(
        task_id='disparar_dw_reporting',
        trigger_dag_id='dag_dw_reporting',
        wait_for_completion=False,
        retries=3,
    )

    # Orden de ejecución: cada paso depende del anterior
    (
        disparar_crm
        >> disparar_produccion
        >> disparar_inventario
        >> disparar_transformacion
        >> disparar_dw_reporting
    )
