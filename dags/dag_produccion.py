"""
DAG de extracción de datos de Producción.

Segundo eslabón del pipeline de ingesta. Simula la lectura de datos
del área de producción (órdenes, lotes, etc.).

Es disparado por dag_orquestador (tarea disparar_produccion).
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging
import time

default_args = {
    'owner': 'grupo2',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}


def simular_tarea(tarea_nombre):
    """
    Simula una tarea de extracción/procesamiento de datos.

    Args:
        tarea_nombre: Nombre descriptivo que se muestra en los logs.

    Returns:
        True si la operación terminó correctamente.
    """
    logger = logging.getLogger("airflow.task")

    logger.info(f"========== INICIO: {tarea_nombre} ==========")
    time.sleep(2)
    logger.info(
        f"MENSAJE DE EJECUCIÓN: Extrayendo/Procesando datos para {tarea_nombre} "
        f"según la arquitectura definida."
    )
    logger.info("ESTADO: Operación validada y exitosa.")
    logger.info(f"========== FIN: {tarea_nombre} ==========")

    return True


with DAG(
    dag_id='dag_produccion',
    default_args=default_args,
    description='DAG para simular la extracción de datos de Producción',
    schedule_interval=None,
    start_date=datetime(2026, 6, 1),
    catchup=False,
    tags=['produccion'],
) as dag:

    extraer_produccion = PythonOperator(
        task_id='extraer_produccion',
        python_callable=simular_tarea,
        op_kwargs={'tarea_nombre': 'Extracción de Producción'},
        retries=3,
    )
