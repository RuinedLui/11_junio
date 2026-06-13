"""
DAG de transformación de datos.

Cuarta etapa del pipeline. Simula la limpieza, unión y transformación
de los datos extraídos de CRM, Producción e Inventario.

Es disparado por dag_orquestador (tarea disparar_transformacion).
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
    Simula una tarea de transformación de datos.

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
    dag_id='dag_transformacion',
    default_args=default_args,
    description='DAG para la transformación de datos',
    schedule_interval=None,
    start_date=datetime(2026, 6, 1),
    catchup=False,
    tags=['transformacion'],
) as dag:

    transformar_datos = PythonOperator(
        task_id='transformar_datos',
        python_callable=simular_tarea,
        op_kwargs={'tarea_nombre': 'Transformación de Datos'},
        retries=3,
    )
