"""
DAG de carga al Data Warehouse y reportería.

Última etapa del pipeline. Simula la carga de datos transformados
al DW y la generación de reportes para el negocio.

Es disparado por dag_orquestador (tarea disparar_dw_reporting).
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
    Simula la carga al DW y la generación de reportes.

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
    dag_id='dag_dw_reporting',
    default_args=default_args,
    description='DAG para cargar datos al DW y generar reportes',
    schedule_interval=None,
    start_date=datetime(2026, 6, 1),
    catchup=False,
    tags=['dw', 'reporting'],
) as dag:

    cargar_y_reportar = PythonOperator(
        task_id='cargar_y_reportar',
        python_callable=simular_tarea,
        op_kwargs={'tarea_nombre': 'Carga DW y Reportería'},
        retries=3,
    )
