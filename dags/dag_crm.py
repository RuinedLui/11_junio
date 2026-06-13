"""
DAG de extracción de datos CRM.

Primer eslabón del pipeline de ingesta. Simula la lectura de datos
desde el sistema CRM y los deja listos para las etapas siguientes.

Es disparado por dag_orquestador (tarea disparar_crm).
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging
import time

# Configuración compartida por todas las tareas del DAG.
# retries=3 → Airflow reintenta hasta 3 veces antes de marcar la tarea como failed.
default_args = {
    'owner': 'grupo2',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}


def simular_tarea(tarea_nombre):
    """
    Simula una tarea de extracción/procesamiento de datos.

    En producción aquí iría la lógica real (conexión a BD, API, etc.).
    Los logger.info() aparecen en la pestaña Log de Airflow (no en Event Log).

    Args:
        tarea_nombre: Nombre descriptivo que se muestra en los logs.

    Returns:
        True si la operación terminó correctamente.
    """
    logger = logging.getLogger("airflow.task")

    logger.info(f"========== INICIO: {tarea_nombre} ==========")

    # Simula tiempo de lectura/procesamiento de datos
    time.sleep(2)

    logger.info(
        f"MENSAJE DE EJECUCIÓN: Extrayendo/Procesando datos para {tarea_nombre} "
        f"según la arquitectura definida."
    )
    logger.info("ESTADO: Operación validada y exitosa.")
    logger.info(f"========== FIN: {tarea_nombre} ==========")

    return True


with DAG(
    dag_id='dag_crm',
    default_args=default_args,
    description='DAG para simular la extracción de datos de CRM',
    schedule_interval=None,  # Solo se ejecuta cuando el orquestador lo dispara
    start_date=datetime(2026, 6, 1),
    catchup=False,  # No ejecutar corridas históricas pendientes
    tags=['crm'],
) as dag:

    extraer_crm = PythonOperator(
        task_id='extraer_crm',
        python_callable=simular_tarea,
        op_kwargs={'tarea_nombre': 'Extracción de CRM'},
        retries=3,
    )
