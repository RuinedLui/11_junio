from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {'owner': 'grupo2', 'retries': 3, 'retry_delay': timedelta(minutes=5)}

def simular_tarea(tarea_nombre):
    # Usamos el logger oficial de Airflow
    logger = logging.getLogger("airflow.task")
    
    # 1. Registro de Inicio
    logger.info(f"========== INICIO: {tarea_nombre} ==========")
    
    # Simular un pequeño tiempo de procesamiento
    time.sleep(2) 
    
    # 2. Mensaje de ejecución
    logger.info(f"MENSAJE DE EJECUCIÓN: Extrayendo/Procesando datos para {tarea_nombre} según la arquitectura definida.")
    
    # 3. Estado
    logger.info(f"ESTADO: Operación validada y exitosa.")
    
    # 4. Registro de Fin
    logger.info(f"========== FIN: {tarea_nombre} ==========")
    
    return True