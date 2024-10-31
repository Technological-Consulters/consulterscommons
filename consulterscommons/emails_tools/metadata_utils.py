from prefect import runtime, task
from prefect.variables import Variable

from consulterscommons.log_tools.prefect_log_config import PrefectLogger

logger_global = PrefectLogger(__file__)

DEVS_DATA_VARIABLE_NAME = "devs_responsables"
METADATA_TEMPLATE = "metadata_template"

@task
def log_metadata(responsible_id: str, script_path: str, area: str, **kwargs) -> None:
    """
    Registra metadatos para un script.

    Parámetros:
    - responsable_id (str): ID del responsable.
    - script_path (str): Ruta del script.
    - area (str): Área del script.
    - **kwargs: Argumentos adicionales opcionales para el registro de metadatos.
        - development_state (str, opcional): Estado del desarrollo del script.
    """

    # Verificar que los argumentos obligatorios no estén vacíos
    if not responsible_id or not script_path or not area:
        raise ValueError("Los argumentos 'responsible_id', 'script_path' y 'area' son obligatorios.")

    logger = logger_global.obtener_logger_prefect()

    dict_devs = Variable.get(DEVS_DATA_VARIABLE_NAME, default={})
    metadata_template = Variable.get(METADATA_TEMPLATE, default={})

    logger.debug("Plantilla y variables obtenidas")

    new_metadata = metadata_template.copy()
    new_metadata.update({
        'script_path': script_path, 
        'responsible_developer': dict_devs.get(responsible_id, 'Desconocido'), 
        'area': area
    })

    # Agregar argumentos adicionales de kwargs a new_metadata
    new_metadata.update(kwargs)

    # Get existing metadata for the deployment
    if runtime.deployment.id:
        deployment_metadata_key = str(runtime.deployment.id).replace('-', '_')
        original_metadata = Variable.get(deployment_metadata_key, default=None)
    else:
        return

    if original_metadata:
        logger.info("Metadata original obtenida: %s", str(original_metadata))
        # Update only if metadata has changed
        if original_metadata != new_metadata:
            Variable.set(name=deployment_metadata_key, value=new_metadata, overwrite=True)
            logger.info("Metadata actualizada: %s", str(new_metadata))
    else:
        # Create new metadata if none exists
        Variable.set(name=deployment_metadata_key, value=new_metadata, tags=['metadata', 'dict'])
        logger.info("Metadata creada: %s", str(new_metadata))
    return


@task
def get_metadata(deployment_id: str) -> dict:
    """Retorna el diccionario de metadata asociado a un deployment_id"""

    if not deployment_id:
        return None
    elif not isinstance(deployment_id, str):
        deployment_id = str(deployment_id)

    variable_name = deployment_id.replace('-', '_')
    metadata = Variable.get(variable_name, default=None)

    if isinstance(metadata, dict):
        return metadata
    else:
        return None
