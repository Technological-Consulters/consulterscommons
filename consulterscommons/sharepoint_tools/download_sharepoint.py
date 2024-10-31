"""
Módulo para descargar un archivo de Sharepoint.
Utiliza Prefect para el manejo de logs y keyring para la obtención de contraseñas.
"""

import os
import inspect
import __main__

from office365.sharepoint.client_context import ClientContext
from office365.sharepoint.files.file import File
from office365.runtime.auth.user_credential import UserCredential
from prefect import task

from consulterscommons.log_tools.prefect_log_config import PrefectLogger

logger_global = PrefectLogger(__file__)


@task(retries=3, retry_delay_seconds=10)
def download_sharepoint(sharepoint_email: str,
                        sharepoint_password: str,
                        file_to_download_url: str,
                        file_output_path: str = None) -> File:
    """
    Descarga un archivo de Sharepoint en el directorio seleccionado.
    Si no se especifica un directorio de salida, se descarga en una carpeta 'files' del directorio de trabajo del script que llama a la función.

    Args:
        file_to_download_url (str): URL del archivo a descargar.
        sharepoint_email (str, optional): Correo electrónico de Sharepoint. Defaults to SHAREPOINT_EMAIL.
        sharepoint_password (str, optional): Contraseña de Sharepoint. Defaults to SHAREPOINT_PASSWORD.
        file_output_path (str, optional): Ruta de salida del archivo descargado. Defaults to None.

    Returns:
        File: Objeto File de Sharepoint.

    Raises:
        Exception: Si ocurre un error al descargar el archivo.
    """

    logger = logger_global.obtener_logger_prefect()

    try:
        file_name = os.path.basename(file_to_download_url)

        if file_output_path is None:
            # working_dir = os.path.dirname(__file__)
            working_dir = os.path.dirname(
                __main__.__file__) if __main__.__file__ else os.path.dirname(inspect.stack()[1].filename)
            file_output_path = os.path.join(working_dir, 'files', file_name)

        # Definir credenciales
        user_cred = UserCredential(sharepoint_email, sharepoint_password)

        # Crear una conexión inicial para validar
        # ctx = ClientContext(file_to_download_url).with_credentials(user_cred)
        # web = ctx.web
        # ctx.load(web)
        # try:
        #     ctx.execute_query()
        #     logger.info("Conexión exitosa con SharePoint.")
        # except Exception as conn_err:
        #     logger.error("No se pudo establecer la conexión con SharePoint.")
        #     logger.error("Error: %s", conn_err)
        #     raise

        # Crear directorio si no existe
        file_output_dir = os.path.dirname(file_output_path)
        if not os.path.exists(file_output_dir):
            os.makedirs(file_output_dir)

        # Descargar archivo
        with open(file_output_path, 'wb') as local_file:
            File.from_url(file_to_download_url).with_credentials(
                user_cred).download(local_file).execute_query()

        logger.info("Archivo '%s' descargado en '%s'",
                    file_name, file_output_path)

    except Exception as err:
        logger.error('Ocurrió un error al descargar el archivo de SharePoint')
        logger.error("Error: %s", err)
        raise

    return file_output_path


if __name__ == '__main__':
    pass
    # file_to_download_url_ = r'https://tefairessurenos.sharepoint.com/sites/SupplyADS/Documentos compartidos/LOGISTICA_ADS/1. PLANILLAS GENERALES 1/Seguimiento tránsitos BsAs-RGA.xlsx'

    # download_sharepoint(SHAREPOINT_EMAIL, SHAREPOINT_PASSWORD, file_to_download_url_)
