"""
Módulo para descargar un archivo de Sharepoint.
Utiliza Prefect para el manejo de logs y keyring para la obtención de contraseñas.
"""

import os

from office365.runtime.auth.user_credential import UserCredential
from office365.sharepoint.client_context import ClientContext
from prefect import task

from consulterscommons.log_tools.prefect_log_config import PrefectLogger

logger_global = PrefectLogger(__file__)


@task(retries=2, retry_delay_seconds=5)
def upload_sharepoint(sharepoint_email: str,
                      sharepoint_password: str,
                      file_to_upload_path: str,
                      site_url: str,
                      sharepoint_folder_url: str,
                      file_name: str = None) -> None:
    """
    Sube un archivo a una carpeta de SharePoint.

    Args:
        - sharepoint_email (str): Correo electrónico de SharePoint.
        - sharepoint_password (str): Contraseña de SharePoint.
        - file_to_upload_path (str): Ruta del archivo a subir.
        - site_url (str): URL del sitio de SharePoint.
        - sharepoint_folder_url (str): URL de la carpeta en SharePoint donde se cargará el archivo.
        - file_name (str, optional): Nombre del archivo en SharePoint. Si no se especifica, se usará el nombre del archivo local.

    Returns:
        - None

    Raises:
        - Exception: Si ocurre un error al subir el archivo.
    """

    logger = logger_global.obtener_logger_prefect()

    try:
        # Verificar que el archivo existe
        if not os.path.isfile(file_to_upload_path):
            raise FileNotFoundError(f"El archivo {file_to_upload_path} no existe.")

        # Definir nombre de archivo en SharePoint
        if file_name is None:
            file_name = os.path.basename(file_to_upload_path)

        user_credentials = UserCredential(sharepoint_email, sharepoint_password)

        # Crear contexto del cliente
        ctx = ClientContext(site_url).with_credentials(user_credentials)

        # Subir archivo
        target_folder = ctx.web.get_folder_by_server_relative_url(sharepoint_folder_url)
        with open(file_to_upload_path, 'rb') as file:
            file_content = file.read()
            target_folder.upload_file(file_name, file_content).execute_query()

        logger.info("Archivo '%s' subido a SharePoint en '%s'",
                    file_name, sharepoint_folder_url)

    except Exception as err:
        logger.error('Ocurrió un error al subir el archivo a SharePoint')
        logger.error("Error: %s", err)
        raise


if __name__ == '__main__':
    pass
    # upload_sharepoint(email, password
    #                 r'C:\Reportes_Power_BI\Python\PRODUCCIÓN\Ciberbrain\files\Seriales Faltantes en Ciberbrain.csv',
    #                 'https://tefairessurenos.sharepoint.com/sites/Ciberbrain',
    #                 '/sites/Ciberbrain/Documentos%20Compartidos/Discrepancias'
    #                 )
