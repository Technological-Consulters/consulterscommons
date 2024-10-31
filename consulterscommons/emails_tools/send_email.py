"""
Módulo para enviar correos electrónicos utilizando SMTP.

    TODO:
        - Implementar clase Email para manejar la conexión al servidor SMTP y desacoplarlo de la función send_email.
"""

import os
import smtplib
from typing import Union
import email
from email import encoders
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase

from prefect import task
from prefect.blocks.system import Secret
from prefect.variables import Variable

from consulterscommons.log_tools.prefect_log_config import PrefectLogger


logger_global = PrefectLogger(__file__)


# async def get_credenciales():
#     mail_username = await Variable.get('alertas_email')
#     mail_server = await Variable.get('alertas_email_sv')
#     mail_port = 587  # Esto esta hardcodeado pero se puede implementar como una variable en caso que cambie
#     secret_block_alertas = Secret.load("alertas-email-pass")
#     mail_password = await secret_block_alertas.get()

#     return mail_username, mail_server, mail_port, mail_password


# def sync_get_credenciales():
#     return asyncio.run(get_credenciales())


@task(retries=1, retry_delay_seconds=15)
def send_email(mail_to: Union[str, list[str]], subject: str, body: str, attachment_path: str = None, is_html: bool = True):
    """
    Conecta al servidor SMTP usando constantes y envía un correo electrónico.

    Args:
        mail_to (str or list[str]): Dirección(es) de correo electrónico del destinatario.
        subject (str): Asunto del correo.
        body (str): Cuerpo del correo en formato HTML.
        attachment_path (str, optional): Ruta del archivo adjunto, si se proporciona.
        is_html (bool, optional): Indica si el cuerpo del correo es HTML. Por defecto es True.

    Raises:
        smtplib.SMTPException: Se produce si hay un error en la conexión o envío del correo.
        FileNotFoundError: Se produce si no se encuentra el archivo adjunto especificado.
        email.errors.MessageError: Se produce si hay un error en la estructura del mensaje de correo.
    """

    logger = logger_global.obtener_logger_prefect()

    if isinstance(mail_to, list):
        mail_to = ", ".join(mail_to)

    # Configurar el mensaje de correo
    mail_username = Variable.get('alertas_email')
    mail_server = Variable.get('alertas_email_sv')
    mail_port = 587  # Esto esta hardcodeado pero se puede implementar como una variable en caso que cambie
    secret_block_alertas = Secret.load("alertas-email-pass")
    mail_password = secret_block_alertas.get()

    mimemsg = MIMEMultipart()
    mimemsg['From'] = mail_username
    mimemsg['To'] = mail_to
    mimemsg['Subject'] = subject
    if is_html:
        mimemsg.attach(MIMEText(body, 'html'))
    else:
        mimemsg.attach(MIMEText(body, 'plain'))

    if attachment_path:
        with open(attachment_path, 'rb') as attachment:
            mimefile = MIMEBase('application', 'octet-stream')
            mimefile.set_payload(attachment.read())
            encoders.encode_base64(mimefile)
            mimefile.add_header('Content-Disposition',
                                f"attachment; filename={os.path.basename(attachment_path)}")
            mimemsg.attach(mimefile)

    # Configurar el servidor de correo y enviar el mensaje
    try:
        mail_server = smtplib.SMTP(host=mail_server, port=mail_port)
        mail_server.starttls()
        mail_server.login(mail_username, mail_password)
        mail_server.send_message(mimemsg)
        mail_server.quit()
        logger.info("Mail enviado. Subject: %s", subject)
        logger.info("Destinatario: %s", mail_to)
    except email.errors.MessageError as error_email:
        logger.error("Error en estructura del correo: %s", error_email)
    except smtplib.SMTPException as error_smpt:
        logger.error("Error al enviar el correo: %s", error_smpt)
