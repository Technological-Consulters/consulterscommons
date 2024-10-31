"""
Modulo para desencriptar archivos de Microsoft Office.
"""

import os
import shutil
import msoffcrypto


def decrypt_msfile(file_path: str, decrypt_password: str):
    file_name = os.path.basename(file_path)
    directory = os.path.dirname(file_path)

    # Copiar para desencriptar el archivo nuevo
    aux_path = os.path.join(directory, 'decrypt_' + file_name)
    shutil.copy(file_path, aux_path)

    # Desencripta el archivo nuevo
    with open(file_path, "rb") as fp_read, open(aux_path, "wb") as fp_write:
        msf = msoffcrypto.OfficeFile(fp_read)
        msf.load_key(password=decrypt_password)
        msf.decrypt(fp_write)

    return aux_path
