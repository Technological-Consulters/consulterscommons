"""
    Módulo para estandarizar los nombres de las columnas de una tabla SQL.
"""

import re
import string
import unicodedata

def standardize_sql_column_names(original_col_names: list, remove_punct: bool = True, remove_accents: bool = True) -> list:
    """
    Estandariza los nombres de las columnas eliminando la puntuación, normalizando los caracteres unicode,
    convirtiéndolos a mayúsculas, reemplazando los espacios por guiones bajos y eliminando guiones bajos consecutivos.

    Args:
        original_col_names (list): Una lista de nombres de columnas originales.
        remove_punct (bool, opcional): Bandera para indicar si se debe eliminar la puntuación. Por defecto es True.
            Ejemplo: 
                Si es True, la columna 'Producción' se estandarizará como 'PRODUCCION'.
                Si es False, la columna 'Producción' se estandarizará como 'PRODUCCIÓN'.

    Returns:
        list: Una lista de nombres de columnas estandarizados.
    
    Ejemplo:
    ```python
        original_col_names = ['Fasón', '''Ho?a sóy
                               u=a c/lumn4''', 'CoLuMN~W1th!-Spé&ciál-Cháractérs   ', '''Ánother-Çolumn
                               ''', 'Yét_Another-Çolumn']
        standardized_col_names = standardise_column_names(original_col_names)
        print(standardized_col_names)
        # Salida: ['FASON', 'HOYA_SOY_U_A_C_LUMN4', 'COLUMN_W1TH_SPECIAL_CHARACTERS', 'ANOTHER_COLUMN', 'YET_ANOTHER_COLUMN']
    ```
    """

    # Punctuation contiene '!"#$%&\'()*+,-./:;<=>?@[\\]^_`{|}~'
    # Extiendo la lista con otros caracteres especiales
    punctuation = string.punctuation + '¡¿' + '“”‘’' + '´¨' + '°'

    translator = str.maketrans(punctuation, ' ' * len(punctuation))
    new_col_names = []

    if not remove_punct:
        print('Advertencia: No se eliminó la puntuación de los nombres de las columnas. Se recomienda hacerlo para evitar problemas de codificación.')

    if not remove_accents:
        print('Advertencia: No se eliminaron los acentos de los nombres de las columnas. Se recomienda hacerlo para evitar problemas de codificación.')

    for c in original_col_names:
        # Eliminar saltos de linea
        c_new = c.replace('\n', '').strip()
        # Eliminar acentos
        if remove_accents:
            c_new = ''.join(char for char in unicodedata.normalize('NFKD', c_new) if not unicodedata.combining(char))
        # Convertir a mayúsculas
        c_new = c_new.upper()

        # Eliminar puntuación, por ejemplo ['.', ',', '!', '?', '-', '_', '(', ')', '[', ']', '{', '}', ':', ';']
        if remove_punct:
            c_new = c_new.translate(translator)

        # Reemplazar espacios por guiones bajos
        c_new = '_'.join(c_new.split(' '))

        # Eliminar guiones bajos consecutivos
        if c_new[-1] == '_':
            c_new = c_new[:-1]
        c_new = re.sub(r'\_+', '_', c_new)

        new_col_names.append(c_new)

    return new_col_names

if __name__ == '__main__':
    pass
    # Casos de prueba
    # original_col_names = ['Ramón', '''Ho?a sóy
    #                        u=a c/lumn4''', 'CoLuMN~W1th!-Spé&ciál-Cháractérs   ', '''Ánother-Çolumn
    #                        ''', 'Yét_Another-Çolumn']
    # standardized_col_names = standardise_column_names(original_col_names)
    # print(standardized_col_names)
