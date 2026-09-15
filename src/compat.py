"""Compatibilidad Python 2.7 / 3.

El nodo de prod corre Python 2.7.5 sin python3 instalado (2.7 esta EOL desde 2020: si en algun
momento se instala python3 en el nodo, este modulo deja de hacer falta y se puede borrar sin tocar
el resto del codigo, que ya usa .format() en vez de f-strings justamente por esto).
"""
import os
from abc import ABCMeta, abstractmethod  # noqa: F401 (abstractmethod se re-exporta para los que importen de aca)

# abc.ABC no existe en Python 2, solo ABCMeta. Esto es exactamente como esta definido ABC en Python 3.4+.
ABC = ABCMeta(str("ABC"), (object,), {})


def makedirs(path):
    """os.makedirs sin pisar el directorio si ya existe (el kwarg exist_ok no existe en Python 2)."""
    if not os.path.isdir(path):
        try:
            os.makedirs(path)
        except OSError:
            if not os.path.isdir(path):
                raise


def replace(src, dst):
    """os.replace no existe en Python 2; en POSIX os.rename ya sobreescribe el destino atomicamente."""
    os.rename(src, dst)
