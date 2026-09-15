"""Helpers de fecha/hora usados por el resto de los modulos."""
from datetime import datetime


def epoch_ms_to_local_str(epoch_ms):
    """1783465310897 -> '2026/07/07 20:29:01' (hora local del nodo)."""
    return datetime.fromtimestamp(epoch_ms / 1000).strftime("%Y/%m/%d %H:%M:%S")


def utc_now_iso(epoch_seconds=None):
    """Timestamp en el formato que exige el mapping de Elastic: '2026-07-07T23:47:12.000Z'.

    Con milisegundos siempre presentes (aunque sean .000 cuando se arma desde un epoch en segundos,
    como el "now" del detector) porque el mapping del indice no acepta el formato sin milisegundos.
    """
    dt = datetime.utcfromtimestamp(epoch_seconds) if epoch_seconds is not None else datetime.utcnow()
    return dt.strftime("%Y-%m-%dT%H:%M:%S.") + "{:03d}Z".format(dt.microsecond // 1000)
