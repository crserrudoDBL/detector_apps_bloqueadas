"""Criterio de deteccion de bloqueo y armado del documento de alerta."""
from utils import epoch_ms_to_local_str, utc_now_iso
from datetime import datetime, timezone

def _get_ts_int(s):
    """Convierte el timestamp a entero leyendo la clave nueva o la vieja."""
    # Buscamos la clave nueva primero, si no existe usamos la vieja
    fecha = s.get("timestamp", s.get("ts"))
    
    if isinstance(fecha, int):
        return fecha
    
    dt = datetime.strptime(fecha, "%Y-%m-%dT%H:%M:%S.%fZ")
    return int(dt.replace(tzinfo=timezone.utc).timestamp())

def is_blocked(snapshots, current, now, window_seconds):
    # Usamos la nueva funcion retrocompatible para la resta
    old_enough = [s for s in snapshots if now - _get_ts_int(s) >= window_seconds]
    
    if not old_enough:
        return False
        
    # Usamos la nueva funcion para buscar la foto mas reciente
    ref = max(old_enough, key=_get_ts_int)
    
    p_old, p_new = ref["progress"], current
    return (
        p_old.get("job_id") == p_new.get("job_id")
        and p_old.get("submitted") == p_new.get("submitted")
        and p_old.get("stages_succeeded") == p_new.get("stages_succeeded")
        and p_old.get("tasks_succeeded") == p_new.get("tasks_succeeded")
    )

def build_alert_doc(rm, app, detected_ts):
    proceso = app.get("name", "")
    usuario = app.get("user", "")
    queue = app.get("queue", "")
    porcentaje = round(app.get("clusterUsagePercentage", 0.0), 2)
    enlace = "{}/cluster/app/{}".format(rm, app["id"])

    message = (
        "<b>Aplicacion:</b> {}\n"
        "<b>Usuario:</b> {} | <b>Queue:</b> {}\n"
        "<b>% Uso de cluster:</b> {}%\n"
        "{}"
    ).format(app["id"], usuario, queue, porcentaje, enlace)

    return {
        "application_id": app["id"],
        "proceso": proceso,
        "porcentaje_uso_cluster": porcentaje,
        "usuario": usuario,
        "queue": queue,
        "tiempo_de_inicio": epoch_ms_to_local_str(app["startedTime"]),
        "enlace_a_la_aplicacion": enlace,
        "estado": "BLOQUEADA",
        "processed": False,
        "message": message,
        "config": {"title": "Aplicacion Spark Bloqueada"},
        "@timestamp": utc_now_iso(detected_ts),
    }

