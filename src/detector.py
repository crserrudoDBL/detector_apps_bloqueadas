"""Orquesta una corrida de deteccion: YARN -> estado previo -> criterio -> sink -> estado nuevo."""
import time

# IMPORTAMOS _get_ts_int desde models.py
from models import is_blocked, build_alert_doc, _get_ts_int
from utils import utc_now_iso

def run(yarn_client, state_repo, alert_sink, logger, min_elapsed_ms, blocked_window_seconds,
        state_retention_seconds, dry_run):
    now = int(time.time())
    rm = yarn_client.get_active_rm()
    apps = yarn_client.get_running_spark_apps(rm, min_elapsed_ms)
    running_ids = [a["id"] for a in apps]

    state = state_repo.load(running_ids)
    bloqueadas = []

    for app in apps:
        app_id = app["id"]
        progress = yarn_client.get_spark_progress(rm, app_id)
        if progress is None:
            continue

        entry = state.get(app_id, {"snapshots": [], "alertada": False})
        snapshots = entry["snapshots"]

        logger.info(
            "[{}] criterio={} job={} submitted={} stages={}/{} tasks={}/{}".format(
                app_id, progress["criterio"], progress["job_id"], progress["submitted"],
                progress["stages_succeeded"], progress["stages_total"],
                progress["tasks_succeeded"], progress["tasks_total"],
            )
        )

        if is_blocked(snapshots, progress, now, blocked_window_seconds):
            bloqueadas.append(app_id)
            if dry_run or not entry["alertada"]:
                alert_sink.emit(build_alert_doc(rm, app, now))
                entry["alertada"] = True
            else:
                logger.info("[{}] sigue bloqueada (ya alertada)".format(app_id))
        else:
            if entry["alertada"]:
                logger.info("[{}] volvio a avanzar, se resetea alerta".format(app_id))
            entry["alertada"] = False

        # Guardamos la nueva foto asegurando que vaya en formato ISO
        snapshots.append({"timestamp": utc_now_iso(now), "progress": progress})
        # EL FIX: Usamos _get_ts_int(s) en lugar de s["ts"] para soportar texto y numero
        snapshots[:] = [s for s in snapshots if now - _get_ts_int(s) <= state_retention_seconds]

        state_repo.save(app_id, entry)

    state_repo.prune(running_ids)

    if bloqueadas:
        logger.warning("Aplicaciones BLOQUEADAS detectadas: {}".format(", ".join(bloqueadas)))
    else:
        logger.info("Sin aplicaciones bloqueadas en esta corrida.")
