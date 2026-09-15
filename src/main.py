#!/usr/bin/env python3
"""Detector de aplicaciones Spark bloqueadas en YARN.

Etapa: DETECCION (no mata nada, solo detecta y emite el documento de alerta).
Pensado para correr cada 15 minutos via cron en un nodo del cluster.

Requiere python3 (usa f-strings/type hints en el resto de los modulos, igual que el resto del equipo).

Uso:
  python3 main.py --dry-run --verbose                              # prueba local, sin tocar Elastic
  python3 main.py --dry-run --state-backend elastic                # prueba con estado ya en Elastic
  python3 main.py --conf ../conf/prod.json                         # produccion: alerta a Elastic

Credenciales de Elastic: DETECTOR_ES_USER / DETECTOR_ES_PASSWORD por variable de entorno si estan
seteadas; si no, usa el usuario compartido dblandit/dblandit por default (ver build_es_client).
"""
import argparse
import json
import os
import sys

from logger import ProcessLogger
from yarn_client import YarnClient
from state_repo import FileStateRepo, ElasticStateRepo
from alert_repo import ConsoleAlertSink, ElasticAlertSink
import detector

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_CONF = os.path.join(BASE_DIR, "..", "conf", "prod.json")
DEFAULT_LOG_DIR = os.path.join(BASE_DIR, "..", "log")
DEFAULT_STATE_FILE = "/var/tmp/detector_apps_bloqueadas/state.json"


def build_es_client(conf):
    from elasticsearch import Elasticsearch

    # Si DETECTOR_ES_USER/DETECTOR_ES_PASSWORD estan seteadas se usan esas; si no, cae al usuario
    # compartido (mismo que usa el resto del equipo) para no bloquear pruebas rapidas.
    user = os.environ.get("DETECTOR_ES_USER") or "dblandit"
    password = os.environ.get("DETECTOR_ES_PASSWORD") or "dblandit"

    return Elasticsearch(
        [conf["elastic"]["url"]],
        http_auth=(user, password),
        verify_certs=conf["elastic"].get("verify_ssl", False),
    )


def parse_args():
    p = argparse.ArgumentParser(description="Detector de apps Spark bloqueadas en YARN")
    p.add_argument("--conf", default=DEFAULT_CONF, help="Archivo de configuracion JSON (default: conf/prod.json)")
    p.add_argument("--dry-run", action="store_true", help="No indexa alertas en Elastic: las imprime por consola")
    p.add_argument("--verbose", action="store_true", help="Log detallado tambien por consola")
    p.add_argument("--state-backend", choices=["file", "elastic"], default="file",
                    help="Donde persistir los snapshots de progreso entre corridas (default: file)")
    p.add_argument("--state-file", default=DEFAULT_STATE_FILE,
                    help="Ruta del archivo de estado (solo con --state-backend file)")
    p.add_argument("--rm", action="append", metavar="URL",
                    help="URL de ResourceManager (repetible). Sobreescribe la config, util para dev.")
    p.add_argument("--min-elapsed-min", type=int, default=None, metavar="MIN",
                    help="Minutos minimos de ejecucion para considerar candidata. Sobreescribe la config.")
    p.add_argument("--blocked-window-min", type=int, default=None, metavar="MIN",
                    help="Minutos sin avance para declarar bloqueada. Sobreescribe la config.")
    return p.parse_args()


def main():
    args = parse_args()

    with open(args.conf) as f:
        conf = json.load(f)

    resource_managers = args.rm or conf["resource_managers"]
    min_elapsed_min = args.min_elapsed_min if args.min_elapsed_min is not None else conf.get("min_elapsed_minutes", 60)
    blocked_window_min = args.blocked_window_min if args.blocked_window_min is not None else conf.get("blocked_window_minutes", 60)
    state_retention_hours = conf.get("state_retention_hours", 3)

    logger = ProcessLogger(log_dir=DEFAULT_LOG_DIR, log_prefix="detector_apps_bloqueadas", also_console=args.verbose)

    logger.info("Modo: {} | state-backend={}".format("DRY-RUN" if args.dry_run else "PRODUCCION", args.state_backend))
    logger.info("Config: RMs={} | min_elapsed={}min | blocked_window={}min".format(
        resource_managers, min_elapsed_min, blocked_window_min))

    yarn_client = YarnClient(resource_managers, logger=logger)

    es_client = None
    if args.state_backend == "elastic" or not args.dry_run:
        es_client = build_es_client(conf)

    if args.state_backend == "elastic":
        state_repo = ElasticStateRepo(es_client, conf["indices"]["state"], logger=logger)
    else:
        state_repo = FileStateRepo(args.state_file, logger=logger)

    alert_sink = ConsoleAlertSink() if args.dry_run else ElasticAlertSink(es_client, conf["indices"]["alerts"], logger=logger)

    try:
        detector.run(
            yarn_client=yarn_client,
            state_repo=state_repo,
            alert_sink=alert_sink,
            logger=logger,
            min_elapsed_ms=min_elapsed_min * 60 * 1000,
            blocked_window_seconds=blocked_window_min * 60,
            state_retention_seconds=state_retention_hours * 3600,
            dry_run=args.dry_run,
        )
    except Exception:
        logger.exception("Fallo en la ejecucion del detector")
        sys.exit(1)


if __name__ == "__main__":
    main()
