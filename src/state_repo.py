"""Repositorios de estado: snapshots de progreso por aplicacion entre corridas del detector.

Un "snapshot" es una foto de {job_id, submitted, stages_succeeded, tasks_succeeded} en un instante.
El detector los compara para decidir si una app esta bloqueada (ver models.is_blocked).

Hay dos backends intercambiables:
  - FileStateRepo:    un JSON en disco. Simple, no requiere Elastic. Usar mientras el indice no exista.
  - ElasticStateRepo: un documento por app en el indice "estados_..." (_id = application_id, patron
                       upsert como sugirio el compa: nunca duplica, y Kibana lo hace debuggeable).
"""
import json
import os

from compat import ABC, abstractmethod, makedirs, replace
from utils import utc_now_iso


class IStateRepo(ABC):
    @abstractmethod
    def load(self, app_ids):
        """Devuelve {app_id: {"snapshots": [...], "alertada": bool}} para los app_ids pedidos."""

    @abstractmethod
    def save(self, app_id, entry):
        """Persiste el estado (snapshots + alertada) de una aplicacion."""

    @abstractmethod
    def prune(self, running_ids):
        """Da de baja el estado de aplicaciones que dejaron de estar RUNNING."""


class FileStateRepo(IStateRepo):
    def __init__(self, path, logger=None):
        self.path = path
        self.logger = logger
        makedirs(os.path.dirname(os.path.abspath(path)))
        self._state = self._load_all()

    def _load_all(self):
        if os.path.exists(self.path):
            try:
                with open(self.path) as f:
                    return json.load(f)
            except Exception as e:
                if self.logger:
                    self.logger.warning("Estado corrupto, se reinicia: {}".format(e))
        return {}

    def load(self, app_ids):
        return {app_id: self._state[app_id] for app_id in app_ids if app_id in self._state}

    def save(self, app_id, entry):
        self._state[app_id] = entry
        self._flush()

    def prune(self, running_ids):
        running = set(running_ids)
        stale = [app_id for app_id in self._state if app_id not in running]
        for app_id in stale:
            del self._state[app_id]
        if stale:
            self._flush()

    def _flush(self):
        tmp = self.path + ".tmp"
        with open(tmp, "w") as f:
            json.dump(self._state, f, indent=2)
        replace(tmp, self.path)


class ElasticStateRepo(IStateRepo):
    """1 documento por aplicacion, _id = application_id (upsert, nunca duplica)."""

    def __init__(self, es_client, index, logger=None):
        self.es = es_client
        self.index = index
        self.logger = logger

    def load(self, app_ids):
        if not app_ids:
            return {}
        resp = self.es.mget(index=self.index, body={"ids": app_ids})
        result = {}
        for doc in resp.get("docs", []):
            if doc.get("found"):
                src = doc["_source"]
                result[doc["_id"]] = {"snapshots": src.get("snapshots", []), "alertada": src.get("alertada", False)}
        return result

    def save(self, app_id, entry):
        body = {
            "application_id": app_id,
            "snapshots": entry["snapshots"],
            "alertada": entry["alertada"],
            "@timestamp": utc_now_iso(),
        }
        self.es.index(index=self.index, id=app_id, document=body)

    def prune(self, running_ids):
        # No hace falta borrar a mano: el indice de estado tiene politica de retencion (30 dias / 25GB)
        # y las apps viven horas, no dias, asi que los documentos huerfanos se purgan solos. Si en algun
        # momento se necesita limpieza inmediata, se puede reemplazar esto por un _delete_by_query
        # excluyendo running_ids, pero agrega una consulta pesada en cada corrida sin necesidad real.
        pass
