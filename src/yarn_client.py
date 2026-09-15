"""Cliente HTTP para la API REST del ResourceManager y del Spark ApplicationMaster (via proxy del RM)."""
import requests


class YarnClient:
    def __init__(self, resource_managers, timeout=15, logger=None):
        self.resource_managers = resource_managers
        self.timeout = timeout
        self.logger = logger
        self.session = requests.Session()
        self.session.trust_env = False

    def get_active_rm(self):
        """Prueba cada RM configurado y devuelve el primero que responda como ACTIVE (maneja HA)."""
        for rm in self.resource_managers:
            try:
                r = self.session.get("{}/ws/v1/cluster/info".format(rm), timeout=self.timeout)
                r.raise_for_status()
                info = r.json().get("clusterInfo", {})
                if info.get("haState", "ACTIVE").upper() == "ACTIVE":
                    if self.logger:
                        self.logger.info("RM activo: {}".format(rm))
                    return rm
            except Exception as e:
                if self.logger:
                    self.logger.warning("RM {} no responde o no es activo: {}".format(rm, e))
        raise RuntimeError("Ningun ResourceManager activo encontrado")

    def get_running_spark_apps(self, rm, min_elapsed_ms):
        """Apps SPARK en RUNNING con mas de min_elapsed_ms de ejecucion (candidatas a bloqueo)."""
        url = "{}/ws/v1/cluster/apps".format(rm)
        params = {"states": "RUNNING", "applicationTypes": "SPARK"}
        r = self.session.get(url, params=params, timeout=self.timeout)
        r.raise_for_status()
        apps = (r.json().get("apps") or {}).get("app") or []
        candidatas = [a for a in apps if a.get("elapsedTime", 0) > min_elapsed_ms]
        if self.logger:
            self.logger.info("Apps SPARK RUNNING: {} | candidatas (> umbral): {}".format(len(apps), len(candidatas)))
        return candidatas

    def get_spark_progress(self, rm, app_id):
        """Progreso del job activo del AM de Spark: Submitted, Stages y Tasks succeeded/total.

        Si no hay ningun job en estado RUNNING (el driver puede estar entre jobs, o haciendo trabajo
        que Spark no reporta como job, ej. listing de paths), devuelve los totales acumulados de todos
        los jobs como fallback: si esos totales tampoco se mueven en la ventana, tambien es indicio de
        bloqueo (criterio "sin_job_activo").
        """
        url = "{}/proxy/{}/api/v1/applications/{}/jobs".format(rm, app_id, app_id)
        try:
            r = self.session.get(url, timeout=self.timeout, allow_redirects=True)
            r.raise_for_status()
            jobs = r.json()
        except Exception as e:
            if self.logger:
                self.logger.warning("[{}] no se pudo consultar el AM de Spark: {}".format(app_id, e))
            return None

        running = [j for j in jobs if j.get("status") == "RUNNING"]
        if not running:
            return {
                "criterio": "sin_job_activo",
                "job_id": None,
                "submitted": None,
                "stages_succeeded": sum(j.get("numCompletedStages", 0) for j in jobs),
                "stages_total": sum(len(j.get("stageIds", [])) for j in jobs),
                "tasks_succeeded": sum(j.get("numCompletedTasks", 0) for j in jobs),
                "tasks_total": sum(j.get("numTasks", 0) for j in jobs),
            }

        job = min(running, key=lambda j: j.get("jobId", 0))
        return {
            "criterio": "job_activo_sin_avance",
            "job_id": job.get("jobId"),
            "submitted": job.get("submissionTime"),
            "stages_succeeded": job.get("numCompletedStages", 0),
            "stages_total": len(job.get("stageIds", [])),
            "tasks_succeeded": job.get("numCompletedTasks", 0),
            "tasks_total": job.get("numTasks", 0),
        }
