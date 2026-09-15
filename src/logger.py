"""
Logger universal para procesos Elastic.
Escribe en {log_prefix}.log y rota diariamente a medianoche a {log_prefix}.log.YYYY-MM-DD.
Mantiene un máximo de 10 archivos de log históricos.
"""
import logging
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path


class ProcessLogger:
    MAX_LOG_FILES = 10

    def __init__(self, log_dir, log_prefix, also_console=False):
        """
        Configura rotación diaria automática y retención a 10 días.
        Soporta 'log_dir' como string o como Path.
        """
        self.logs_dir = Path(log_dir)
        self.logs_dir.mkdir(parents=True, exist_ok=True)
        self.log_prefix = log_prefix

        self._logger = logging.getLogger(log_prefix)
        self._logger.setLevel(logging.INFO)
        self._logger.handlers.clear()

        formatter = logging.Formatter('%(asctime)s - [%(levelname)s] - %(message)s')

        # Archivo de log activo único
        log_file = self.logs_dir / f"{self.log_prefix}.log"

        # Handler nativo con rotación diaria y purga automática de >10 logs
        file_handler = TimedRotatingFileHandler(
            filename=log_file,
            when="midnight",
            interval=1,
            backupCount=self.MAX_LOG_FILES,
            encoding="utf-8"
        )
        file_handler.setFormatter(formatter)

        # Sufijo por defecto de Python (genera .YYYY-MM-DD al rotar)
        file_handler.suffix = "%Y-%m-%d"

        self._logger.addHandler(file_handler)

        if also_console:
            console_handler = logging.StreamHandler()
            console_handler.setFormatter(formatter)
            self._logger.addHandler(console_handler)

    # Métodos estándar de logging
    def info(self, msg: str):
        self._logger.info(msg)

    def warning(self, msg: str):
        self._logger.warning(msg)

    def error(self, msg: str):
        self._logger.error(msg)

    def exception(self, msg: str, e=None):
        if e:
            self._logger.exception(f"{msg}: {e}")
        else:
            self._logger.exception(msg)
