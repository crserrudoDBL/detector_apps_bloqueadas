"""Sinks para el documento de alerta: consola (--dry-run) o Elasticsearch.

La deteccion (detector.py) no sabe adonde va el documento, solo llama a sink.emit(doc).
Asi el dia que se quiera cambiar el destino no se toca la logica de deteccion.
"""
import json

from compat import ABC, abstractmethod


class IAlertSink(ABC):
    @abstractmethod
    def emit(self, doc):
        pass


class ConsoleAlertSink(IAlertSink):
    def emit(self, doc):
        print(json.dumps(doc, indent=2, ensure_ascii=False))


class ElasticAlertSink(IAlertSink):
    """1 documento por app bloqueada en el indice de alertas, _id = application_id (upsert, nunca duplica)."""

    def __init__(self, es_client, index, logger=None):
        self.es = es_client
        self.index = index
        self.logger = logger

    def emit(self, doc):
        try:
            self.es.index(index=self.index, id=doc["application_id"], document=doc)
            if self.logger:
                self.logger.info("[{}] indexado en Elastic ({})".format(doc["application_id"], self.index))
        except Exception as e:
            if self.logger:
                self.logger.error("[{}] error indexando en Elastic: {}".format(doc["application_id"], e))
