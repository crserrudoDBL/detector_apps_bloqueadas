# Detector de aplicaciones Spark bloqueadas en YARN (GDPC-2720)

Etapa: **DETECCION**. No mata nada, solo detecta aplicaciones Spark que dejaron de avanzar y emite
un documento de alerta. Pensado para correr cada 15 minutos via cron en un nodo del cluster.

## Requisitos

- **python3** (el codigo usa f-strings; confirmar que el nodo lo tenga instalado y que el cron
  invoque `python3` explicitamente, no `python`).
- `pip install -r requirements.txt` (requests + elasticsearch). Si el nodo no tiene salida a
  internet, instalar desde el repo interno o via wheels locales.

## Estructura

```
conf/
  prod.json           # RMs de prod, indices de Elastic, umbrales
  dev.json             # idem para el entorno de dev
src/
  main.py               # entry point (CLI + wiring de dependencias)
  yarn_client.py         # llamadas a la API REST del RM y al AM de Spark (via proxy)
  state_repo.py          # snapshots de progreso entre corridas: backend file o elastic
  alert_repo.py          # sink del documento de alerta: consola (dry-run) o elastic
  models.py               # criterio de bloqueo + armado del documento de alerta
  detector.py             # orquesta una corrida completa
  logger.py               # logging a archivo (+ consola con --verbose)
log/                     # logs rotados por dia (gitignored)
```

## Como funciona

1. `yarn_client.get_active_rm()` prueba cada RM de la config y devuelve el que esta ACTIVE (maneja HA).
2. `GET /ws/v1/cluster/apps?states=RUNNING&applicationTypes=SPARK`, filtra las que llevan mas de
   `min_elapsed_minutes` corriendo (candidatas).
3. Para cada candidata, `GET /proxy/{app_id}/api/v1/applications/{app_id}/jobs` (la misma data que
   la pantalla "Spark Jobs" del ApplicationMaster) y se extrae del job activo: `submissionTime`,
   Stages succeeded/total, Tasks succeeded/total. Si no hay ningun job `RUNNING` (el driver puede
   estar entre jobs, o haciendo trabajo que Spark no reporta, ej. listing de paths), se usan los
   totales acumulados de todos los jobs como fallback.
4. Se compara contra el snapshot guardado hace >= `blocked_window_minutes` (state repo). Si
   `job_id` + `submitted` + `stages_succeeded` + `tasks_succeeded` **no cambiaron** en esa ventana,
   la app esta **BLOQUEADA**.
5. Se emite el documento (consola en `--dry-run`, o al indice de alertas en Elastic).

### Por que Stages/Tasks *succeeded* y no los totales

Los totales pueden moverse por reintentos de tareas fallidas sin que la app avance realmente
(ej. `1306/1507 (10 failed)`). Lo que define avance real es que crezcan las *finalizadas*, que es
el criterio pedido en el ticket. Por el mismo motivo no se compara `stages_total`/`tasks_total` ni
el motivo de falla ni un tiempo estimado de duracion (fuera de alcance de esta etapa).

### Por que alcanza con comparar dos puntos, no toda la ventana

`stages_succeeded` y `tasks_succeeded` son contadores monotonos no decrecientes. Si el snapshot mas
cercano al borde de la ventana (`>= blocked_window_minutes` atras) coincide con el actual, todo lo
que hubo en el medio necesariamente coincidio tambien. No hace falta guardar ni comparar mas que
esos dos puntos.

## Los dos indices de Elastic

| Indice | Contenido | `_id` |
|---|---|---|
| `apps_bloqueadas_spark` | 1 documento por app detectada como bloqueada (el JSON del ticket) | `application_id` |
| `estados_apps_bloqueadas_spark` | 1 documento por app candidata con sus snapshots de progreso + flag `alertada` | `application_id` |

Ambos usan `_id = application_id` (como sugirio tu companero): cada corrida hace `index()` (PUT),
que es upsert por definicion — nunca duplica, y la ultima escritura pisa a la anterior. Esto asume
que son **indices comunes con politica de borrado**, no *data streams* (los data streams solo
aceptan `_op_type: create` y no permiten fijar `_id` propio ni hacer upsert). Confirmalo una vez con:

```bash
curl -u user:pass -X PUT "$ELASTIC_URL/estados_apps_bloqueadas_spark/_doc/test1" \
  -H 'Content-Type: application/json' -d '{"a":1}'
# repetir el mismo comando: si la primera vez responde "created" y la segunda "updated", esta bien.
```

Sobre la retencion (30 dias / 25GB): para el indice de **estado** es intrascendente (las apps viven
horas, no dias) y ademas evita tener que borrar a mano los documentos de apps ya terminadas —
`ElasticStateRepo.prune()` es un no-op justamente por eso. Para el indice de **alertas** implica que
el historico se pierde al mes; aceptable para operar, pero tenelo presente si mas adelante quieren
metricas de tendencia (exportar antes de que expire).

## Uso

```bash
# Prueba local sin tocar Elastic (imprime el JSON por consola)
python3 src/main.py --conf conf/dev.json --dry-run --verbose

# Prueba con el estado ya viviendo en Elastic, alertas todavia por consola
python3 src/main.py --conf conf/dev.json --dry-run --verbose --state-backend elastic

# Produccion: estado y alertas en Elastic
python3 src/main.py --conf conf/prod.json --state-backend elastic
```

Credenciales de Elastic **nunca hardcodeadas**, por variable de entorno:

```bash
export DETECTOR_ES_USER=...
export DETECTOR_ES_PASSWORD=...
```

Flags utiles para probar sin esperar 1 hora real (bajan los umbrales, no tocan la config):

```bash
python3 src/main.py --conf conf/dev.json --dry-run --verbose \
  --rm http://edh-master-02d.root.corp:8088 \
  --min-elapsed-min 2 --blocked-window-min 6
```

## Cron (cada 15 minutos)

```
*/15 * * * * DETECTOR_ES_USER=... DETECTOR_ES_PASSWORD=... \
  /usr/bin/python3 /opt/detector_apps_bloqueadas/src/main.py \
  --conf /opt/detector_apps_bloqueadas/conf/prod.json --state-backend elastic \
  >> /opt/detector_apps_bloqueadas/log/cron.log 2>&1
```

## Pendiente antes de prod

- Confirmar que ambos indices existen como indices comunes (no data streams) — ver curl arriba.
- Si las UIs del cluster estan kerberizadas, agregar `requests-kerberos` y un `kinit` previo en el cron.
- Soak test en dev con `--state-backend elastic` un rato largo antes de sacar `--dry-run` en prod.
