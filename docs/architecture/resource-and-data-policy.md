# Política de recursos, datos y almacenamiento

Estado: `accepted` para los contratos implementados. Los defaults son conservadores y ajustables;
los techos absolutos son defensas contra inputs hostiles, no objetivos de rendimiento ni prueba de
compatibilidad con hardware.

## Principios vigentes

- Un frame raw es efímero y no se serializa como observación.
- Las observaciones estructuradas pueden permanecer solo en RAM o persistirse de forma opcional.
- La persistencia visual está desactivada por contrato: SQLite rechaza registros declarados
  `persistence_class="visual"`. El productor es responsable de clasificar; no existe todavía
  detección semántica que corrija una etiqueta falsa.
- Toda cola propia tiene límite explícito por capacidad, count o bytes.
- Medición, decisión y enforcement permanecen separados.
- Ningún estado `validated` autoriza afirmar desgaste cero, privacidad forense o aptitud productiva.

## Carriles de datos actuales

| Carril | Contenido | Retención controlada por Viskium |
|---|---|---|
| Captura raw | `BackendFrame` y `FrameEnvelope` como `bytes` | Stack, IPC en memoria y `LatestFrameSlot` de capacidad uno |
| Snapshot one-shot | Un `SnapshotEnvelope` PNG | Solo durante la llamada y su respuesta; el provider no lo cachea |
| Estado latest | Una observación estructurada | `LatestObservationSlot` de capacidad uno hasta reemplazo o cierre |
| Cola de escritura | Observaciones canónicas | Count y bytes configurables; 256 items y 1 MiB por defecto |
| Store semántico | Observaciones seleccionadas con TTL | SQLite opcional, límites configurables, purga manual y recuperación vencida bajo presión |
| Control local | Marcador del data root y grant de consentimiento | Archivos pequeños, versionados y acotados |
| Métricas | Contadores y reason codes sin payload | RAM; no existe persistencia automática de métricas |

## Frames y snapshots efímeros

En captura continua, el controller conserva raw frames solo durante la llamada al backend o en el
slot latest de capacidad uno. Reemplazar o cerrar el slot libera su referencia. El scheduler consume
el frame y no lo incluye en la observación.

El límite de un slot no equivale a “un solo frame en todo el proceso”: el driver, OpenCV, el proceso
hijo, el canal IPC y el consumidor pueden mantener copias transitorias. La memoria real se debe medir
con RSS y un perfil de hardware.

En el camino one-shot, cada solicitud admitida crea un backend, abre, descarta warmup, lee un target,
codifica un PNG en memoria y cierra en `finally`. El provider no guarda frame, backend ni PNG después
de devolver. El backend OpenCV importa `cv2` en un proceso hijo y transfiere BGR por memoria; ante
timeout intenta terminate y luego kill.

El PNG entregado deja de estar bajo control de Viskium: un cliente MCP, el sistema operativo o una
herramienta externa puede copiarlo o persistirlo. Tampoco se promete ausencia forense en pagefile,
crash dumps, memoria del driver o compositor.

## Observaciones y persistencia opcional

El comportamiento implementado es:

- Sin `store` ni `writer`, `LiveScheduler` publica latest-only y cuenta `persistence_skipped`.
- Con store directo, `put()` ocurre en el worker del scheduler.
- Con `ObservationWriter`, `submit()` es no bloqueante y la cola rechaza al alcanzar count o bytes.
- Un resultado `queued` no significa durabilidad; el receipt del store conserva esa distinción.
- El modo actual es best-effort acotado. Un modo `required` que detenga la sesión por falta de
  durabilidad aún no está implementado.

Los bytes del writer son el tamaño canónico de cada observación, no una medición del heap. El límite
no incluye overhead de objetos Python ni la observación que ya está `in_flight`; ambas cosas se deben
vigilar con RSS y las métricas separadas de pending/in-flight.

Antes de persistir, el scheduler limita el tamaño canónico y consulta admission. SQLite además exige
TTL, rechaza registros declarados `prohibited` y `visual`, aplica idempotencia y revisa sus cuotas. El consentimiento de
lectura para agentes es una frontera distinta: no sustituye una futura política de consentimiento o
retención para productores de observaciones sensibles.

## SQLite implementado

`SQLiteStore` es single-owner y usa rollback journal `DELETE`, no WAL.

Defaults actuales:

| Límite | Default |
|---|---:|
| Filas | 100 000 |
| Archivo DB mediante `max_page_count` | 192 MiB |
| Bytes lógicos de documentos | 128 MiB |
| Observación canónica | 64 KiB |
| Reserva libre del volumen por write | 512 MiB |
| `busy_timeout` | 250 ms |
| Query | 256 filas y 1 MiB |
| Purga manual o por presión | 512 filas por transacción |

Política de conexión:

- `journal_mode=DELETE`, `synchronous=NORMAL`, `temp_store=MEMORY`.
- `auto_vacuum=NONE`, `secure_delete=FAST`, `mmap_size=0`.
- Attached databases desactivadas y límites SQLite reducidos.
- Sin `VACUUM`, retry loop, retención periódica/background ni mutación de cuarentena.
- Query y purga manual están acotados. Solo cuando `max_rows` o
  `max_logical_bytes` bloquearían una escritura, el mismo transaction intenta recuperar un lote
  acotado de filas ya expiradas; nunca elimina filas frescas.
- `SQLITE_FULL`, I/O, corrupción y errores read-only ejecutan rollback y enclavan el store en
  `read_only`. `BUSY`/`LOCKED` falla la operación sin envenenar el store.

`max_page_count` limita el archivo principal, no todo el conjunto físico. `footprint()` reporta DB,
journal, WAL y SHM, pero todavía no existe un enforcer externo que sume journal, temporales, logs y
otros artifacts bajo una cuota total. `secure_delete=FAST` tampoco es una garantía de borrado
forense ni de menor desgaste.

## Data root y escrituras controladas

La precedencia implementada es:

```text
CLI explícita
→ VISKIUM_DATA_ROOT
→ storage.root en config
→ directorio local de plataforma
```

Resolver config o ejecutar `doctor` no crea la raíz. `storage init` la inicializa explícitamente con
un UUID, el marcador `.viskium-root.json` y estas categorías:

```text
data-root/
├── .viskium-root.json
├── state/
├── observations/
├── models/
├── runs/
├── logs/
├── cache/
├── tmp/
├── locks/
└── quarantine/
```

La verificación rechaza raíces de filesystem, home, repositorios, rutas remotas y links/reparse points
en paths poseídos. No repara silenciosamente un layout. Mover la raíz a otra letra de unidad protege
capacidad lógica de la unidad original, pero no prueba que sea otro dispositivo físico ni evita
escrituras del sistema operativo fuera de la raíz.

El grant de agente se guarda en `state/agent-consent.json`, con máximo 16 KiB. Contiene id público,
scopes, expiración, sensitivity ceiling, cuota y uso; no contiene bearer token. Las mutaciones usan
reemplazo atómico y un lock acotado. Cada intento de snapshot reserva cuota antes de tocar el
provider, incluso si la captura falla. Un grant dura como máximo siete días y admite como máximo
1024 intentos de snapshot; son techos configurables hacia abajo, no defaults de consumo.

## Recursos y admission

La gobernanza implementada se divide así:

- `ResourceSampler`: obtiene RSS, memoria disponible y espacio libre mediante probes read-only.
- `BudgetPolicy`: decide de forma pura `allow_capture`, `allow_processing` y `allow_persistence`.
- `ResourceAdmissionGate`: cachea el snapshot de recursos 250 ms por defecto y reaplica el estimate
  de cada solicitud.
- Controller, scheduler, writer, store y snapshot provider aplican sus límites localmente.

Un probe desconocido falla cerrado solo en la frontera afectada: memoria desconocida bloquea trabajo
costoso; disco desconocido bloquea persistencia. Una excepción del sampler se convierte en un
snapshot desconocido y reason codes acotados.

`build_agent_application()` usa estos defaults reemplazables para un host modesto:

| Presupuesto | Default |
|---|---:|
| Reserva de memoria disponible | 256 MiB |
| Reserva de disco | 512 MiB |
| RSS máximo del proceso | 2 GiB |
| Cola máxima | 1 MiB / 256 items |

El scheduler superpone `queue_bytes` y `queue_count` actuales del writer en cada decisión de
procesamiento y persistencia. El snapshot de RAM/disco puede reutilizarse durante 250 ms, pero la
presión de cola nunca queda congelada en ese cache. Los límites del writer siguen siendo la última
frontera no bloqueante y usan los mismos defaults.

## Defaults ligeros y techos de captura

La solicitud pública base es 640×480 a 15 FPS con presupuesto de 1 MiB por frame. La política live
base usa:

| Control | Default |
|---|---:|
| Open / shutdown | 5 s / 5 s |
| Read | 250 ms |
| Stale | 2 s |
| Warmup | 3 frames |
| Reintentos de apertura | 5 |
| Cooldown | 250 ms → 30 s |
| Intervalo mínimo de reapertura | 1 s |
| Reset tras stream estable | 30 s |

Los guards globales permiten configurar hasta 32 MiB por frame, dimensión 8192, 240 FPS, timeout de
60 s, cooldown de 300 s, 16 reaperturas y 300 frames de warmup. Son techos opt-in: no reservan esa
memoria, no cambian los defaults y no afirman que OpenCV o una cámara negocien esos modos.

Para el agente, los defaults son PNG de hasta 4 MiB, borde 1280 y espera de 10 s; los techos revisados
son 8 MiB, borde 1920 y 15 s. El provider one-shot usa warmup 3, máximo 16, intervalo mínimo de open
de 0,5 s y una lectura target por defecto; puede habilitarse exactamente un retry adicional solo ante
error recuperable. La admisión suma un estimate reemplazable de 96 MiB para el proceso OpenCV
(techo configurable de 4 GiB) sin reservarlo por anticipado. El límite PNG acota el resultado
codificado, no el pico total de RAM durante
scanlines, compresión y ensamblado.

## Tipos y estructuras

- Frames: `bytes` inmutables; OpenCV entrega actualmente `bgr24`.
- PNG: encoder propio para `gray8`, `rgb24` y `bgr24`, sin metadata adicional.
- Timestamps, secuencias y contadores: rango signed 64-bit.
- Contratos: dataclasses `frozen=True, slots=True`.
- Payload semántico: copia profunda inmutable, máximo 32 niveles y 4096 nodos JSON-shaped.
- Colas densas: `deque`; latest state: un único slot protegido por `Condition`.
- No hay NumPy, tensors ni un modelo en el núcleo neutral.

Reducir resolución, frecuencia, copias y retención tiene prioridad sobre micro-optimizar objetos
Python.

## Métricas disponibles y brechas

Hoy existen snapshots inmutables de contadores para cámara, slots, scheduler, writer, admission,
snapshot y servicio de agente. SQLite expone health, filas, bytes lógicos y footprint de archivos.
Estas métricas no contienen payload ni se persisten automáticamente.

Aún no existen histogramas p50/p95/p99, process-write-bytes, thermal proxy, métricas del driver ni una
serie temporal persistente. Añadirlas requiere muestreo espaciado y cuota para que medir no se
convierta en la principal fuente de CPU o escrituras.
