# Política de recursos, datos y almacenamiento

Estado: `accepted` como política; todos los números permanecen experimentales hasta calibración.

Excepción: los límites defensivos del control plane son techos de seguridad, no objetivos de
rendimiento. La base actual limita cada TOML a 1 MiB, el replay sintético a 10 000 frames y cada
payload de observación a 32 niveles y 4 096 valores JSON. Los enteros del payload son signed 64-bit
y los floats deben ser finitos. Cambiar estos contratos requiere pruebas y revisión explícita.

## Carriles de datos

| Carril | Contenido | Vida |
|---|---|---|
| Hot RAM | Frames, tensors, masks, ROI y tracking inmediato | Milisegundos o segundos |
| Estado de sesión | Observaciones recientes y coalescing | TTL corto en memoria |
| Store semántico | Observaciones estructuradas seleccionadas | TTL + cuota |
| Visual diagnóstico | Keyframes explícitos | Desactivado por defecto |
| Operación | Logs y métricas agregadas | Rotación estricta |

La aplicación no persiste intencionalmente contenido visual salvo diagnóstico explícito. No se promete ausencia forense en pagefile, crash dumps, driver o compositor.

## Clasificación

Todo campo persistible declara una clase:

```text
public
operational
sensitive
identifiable
prohibited
```

OCR, nombres, presencia, horarios, ubicación, identidad y relaciones no se consideran inocuos por carecer de píxeles.

## Modos de persistencia

- `disabled`: no se escriben observaciones.
- `best_effort_bounded`: live continúa ante fallo y genera un `persistence_gap` visible.
- `required`: la sesión se detiene limpiamente si no puede confirmar la observación requerida.

Ningún modo bloquea indefinidamente ni acumula backlog ilimitado.

## Política de escritura

Una observación se guarda solo si:

```text
contenido permitido
AND payload dentro de límites
AND no absorbido por coalescing
AND TTL y cuota válidos
AND reserva del volumen satisfecha
AND modo de persistencia lo permite
```

Se persisten transiciones, cambios significativos y heartbeats espaciados, no una fila por frame. Muestras, eventos y agregados permanecen distinguibles para no destruir evidencia sin declararlo.

## SQLite inicial

- Bases separadas para control y observaciones.
- Una conexión propietaria por writer.
- Journal convencional inicialmente.
- WAL solo tras benchmark, versión aprobada y pruebas de checkpoint/readers largos.
- Transacciones limitadas por filas, bytes y tiempo; valores salen del benchmark.
- `max_page_count` es defensa secundaria, se aplica y verifica en cada conexión y no limita archivos auxiliares.
- El presupuesto cuenta DB, journal o WAL/SHM, temporales, logs, keyframes y artifacts.
- `auto_vacuum=NONE` inicialmente para reutilizar páginas y reducir reescrituras.
- Sin `VACUUM` automático.
- Segmentos rotables solo si operación continua demuestra que debemos devolver espacio al sistema.
- Una DB corrupta se cierra y pone en cuarentena; nunca se borra automáticamente.

Ante `SQLITE_FULL`, `IOERR`, permisos o reserva crítica:

1. Rollback explícito si la conexión sigue en transacción.
2. Store enclavado en `READ_ONLY`.
3. Sin retry storm.
4. Sin crecimiento de la cola en RAM.
5. Gap visible o cierre de sesión según modo.
6. Recovery únicamente con margen, histéresis y health check.

## Paths

Identidad elegida:

```text
VISKIUM_DATA_ROOT
```

Precedencia prevista:

```text
CLI explícita
→ VISKIUM_DATA_ROOT
→ storage.root en config
→ directorio local de plataforma
```

En este equipo la candidata es `D:\ViskiumData`, pero no se crea hasta el bootstrap. La configuración pequeña puede vivir en el directorio de configuración del sistema; datos voluminosos, modelos, logs y temporales quedan bajo la raíz seleccionada.

Layout previsto:

```text
ViskiumData/
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

Toda limpieza verifica el marcador, la ruta absoluta, pertenencia a la raíz y política de la clase. Nunca opera sobre una raíz, home, volumen o repositorio completo.

Usar `D:` protege la capacidad libre de `C:`; no prueba que reduzca desgaste si ambas letras pertenecen al mismo dispositivo físico. Tampoco puede prometerse que Windows, pagefile o librerías nativas nunca escriban fuera de la raíz controlada.

## Memoria y CPU

Gobernanza dividida:

- `ResourceSampler`: mide.
- `BudgetPolicy`: decide de forma pura.
- Cada componente aplica localmente su límite.

La admisión de un componente pesado usa:

```text
available memory suavizada
- reserva del sistema
- pico p99 calibrado
- margen de interferencia
```

No se fija todavía un hard limit universal. Se observan working set, private bytes/commit, memoria disponible, commit del sistema, paging, handles, threads y bytes de colas.

Orden de degradación:

1. No admitir trabajo nuevo costoso.
2. Reducir frecuencia y resolución.
3. Descartar trabajo pendiente obsoleto.
4. Vaciar caches opcionales.
5. Desactivar processors opcionales.
6. Pausar inferencia si la presión continúa.
7. Cerrar controladamente antes de OOM o paging sostenido.

## Tipos y buffers

- Píxeles: `uint8`.
- Diferencias/acumulaciones: tipo con signo suficiente.
- Tensor CPU baseline: `float32`.
- INT8 solo tras bake-off E2E.
- FP16 solo se reconsidera con otro backend/hardware.
- Timestamps: `int64`.
- IDs Python en dominio pequeño; arrays compactos solo en lotes densos medidos.
- Metadatos: dataclasses con `slots`.
- ROI view solo durante la vida del lease; después, copia compacta.

Reducir frecuencia, resolución, copias y retención tiene prioridad sobre empaquetar objetos pequeños.

## Métricas obligatorias

```text
frame_age_ms
frames received/dropped/replaced
queue count/bytes/high-water
processor latency p50/p95/p99
RSS, peak RSS, private bytes y commit
observations produced/coalesced/persisted/rejected
persistence gaps
DB/journal/temp/log bytes
logical payload bytes y process write bytes
disk free y reserve state
commits/checkpoints/retries
handles/threads
```

Las métricas se agregan en RAM y se persisten espaciadamente; medir no puede convertirse en la principal fuente de escrituras.
