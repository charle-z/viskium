# Viskium

[![CI](https://github.com/charle-z/viskium/actions/workflows/ci.yml/badge.svg)](https://github.com/charle-z/viskium/actions/workflows/ci.yml)

Viskium es una base local y acotada para captura temporal, observaciones estructuradas y acceso
visual bajo demanda. Está en **pre-alpha**: ya ofrece infraestructura ejecutable y comprobada,
pero todavía no define un producto final de reconocimiento, traducción o asistencia visual.

El diseño prioriza tres propiedades: los frames e imágenes son efímeros, la cámara tiene un solo
dueño con cierre acotado y la presión de RAM/disco degrada funciones concretas sin tumbar toda la
aplicación. Los valores por defecto son presupuestos modestos para equipos limitados; las
resoluciones, FPS, esperas y tamaños de snapshot se pueden ajustar dentro de techos de seguridad
prácticos.

## Qué existe

| Superficie | Estado actual |
|---|---|
| Contratos, replay exhaustivo sintético y procesamiento determinista | Validado por pruebas automatizadas |
| Replay faithful sintético | Prototipado; latest-only con tiempos fijos, sin paridad física afirmada |
| SQLite para observaciones estructuradas con TTL, cuotas y consultas acotadas | Validado por pruebas automatizadas |
| Latest-frame/latest-observation, scheduler y writer con backpressure | Validado como componentes |
| Controlador de cámara con ownership, deadlines, cooldown y epochs | Validado con dobles y fault tests |
| Backend OpenCV aislado en proceso y snapshot PNG one-shot | Prototipado; validado con dobles, fallos inyectados y pruebas de proceso |
| Servicio MCP local con consentimiento explícito | Prototipado y probado in-memory |

No existen todavía un modelo de visión seleccionado, traducción, UI final, ingestión desde
teléfono, audio, nube ni un pipeline continuo ejecutable que conecte todos los componentes. El
tool de observación del servidor MCP devuelve `empty` hasta que un productor in-process publique
observaciones. Ninguna capacidad se presenta como desplegada.

## Inicio rápido

Requiere Python 3.13 y [uv](https://docs.astral.sh/uv/):

```console
uv sync --locked --no-dev --extra agent --extra camera
uv run --no-sync viskium doctor --json
uv run --no-sync viskium replay --mode faithful --frames 12 --json
```

La instalación base no obliga a instalar OpenCV ni MCP. Se pueden seleccionar por separado con
los extras `camera` y `agent`. La ruta ejecutable soportada actualmente es Windows o Linux.
El modo `faithful` es todavía un replay sintético: conserva la política latest-only bajo tiempos
fijos y no afirma paridad con jitter, reconexiones, deadlines ni drivers de una cámara física.

El extra `camera` fija `opencv-python==5.0.0.93` en Windows para disponer de Media Foundation y
DirectShow; en POSIX usa `opencv-python-headless==5.0.0.93`. La política `AUTO` consulta la
disponibilidad y elige un solo backend antes de abrir el dispositivo; una apertura fallida no
salta silenciosamente a una segunda API ni realiza una segunda apertura.

## Almacenamiento local

La raíz de datos nunca se crea de forma implícita:

```console
uv run --no-sync viskium storage init --data-root .viskium --json
uv run --no-sync viskium storage status --data-root .viskium --json
uv run --no-sync viskium storage purge-expired --data-root .viskium --limit 128 --json
```

Viskium persiste únicamente observaciones estructuradas con TTL. SQLite rechaza los registros que
el productor declara con `persistence_class="visual"` o `sensitivity_class="prohibited"`; todavía
no existe un detector semántico que pueda corregir una clasificación falsa. Los frames crudos y
snapshots de los adaptadores incluidos se mantienen en memoria y Viskium no los escribe al disco.
Esto no promete borrar rastros que el sistema operativo, un driver o el archivo de paginación
puedan conservar fuera del control del proceso.

## Acceso para agentes

El servidor expone exactamente tres tools versionadas:

- `viskium_status_v1`: configuración y health de cierre mínimos, sin contadores de actividad,
  sin abrir cámara ni exigir consentimiento.
- `viskium_latest_observation_v1`: última observación fresca y compatible.
- `viskium_snapshot_v1`: una captura PNG acotada, con apertura y cierre en la misma llamada.

La superficie MCP no ofrece tools para otorgar consentimiento, abrir una sesión continua ni
administrar el lifecycle de cámara. El usuario prepara la raíz y concede scopes fuera del
protocolo:

```console
uv run --no-sync viskium storage init --data-root .viskium
uv run --no-sync viskium consent grant --data-root .viskium --scope observation.read --scope snapshot.read --duration-seconds 3600 --snapshot-quota 100 --sensitivity-ceiling identifiable
uv run --no-sync viskium agent serve --data-root .viskium
```

El último comando habla MCP por `stdio`; normalmente lo inicia el host del agente, no una
persona para interactuar con él. [`.codex/config.toml`](.codex/config.toml) contiene una
configuración de proyecto deshabilitada en clones nuevos. Después de inicializar `.viskium`
y conceder consentimiento, confía en el proyecto, cambia `enabled = true` localmente y reinicia
el host de Codex para que cargue el servidor.

El launcher configurado usa el comando global `python` solo para localizar y ejecutar el Python
3.13 ya sincronizado en `.venv`; no invoca `uv` ni instala nada. En un Linux que solo tenga el
alias `python3`, cambia localmente `command = "python3"`. Si `uv` deja de estar en `PATH` después
de crear el entorno, cualquier comando `uv run --no-sync viskium ...` de esta guía puede ejecutarse
directamente como `.venv\\Scripts\\viskium.exe ...` en Windows o `.venv/bin/viskium ...` en POSIX.

Al terminar, revoca primero el grant para cortar el acceso inmediatamente:

```console
uv run --no-sync viskium consent revoke --data-root .viskium
```

Después restaura `enabled = false` en `.codex/config.toml` y reinicia Codex. Esperar a la expiración
también cierra el grant, pero no es necesario conservarlo cuando la tarea ya terminó.

Defaults del snapshot: 640×480 a 15 FPS solicitados, PNG de hasta 4 MiB, borde de 1280 px y espera
de 10 s. La CLI permite llegar hasta 8 MiB, 1920 px y 15 s. La captura general admite
configuraciones mayores de forma explícita; estos límites del tool one-shot evitan que una llamada
de agente se convierta accidentalmente en streaming.

## Prueba física opt-in

Las pruebas normales y CI no abren cámaras. En un equipo autorizado, el smoke opt-in solicita una
sola imagen 640×480, no la muestra ni la guarda y verifica el cierre:

```powershell
$env:VISKIUM_RUN_CAMERA_TESTS = "1"
uv run pytest tests/hardware/test_camera_smoke.py -m hardware
Remove-Item Env:VISKIUM_RUN_CAMERA_TESTS
```

Se puede seleccionar otro dispositivo con `VISKIUM_CAMERA_DEVICE_INDEX`. No ejecutes esta
prueba sobre hardware ajeno o mientras otra aplicación necesite la cámara. La compatibilidad con
un dispositivo concreto, unplug, busy, sleep/resume y operación prolongada sigue siendo un gate
opt-in; no se presenta como validada por el repositorio.

## Gates de ingeniería

```console
uv run ruff format --check .
uv run ruff check .
uv run mypy
uv run pytest
uv run python -m build --no-isolation --outdir dist
```

Para estos gates instala primero el grupo `dev` como se indica en
[CONTRIBUTING.md](CONTRIBUTING.md). La suite usa branch coverage mínima de 90%, tests
contract/property/fault/integration y una
matriz pública Windows/Ubuntu. Consulta la [arquitectura](docs/architecture/overview.md), la
[política de recursos y datos](docs/architecture/resource-and-data-policy.md), la
[matriz de capacidades](docs/capabilities.md) y la
[resolución adversarial](docs/adversarial-resolution.md).

## Identidad y licencia

- Proyecto, distribución, paquete y CLI: `viskium`.
- Contratos: URN/versiones independientes por frontera.
- Variable de raíz de datos: `VISKIUM_DATA_ROOT`.
- Licencia: [Apache License 2.0](LICENSE).

La marca, los schemas y la ubicación de datos están desacoplados para permitir migraciones sin
romper todo el sistema.
