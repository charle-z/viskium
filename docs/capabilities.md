# Matriz de capacidades

Estados permitidos:

- `planned`: existe una necesidad o diseño, pero no una implementación utilizable.
- `prototyped`: hay una ruta ejecutable, aunque falta cerrar un gate relevante, como hardware
  físico, integración externa o una matriz completa de fallos.
- `validated`: la implementación superó los gates automatizados declarados para el perfil indicado.
  No implica validación de hardware, calidad de visión ni operación prolongada salvo que la evidencia
  lo diga expresamente.
- `deployed`: una release soportada está instalada y operada.

El perfil de validación actual es local y de desarrollo: Python 3.13, pruebas automatizadas, cámaras
falsas, procesos inyectados y bases SQLite temporales. Además pasó un smoke one-shot 640×480 en una
cámara física de un host Windows, atravesando consentimiento, admisión y cliente MCP in-memory. Esa
única evidencia no constituye un perfil de cámara validado, una prueba de jornada prolongada ni una
instalación soportada.

| Capacidad | Estado | Alcance y evidencia actuales | Próximo gate |
|---|---|---|---|
| Identidad Viskium | validated | Paquete `viskium`, metadata, CLI, contratos versionados y licencia Apache-2.0 expresada con metadata PEP 639 | Mantener identidad y contratos estables al preparar la primera release |
| Toolchain Python reproducible | validated | Lock, Ruff, mypy, pytest, build e instalación aislada del wheel | Repetir la misma matriz en CI pública Ubuntu/Windows |
| Configuración, paths y data root | validated | Precedencia CLI → entorno → config → plataforma; layout marcado, inicialización explícita y verificación sin reparación | Fault matrix adicional sobre volúmenes reales |
| CLI de diagnóstico y mantenimiento | validated | `doctor`, `config`, replay, storage y consentimiento tienen pruebas unitarias/integración; `doctor` es read-only | Mantener comandos destructivos explícitos y acotados |
| Contratos neutrales | validated | Envelopes inmutables, payload de observación congelado en profundidad, puertos tipados y receipts v1 | Compatibilidad explícita ante el primer cambio de schema |
| Replay exhaustive sintético | validated | Reloj virtual y resultados deterministas; procesa todos los frames solicitados | Incorporar fixtures temporales no sintéticos sin datos privados |
| Replay faithful sintético | prototyped | Simula únicamente latest-only con intervalos y coste de procesamiento fijos; no reproduce todavía jitter, reconexiones, deadlines ni drivers físicos | Añadir fixtures temporales, reconexiones y deadlines observados sin datos privados |
| Fuente y processor sintéticos | validated | Lifecycle, límites y digest estructurado repetible cubiertos por contract/property/replay tests | Comparar con un processor real solo después del Product Gate |
| Store acotado en memoria | validated | Límites por cantidad y bytes, idempotencia, cierre y rechazo explícito | Conservarlo como referencia de contrato |
| Contratos de captura y cámara falsa | validated | `CaptureBackend`, políticas acotadas y `FakeCameraBackend` con ownership, deadlines y fallos programables | Mantener como contract suite de todo backend nuevo |
| `CameraController` | validated | Un owner thread, epochs, warmup, retries/cooldown cancelables, latest-frame, shutdown y estado `STUCK`, probados con cámara falsa | Caracterización con backend físico antes de llamarlo cámara validada |
| Latest-frame y latest-observation | validated | Slots de capacidad uno, reemplazo sin backlog, waits acotados, cierre con descarte y métricas sin payload | Pruebas de soak con productores/consumidores reales |
| Backend OpenCV aislado en proceso | prototyped | `OpenCVProcessCameraBackend` aplica ownership y deadlines; además de fallos inyectados, un smoke físico confirmó open/read/close y worker ausente en un host Windows | Unplug, busy, sleep/resume y driver hang con hardware identificado |
| Snapshot PNG efímero | validated | Encoder PNG sin dependencia externa, límites de borde/bytes y broker de demanda única probados | Benchmark de CPU/RSS con frames físicos |
| Snapshot de cámara one-shot | prototyped | Un smoke físico 640×480 recorrió grant → admission → MCP in-memory → open/warmup/read → PNG en RAM → close; cuota consumida y worker ausente | Repetir en perfiles identificados y medir CPU/RSS/latencia |
| `LiveScheduler` | validated | Un processor worker, frescura/epoch/identidad, admission, publicación latest-only y persistencia opcional cubiertos por pruebas | Soak con processor real y medición de latencias |
| `ObservationWriter` | validated | Store creado/usado/cerrado en un owner thread; submit no bloqueante y cola limitada por count/bytes | Fault tests con I/O real lento y apagado del host |
| SQLite acotado en modo DELETE | validated | Filas, bytes lógicos, payload, DB y queries acotados; purga manual y recuperación transaccional de un lote vencido bajo presión de filas/bytes; TTL, idempotencia, reserva de volumen, rollback y latch read-only probados en DB temporal | Cuota externa del conjunto DB/journal/temp y soak en volumen limitado |
| `BudgetPolicy` y admission gate | validated | Decisión pura, cache corto, estimaciones reaplicadas y fail-closed por recurso afectado | Calibrar defaults con perfiles de equipos objetivo |
| `ResourceSampler` | validated | Probe read-only de RSS/memoria/disco; ABI Windows explícita evita truncar handles de 64-bit y el smoke del host entrega RSS real | Matriz real Windows/Linux y presión sostenida |
| Consentimiento y servicio de lectura para agentes | validated | `ConsentLedger` y `AgentReadService` implementan grants fuera de banda, cuotas, revalidación y resultados acotados; suite integrada agent/consent está cerrada | Prueba de operación prolongada y recuperación tras crash |
| Transporte MCP local | prototyped | Tres tools versionadas; tests in-memory incluyen un PNG de cámara física bajo grant/admission, y el launcher stdio tiene gates sin hardware | Cliente stdio externo con cámara, revocación concurrente y soak |
| Processor real o modelo | planned | No existe selección ni calidad de visión validada | Tres escenarios de producto aprobados y benchmark comparable |
| Vista semántica | planned | No hay UI ni interpretación visual implementada | Prototipo con observaciones sintéticas antes de conectar visión |
| Persistencia visual o keyframes | planned | Desactivada por contrato: SQLite rechaza registros declarados `persistence_class="visual"`; no existe un detector semántico que audite al productor | Requisito explícito, consentimiento, threat model y cuota independiente |
| Teléfono como fuente | planned | No existe transporte, sincronización ni autenticación | ADR y threat model remoto separados |
| INT8, FP16, multiprocessing de inferencia o Rust/PyO3 | planned | No hay hot path ni modelo que justifique estas optimizaciones | Perfil E2E que demuestre beneficio neto |

Ninguna capacidad está `deployed`. `validated` solo afirma que el contrato de software indicado pasó
su evidencia automatizada; no demuestra calidad de visión, compatibilidad con una cámara concreta,
rendimiento sostenido ni aptitud para producción. El prototipo MCP tampoco convierte a Viskium en un
agente autónomo: expone lecturas acotadas y una captura one-shot bajo consentimiento existente.
