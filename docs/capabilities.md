# Matriz de capacidades

Estados permitidos:

- `planned`: diseñado, sin implementación.
- `prototyped`: existe una ruta demostrable, sin validación completa.
- `validated`: superó sus gates en un hardware profile definido.
- `deployed`: está instalado y operado como release soportada.

| Capacidad | Estado | Evidencia | Próximo gate |
|---|---|---|---|
| Identidad Viskium | validated | Paquete `viskium`, metadata, CLI y schemas v1 | Reserva/publicación solo tras resolver licencia |
| Constitución de ingeniería | prototyped | Documentos fundacionales y límites aplicados a F1/F2 | Mantenerla y revisarla en cada ADR y fase |
| Toolchain Python reproducible | validated | Python 3.13, lock, Ruff, mypy, pytest, build e instalación aislada del wheel | Confirmar la misma matriz en CI pública Ubuntu/Windows |
| CLI `doctor`/`config` | validated | Diagnóstico read-only, precedencia de configuración y errores cubiertos por pruebas | Ampliar únicamente con recursos implementados |
| Contratos neutrales | validated | Envelopes inmutables, puertos tipados, clasificación y receipts v1 | Compatibilidad explícita ante el primer cambio de schema |
| Replay exhaustive | validated | Replay sintético determinista procesa todos los frames | Fixtures no sintéticos solo con política aprobada |
| Replay faithful | validated | Prototipo sintético determinista reemplaza trabajo pendiente obsoleto | F4: deadlines, validez temporal y processor lento |
| Fuente sintética | validated | Lifecycle, reloj virtual y límites cubiertos por contract/property tests | Mantener como referencia para nuevos adapters |
| Processor determinista | validated | Digest estructurado repetible con procedencia versionada | Compararlo con el primer processor real tras Product Gate |
| Store acotado en memoria | validated | Límites por cantidad/bytes, idempotencia, cierre y rechazo explícito | F3: políticas de presión y persistencia |
| Suite fundacional | validated | Unit, property, contract, replay e integration tests; branch coverage supera el gate de 90% | Conservar gates al ampliar F3/F4 |
| Captura de cámara | planned | Plan F5 | Hardware characterization autorizada |
| Processor real/modelo | planned | Bloqueado por Product Gate | Tres escenarios aprobados |
| Vista semántica | planned | Diseño condicionado por Product Gate | Prototipo con observaciones falsas |
| SQLite acotado | planned | Política de almacenamiento | Fault suite y quota total |
| WAL | planned | Aplazado por diseño | Benchmark + versión SQLite aprobada |
| ResourceSampler/BudgetPolicy | planned | Política de recursos | Pressure tests |
| Keyframes diagnósticos | planned | Desactivados por defecto | Requisito, consentimiento y cuota |
| Teléfono como fuente | planned | Aplazado | ADR + threat model remoto |
| INT8/FP16 | planned | Aplazado | Bake-off E2E |
| Multiprocessing | planned | Aplazado | Perfil o aislamiento demostrado |
| Rust/PyO3 | planned | Aplazado | Hot path estable >= ~20% E2E |

Ninguna capacidad está `deployed`. Las validaciones anteriores corresponden al núcleo sintético
fundacional y no demuestran calidad de visión, rendimiento sostenido, seguridad de cámara ni
operación en producción. La presencia en este archivo tampoco implica que una capacidad aplazada
deba implementarse; puede eliminarse cuando el producto quede definido.
