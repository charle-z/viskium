# Registro de resolución adversarial

Estados:

- `VALIDATED`: la resolución está implementada y pasó sus gates automatizados dentro del alcance
  indicado.
- `PROTOTYPED`: existe una implementación, pero falta hardware, operación real o una integración
  necesaria para cerrar el hallazgo.
- `RESOLVED`: la decisión de diseño está cerrada, aunque su implementación depende de una fase
  posterior.
- `DEFERRED`: pospuesta deliberadamente hasta que exista evidencia.
- `PRODUCT-GATE`: no puede decidirse responsablemente sin escenarios de producto.

No quedan hallazgos que impidan continuar los slices fundacionales. Sí quedan gates físicos,
operacionales y de producto; por eso ninguna ruta está `deployed` y la cámara/OpenCV one-shot
permanece `prototyped`.

| ID | Hallazgo adversarial | Resolución actual | Estado | Gate restante |
|---|---|---|---|---|
| ADV-001 | Arquitectura describía un producto aún desconocido | Núcleo limitado a source, processor, observation y store; el boundary de agentes es separado | VALIDATED | Core no contiene graph, belief, action o planner |
| ADV-002 | No existe contrato de producto | Exigir tres escenarios antes del primer processor real | PRODUCT-GATE | Entrada, salida, evento mínimo, SLA, error, retención y jornada por escenario |
| ADV-003 | Fixtures parecían contradecir no-retención | Producción, desarrollo y diagnóstico tienen políticas separadas | RESOLVED | Solo fixtures sintéticos, públicos o consentidos con procedencia |
| ADV-004 | Replay “igual a live” era falso | Hay exhaustive y faithful sintéticos; faithful implementa latest-only, no paridad completa de driver | PROTOTYPED | Añadir jitter, reconexiones y deadlines observados sin datos privados |
| ADV-005 | Persistencia puede bloquear live | `disabled` y best-effort acotado existen; writer no bloquea submit y reporta rechazo/fallo | PROTOTYPED / PRODUCT-GATE | El modo `required` y su política de cierre aún no existen |
| ADV-006 | Una revisión vieja podía rechazar toda inferencia | Scheduler valida epoch, identidad, frame age y result age antes de publicar | VALIDATED | Semántica de fusión por campo depende del producto |
| ADV-007 | Event sourcing o belief state eran prematuros | Observaciones append compactas; estado complejo queda fuera del núcleo | VALIDATED | Nuevo concepto requiere escenario y ADR |
| ADV-008 | `ResourceGuardian` podía ser god object | `ResourceSampler` + `BudgetPolicy` pura + enforcement local + admission cache | VALIDATED | Calibración con perfiles reales |
| ADV-009 | “2–3 frames” ignoraba buffers internos | Solo se promete capacidad de slots propios; se reconocen copias de driver, IPC y consumidor | PROTOTYPED | Medir RSS/copies con cada backend físico |
| ADV-010 | `frozen=True` no congelaba estructuras anidadas | Frames son `bytes`; payload semántico se copia y congela en profundidad con límites | VALIDATED | Un futuro buffer lease requerirá contrato y tests propios |
| ADV-011 | Latest-only no garantiza frame fresco | Controller detecta stale y scheduler limita frame age | PROTOTYPED | Caracterizar timestamps y buffers MSMF/DSHOW u otro backend real |
| ADV-012 | `read()` puede quedar bloqueado | OpenCV corre en proceso hijo y el backend hace terminate/kill al vencer el deadline | PROTOTYPED | Unplug y driver hang con cámara física |
| ADV-013 | Un gate gris perdería cambios cromáticos o estáticos | Ningún gate visual barato se incorpora antes del Product Gate | RESOLVED | Si aparece, exigir heartbeat e fixtures isólumínicos |
| ADV-014 | NV12/Y nativo era una suposición | El adapter actual declara BGR24; otros formatos requieren spike | DEFERRED | Benchmark real de formatos, copies, CPU y frame age |
| ADV-015 | Timestamp de `read()` no es exposición | Source timestamp es opcional; received monotonic y quality son explícitos | VALIDATED | Estímulo físico si el producto necesita latencia de exposición |
| ADV-016 | Una vista congelada puede parecer live | Latest observation distingue stale/future/closed; no existe UI que pueda prometer live | PROTOTYPED | UX de desconexión antes de una vista semántica |
| ADV-017 | “Modelo compacto” no está demostrado | Processor sintético primero; bake-off después del Product Gate | PRODUCT-GATE | Precisión útil, p95, RSS, CPU y thermal proxy |
| ADV-018 | FP16/INT8 podían empeorar CPU | No hay tensor baseline; INT8/FP16 permanecen fuera hasta existir modelo y benchmark | DEFERRED | Ganancia E2E con paridad funcional |
| ADV-019 | Tipos pequeños no ahorran objetos Python | Rangos signed 64-bit y límites estructurales; compactación solo en buffers medidos | VALIDATED | Benchmark E2E antes de introducir arrays especializados |
| ADV-020 | Seguridad física estaba sobrediseñada | Owner único, lifecycle mínimo, cooldown y cero controles ópticos | PROTOTYPED | Open/close, unplug, sleep/resume y camera busy físicos |
| ADV-021 | No se puede prometer desgaste cero | Garantía limitada a APIs documentadas, límites de escrituras y operación conservadora | VALIDATED | Ningún texto promete inmunidad física |
| ADV-022 | Semántica puede ser más sensible que una imagen | Contratos exigen sensitivity/persistence class; prohibited se rechaza | VALIDATED / PRODUCT-GATE | Política por schema y escenario |
| ADV-023 | Presupuestos exactos eran arbitrarios | Defaults ligeros separados de techos opt-in y todos son reemplazables | PROTOTYPED | Presupuesto final referencia p99 y hardware profile |
| ADV-024 | `max_page_count` no limita todo SQLite | Se limita DB y se reporta DB/journal/WAL/SHM; cuota externa total sigue ausente | PROTOTYPED | Enforcer de volumen que incluya temporales, logs y artifacts |
| ADV-025 | `journal_size_limit` no es hard WAL cap | SQLite usa journal `DELETE`; WAL no se habilita | DEFERRED | Benchmark/versionado/checkpoints antes de considerar WAL |
| ADV-026 | `SQLITE_FULL` no garantiza rollback total | Rollback explícito, latch read-only y health state con fault tests | VALIDATED | Repetir en volumen limitado real opt-in |
| ADV-027 | Purga desesperada puede fallar con disco lleno | Purga solo explícita y acotada; no se toca un journal caliente automáticamente | RESOLVED | Recovery de margen fuera de DB si la operación continua lo exige |
| ADV-028 | Mover datos a D no prueba menor desgaste | Se diferencia capacidad lógica de dispositivo físico | VALIDATED | Inventario futuro del storage device |
| ADV-029 | SO y runtimes pueden escribir fuera del data root | Promesa limitada a persistencia controlada; paths efectivos son reportables | VALIDATED | Threat model operativo cubre pagefile, dumps y clientes |
| ADV-030 | `VACUUM` amplifica escrituras y necesita espacio | `auto_vacuum=NONE`, reuse y ninguna ejecución automática de `VACUUM` | VALIDATED / DEFERRED | Soak decide si alguna vez hacen falta segmentos |
| ADV-031 | Logs rotatorios pueden recibir una entrada gigante | No existe todavía un pipeline de logs persistentes | DEFERRED | Antes de crearlo: límite por record, rate limit y cuota de directorio |
| ADV-032 | Coalescing puede destruir evidencia | Idempotencia/coalescing conservan receipts; fidelidad semántica depende del escenario | RESOLVED / PRODUCT-GATE | Distinguir sample, event y aggregate por schema |
| ADV-033 | Determinismo no demuestra corrección | El processor sintético solo demuestra repetibilidad, no visión correcta | PRODUCT-GATE | Goldens humanos y baseline bajo el mismo presupuesto |
| ADV-034 | UX operativa estaba ausente | MCP expone status acotado; no hay UI ni vista semántica | PRODUCT-GATE | Diseñar camera active, stale, gap, quota y delete antes de UI definitiva |
| ADV-035 | Teléfono no es solo otro adapter local | Fuente remota exige clock, jitter, auth, batería y thermal profile | DEFERRED | ADR y threat model separados |
| ADV-036 | Nombre podía acoplarse a rutas/schema | Marca, distribución, import, schema y data-root identity están separados | VALIDATED | Cambio de display name no migra datos |
| ADV-037 | Un tool de agente podía abrir hardware sin control o convertirse en video | Consentimiento fuera de banda, cuota por intento, one-shot serializado y smoke físico mediante MCP in-memory | PROTOTYPED | Cliente stdio externo, revocación concurrente y más perfiles físicos |
| ADV-038 | La tool latest podía aparentar un pipeline que no existe | `build_agent_application()` no inicia producer y el estado vacío es explícito | VALIDATED | Composition root E2E antes de anunciar observaciones live |
| ADV-039 | Un límite oculto podía volver inutilizable la cámara | Defaults 640×480@15/1 MiB y techos configurables hasta 32 MiB sin reserva anticipada | VALIDATED | Ajustar por mediciones, no reducir techos por intuición |
| ADV-040 | RSS desconocido negaba toda captura en Windows | Firmas ctypes ABI explícitas evitan truncar el pseudo-handle de proceso; probe real y tests de fallo parcial | VALIDATED | Repetir en matriz Windows/Linux y presión sostenida |

## Ataques automatizados cubiertos

Las pruebas actuales ejercitan, con dobles o recursos temporales:

1. Writer bloqueado, cola llena por count/bytes y shutdown no cooperativo.
2. Rechazo por reserva de disco, límites de DB/filas/payload y errores SQLite inyectados.
3. Presión o probes desconocidos de RAM/disco con fail-closed por frontera.
4. Processor lento, bloqueado, con excepción o resultado tardío/identidad incorrecta.
5. Frame futuro, viejo, reemplazado o de otra epoch.
6. Reconexión, cambio de epoch, open/read/close fuera de deadline y backend inseguro.
7. NaN, enteros fuera de rango, nesting enorme, ciclos y payload prohibido.
8. Solicitudes concurrentes de snapshot, agotamiento de cuota y revocación/cambio de grant.
9. Inputs MCP desconocidos, sobredimensionados o fuera de límites.
10. Diferencia determinista entre replay exhaustive y faithful sintéticos.

## Gates adversariales todavía opt-in

No se consideran cerrados sin el entorno correspondiente:

1. Cámara ocupada, unplug, sleep/resume y driver que no responde.
2. Jornada prolongada con RSS, paging, CPU, temperatura y frame age reales.
3. Volumen limitado real, pérdida de permisos y espacio agotado incluyendo archivos auxiliares.
4. Cliente MCP externo sobre stdio y retención del PNG fuera de Viskium.
5. Pipeline continuo compuesto de cámara → processor → observación → SQLite.
6. Vista semántica stale y cualquier métrica/UX que dependa de un producto definido.
7. Log storm, porque el pipeline persistente de logs todavía no existe.

Cada gate debe declarar outcome esperado, timeout, métrica, cleanup y si usa datos o hardware
consentidos. Un test con fake valida el contrato de software, no el comportamiento del dispositivo.
