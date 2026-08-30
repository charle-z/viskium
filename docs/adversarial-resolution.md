# Registro de resolución adversarial

Estados:

- `RESOLVED`: decisión cerrada, pendiente de implementación y prueba.
- `DEFERRED`: pospuesta deliberadamente hasta que exista evidencia.
- `PRODUCT-GATE`: no puede decidirse responsablemente sin escenarios de producto.

No quedan hallazgos que impidan construir las fases fundacionales. Los bloqueos restantes son deliberados y evitan inventar el producto.

| ID | Hallazgo adversarial | Resolución | Estado | Gate |
|---|---|---|---|---|
| ADV-001 | Arquitectura describía un producto aún desconocido | Núcleo limitado a source, processor, observation, view y store | RESOLVED | Core no contiene graph, belief, action o planner |
| ADV-002 | No existe contrato de producto | Exigir tres escenarios antes del primer modelo | PRODUCT-GATE | Entrada, salida, evento mínimo, SLA, error, retención y jornada por escenario |
| ADV-003 | Fixtures parecían contradecir no-retención | Producción, desarrollo y diagnóstico tienen políticas separadas | RESOLVED | Fixtures sintéticos/públicos/consentidos con hash y manifest |
| ADV-004 | Replay “igual a live” era falso | Replay exhaustive y faithful; comparten processor y contratos | RESOLVED | Faithful reproduce drops, deadlines y reconexiones |
| ADV-005 | Persistencia puede bloquear live | Modos `disabled`, `best_effort_bounded` y `required` | RESOLVED / PRODUCT-GATE | Store lento produce gap o detención limpia, nunca espera infinita |
| ADV-006 | Revisión vieja podía rechazar toda inferencia | Validez por epoch, tiempo, campo y evidencia; revisión solo como procedencia | RESOLVED | Processor 10× lento aún aporta resultados válidos cuando corresponde |
| ADV-007 | Event sourcing/belief state prematuros | Append de observaciones compacto; estado complejo fuera del núcleo | RESOLVED | Nuevo concepto requiere escenario y ADR |
| ADV-008 | `ResourceGuardian` podía ser god object | Sampler + policy pura + enforcement local | RESOLVED | Policies se prueban sin I/O |
| ADV-009 | “2–3 frames” ignoraba buffers internos | Límite solo sobre buffers propios; medir RSS y copias reales | RESOLVED | Informe distingue slots propios de memoria total |
| ADV-010 | `frozen=True` no congela NumPy | `buffer_id`, generation, lease y copia de ROI larga | RESOLVED | Rotación no cambia datos del consumidor |
| ADV-011 | Latest-only no garantiza frame fresco | Medir frame age y comparar backends | DEFERRED | Caracterización MSMF/DSHOW y rechazo de frames viejos |
| ADV-012 | `read()` puede quedar bloqueado | Deadline de shutdown; proceso aislado solo si el fallo se reproduce | DEFERRED | Unplug test decide aislamiento |
| ADV-013 | Gate gris pierde cambios cromáticos/estáticos | Señal consultiva, heartbeat, histéresis y límite de tracking | RESOLVED | Fixtures isólumínicos, parpadeo y objeto inmóvil |
| ADV-014 | NV12/Y nativo era una suposición | No diseñar alrededor de un formato antes del spike | DEFERRED | Benchmark real de formatos/copies/CPU/frame age |
| ADV-015 | Timestamp de `read()` no es exposición | Source timestamp opcional + received monotonic + quality | RESOLVED | Latencia real se mide con estímulo si el producto la necesita |
| ADV-016 | Vista congelada puede parecer live | TTL y estado `STALE` visibles | RESOLVED | Desconexión invalida u oculta la escena |
| ADV-017 | “Modelo compacto” no está demostrado | Processor falso primero; bake-off después del producto gate | PRODUCT-GATE | Precisión útil, p95, RSS, CPU y thermal proxy |
| ADV-018 | FP16/INT8 podían empeorar CPU | FP32 baseline; INT8/FP16 detrás de benchmark | DEFERRED | Ganancia E2E con paridad funcional |
| ADV-019 | Tipos pequeños no ahorran objetos Python | Tipos compactos solo en buffers/arrays medidos | RESOLVED | Benchmark E2E y tests de overflow |
| ADV-020 | Seguridad física estaba sobrediseñada | Owner único, lifecycle mínimo, cooldown y cero controles ópticos inicialmente | RESOLVED | Open/close, unplug, sleep/resume y camera busy |
| ADV-021 | No se puede prometer desgaste cero | Garantía limitada a APIs documentadas y operación conservadora | RESOLVED | Ningún texto promete inmunidad física |
| ADV-022 | Semántica puede ser más sensible que una imagen | Clasificación por campo y política | RESOLVED / PRODUCT-GATE | Schema exige sensitivity y persistence class |
| ADV-023 | Presupuestos exactos eran arbitrarios | Todos son seeds de benchmark, no SLAs | RESOLVED | Presupuesto final referencia p99 y hardware profile |
| ADV-024 | `max_page_count` no limita todo SQLite | Defensa secundaria por conexión + cuota externa total | RESOLVED | Test cuenta DB, journal/WAL/SHM y temporales |
| ADV-025 | `journal_size_limit` no es hard WAL cap | No elegir WAL inicialmente | DEFERRED | Versión aprobada, readers largos y checkpoint tests |
| ADV-026 | `SQLITE_FULL` no garantiza rollback total | Rollback explícito, latch read-only y health check | RESOLVED | Fault injection inspecciona estado de transacción |
| ADV-027 | Purga desesperada puede fallar con disco lleno | Recuperar margen fuera de DB activa; no tocar journals calientes | RESOLVED | Volumen limitado simulado, nunca disco real |
| ADV-028 | Mover datos a D no prueba menor desgaste | Diferenciar capacidad lógica y dispositivo físico | RESOLVED | Inventario futuro del storage device |
| ADV-029 | SO y runtimes pueden escribir en C | Promesa limitada a persistencia controlada por Viskium | RESOLVED | Bootstrap reporta paths efectivos; threat model cubre lo externo |
| ADV-030 | `VACUUM` puede amplificar escrituras y necesitar espacio | `auto_vacuum=NONE`, reuse; segmentos solo si se justifican | RESOLVED / DEFERRED | Soak decide segmentación |
| ADV-031 | Logs rotatorios pueden recibir una entrada gigante | Límite por record, rate limit y cuota de directorio | RESOLVED | Log storm queda acotada |
| ADV-032 | Coalescing puede destruir evidencia | Distinguir sample, event y aggregate | RESOLVED / PRODUCT-GATE | Política por escenario declara fidelidad requerida |
| ADV-033 | Determinismo no demuestra corrección | Goldens + expectativa humana + baseline simple + casos ambiguos | RESOLVED | Modelo debe superar baseline bajo mismo presupuesto |
| ADV-034 | UX operativa estaba ausente | Estados mínimos: camera active, pause, stale, persistence gap, quota y delete | PRODUCT-GATE | Prototipo de UX antes de UI definitiva |
| ADV-035 | Teléfono no es solo otro adapter local | Fuente remota con clock, jitter, auth, batería y thermal profile | DEFERRED | ADR y threat model separados |
| ADV-036 | Nombre podía acoplarse a rutas/schema | Marca, distribución, import, schema y data root separados | RESOLVED | Cambio de display name no migra datos |

## Ataques obligatorios

Antes de declarar estable cualquier slice relevante se ejecutan los ataques que le correspondan:

1. Writer bloqueado mientras llegan frames.
2. Cuota principal y archivos auxiliares casi llenos.
3. Volumen o temporal sin espacio/permisos.
4. Presión externa de RAM y paging.
5. Processor diez veces más lento que la fuente.
6. Consumidor retiene lease durante rotación.
7. Frame atrasado entregado por backend.
8. Reconexión con nueva resolución/epoch.
9. Sleep/resume y cámara ocupada.
10. Llamada nativa que no responde.
11. NaN, shape enorme o excepción del processor.
12. Error repetitivo que intenta inundar logs.
13. Pinned data agota cuota.
14. Contenido semántico sensible o prohibido.
15. Replay exhaustive pasa y faithful falla.
16. Vista semántica stale después de desconexión.

Cada ataque debe tener outcome esperado, timeout, métrica y prueba automatizable o hardware opt-in claramente identificada.
