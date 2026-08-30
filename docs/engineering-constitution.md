# Constitución de ingeniería de Viskium

Estado: `accepted` para las fases fundacionales.

## Propósito

Esta constitución evita dos errores simétricos: construir una plataforma compleja antes de conocer el producto y escribir un prototipo desechable que luego no pueda verificarse, operar ni optimizarse.

Viskium comenzará pequeño, pero cada parte que entre al camino de ejecución tendrá contrato, propietario, presupuesto, fallo observable y prueba.

## Principios no negociables

1. **Núcleo neutral.** No se incorporan todavía conceptos como mundo, creencia, relación, planificación, acción o identidad. Aparecerán únicamente cuando un escenario de producto los exija.
2. **Monolito modular.** Un proceso y una instalación antes que microservicios, brokers o protocolos distribuidos.
3. **Determinismo primero.** La primera ruta funcional utiliza fuentes sintéticas y replay, no una cámara ni un modelo real.
4. **Frescura en vivo, exhaustividad en laboratorio.** Live puede descartar trabajo obsoleto; replay exhaustive no. Replay faithful reproduce los descartes y deadlines del modo live.
5. **Ownership exclusivo.** Cámara, buffers, processor, conexión de escritura y snapshot visible tienen un único propietario.
6. **Todo está acotado.** Colas, caches, buffers, lotes, archivos, logs, payloads y retención tienen límites por cantidad y por bytes.
7. **Persistencia selectiva.** Frames, tensores, masks y crops son efímeros. Las observaciones estructuradas pueden persistir bajo clasificación, TTL y cuota.
8. **Degradación explícita.** `STALE`, presión de memoria, persistencia suspendida, gaps y componentes no disponibles se muestran; jamás se maquillan como estado sano.
9. **Medir antes de optimizar.** Una reducción de `nbytes` no basta. Toda optimización debe mejorar precisión, latencia, memoria, CPU o escrituras extremo a extremo.
10. **Structured truth.** Narrativas, captions y texto generado nunca son fuente de verdad. Las vistas derivan de contratos estructurados y procedencia.
11. **Privacidad por significado.** Una observación sin píxeles puede seguir siendo sensible. La clasificación se aplica por campo y contenido.
12. **Hardware conservador.** Una sola sesión de cámara, lifecycle explícito, cooldown, controles ópticos deshabilitados por defecto y ninguna operación vendor-specific.
13. **Reproducibilidad.** Toda ejecución evaluable conserva versión del código, lock, configuración efectiva, hardware profile, schema, fixture y checksums de artefactos.
14. **Estado honesto.** Toda capacidad se marca `planned`, `prototyped`, `validated` o `deployed`.
15. **Sin nombres grandilocuentes ni antropomorfismo.** El vocabulario técnico describe lo que el sistema mide o produce, no lo que supuestamente “entiende”.

## Patrones aprobados

- Functional core / imperative shell.
- Ports and adapters solo en fronteras volátiles: fuente, reloj, processor, almacenamiento y salida.
- Composition root manual.
- Pipeline fijo en las primeras fases.
- State machine para lifecycle de recursos.
- Strategy para políticas con implementaciones reales.
- Single writer para almacenamiento.
- Backpressure y latest-value para datos que pierden valor con el tiempo.
- Lease + generation para buffers propios.
- Circuit breaker y cooldown para fallos repetidos.
- Bulkheads mediante presupuestos y colas separadas dentro del proceso.

## Patrones prohibidos sin gate

- Microservicios, Kafka, Redis, gRPC o bus externo.
- Framework de dependency injection.
- Motor DAG genérico.
- Plugin system dinámico.
- CQRS o event sourcing completo.
- Vector database sin búsqueda vectorial real.
- Grafo sin consultas concretas.
- `asyncio` sin I/O concurrente que lo justifique.
- Multiprocessing o shared memory sin profiling o necesidad de aislamiento.
- Rust/PyO3 sin hot path estable y medido.
- Modelo generativo ejecutado por frame.
- Persistencia de imágenes “por si acaso”.
- Un módulo, tabla o evento sin consumidor y política de vida.

## Definition of Ready

Un cambio entra a implementación únicamente cuando declara:

- Escenario o riesgo que resuelve.
- Entrada y salida.
- Contrato afectado.
- Clasificación de datos.
- Presupuesto de recursos.
- Modos de fallo y política de recuperación.
- Prueba de aceptación.
- Estado de la capacidad.

## Definition of Done

Un componente está terminado únicamente si:

- Se ejecuta desde el paquete instalado, no solo desde el checkout.
- Su contrato está versionado.
- Tiene ownership explícito.
- Sus colas, caches y archivos están acotados.
- Expone métricas y health state.
- Tiene shutdown y cleanup idempotentes.
- Supera unit, contract, integration y fault tests proporcionales al riesgo.
- Produce manifest cuando corresponde.
- No introduce persistencia no declarada.
- No tiene retry infinito, warnings ocultos ni estado stale presentado como live.
- Su documentación y capability status están actualizados.
