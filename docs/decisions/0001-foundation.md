# ADR 0001 — Identidad y fundamento técnico

Estado: `accepted`, con decisiones posteriores registradas sin reescribir el contexto original.

Fecha: 2026-08-30

## Evolución posterior

Este ADR conserva las decisiones tal como se formularon al iniciar el repositorio. Durante el mismo
corte fundacional, las siguientes partes dejaron de ser provisionales o fueron reemplazadas:

| Decisión inicial | Estado posterior |
|---|---|
| Distribución provisional e import/CLI previstos | Distribución, paquete importable y CLI son `viskium`; la versión actual sigue siendo pre-alpha. |
| Raíz fija ligada al host | Reemplazada por una raíz configurable con default de plataforma; `.viskium` es la opción explícita del checkout y nunca se crea implícitamente. |
| Git y publicación fuera de esta decisión | El código fuente se publica en GitHub; todavía no existe una release soportada ni publicación en PyPI. |
| Licencia pendiente | Resuelta como Apache License 2.0 con metadata PEP 639 y `LICENSE` en los artefactos. |
| Schema IDs `viskium.<contrato>` | Se conserva para observaciones semánticas; los contratos de frontera usan URN versionadas independientes. |
| Replay `faithful` previsto como espejo de live | La implementación actual ofrece replay sintético: `exhaustive` procesa todos los frames y `faithful` solo simula latest-only con tiempos fijos; no se afirma paridad con cámara física. |

La búsqueda preliminar del nombre continúa sin constituir reserva de marca ni clearance legal. Esta
actualización documenta implementación y publicación técnica, no disponibilidad comercial.

## Contexto

El proyecto necesita una identidad estable y una infraestructura capaz de evolucionar antes de definir su producto final. La auditoría adversarial demostró que acoplar marca, dominio, almacenamiento y modelo produciría decisiones prematuras.

## Decisión

- Nombre público: `Viskium`.
- Distribución Python provisional: `viskium`.
- Import package y CLI previstos: `viskium`.
- Schema IDs: `viskium.<contrato>` con `schema_version` entero independiente.
- Raíz de datos configurable con `VISKIUM_DATA_ROOT`.
- Candidata local separada del checkout, sin crear todavía.
- Monolito modular Python con functional core/imperative shell.
- El core comienza neutral y sin ontología de producto.
- Marca, distribución, import, schemas y paths permanecen desacoplados.

El nombre del directorio de checkout no forma parte de la identidad pública. Git y la publicación
quedaban fuera de esta decisión inicial.

## Alternativas rechazadas

- Usar el nombre público en cada tabla y ruta.
- Inventar un reverse-domain sin poseer dominio.
- Diseñar belief graph, planner o modelos antes del Product Gate.
- Guardar datos voluminosos en el volumen del sistema por defecto.
- Usar el Registro de Windows como almacén de datos o configuración compleja.

## Consecuencias

- Un rebranding futuro no requiere reinterpretar schemas ni mover datos automáticamente.
- La distribución puede cambiar a `viskium-core` sin cambiar `import viskium` si PyPI lo exige.
- La disponibilidad comercial, dominio, handles y marca se verifican antes del primer release público.
- El producto puede evolucionar sin que la infraestructura lo haya definido de antemano.

## Evidencia y revisión

La búsqueda preliminar del 2026-08-30 no mostró una coincidencia exacta obvia para `Viskium`, pero no constituye reserva ni clearance legal.

Revisar este ADR antes de:

- Registrar organización, dominio o paquete público.
- Elegir licencia.
- Producir datos que deban sobrevivir un cambio de namespace.
- Cambiar la frontera monolítica.

Los gates de licencia y publicación del código fuente fueron revisados en la evolución posterior.
Siguen vigentes los gates de marca/dominio, publicación de paquete soportado, namespace durable y
cambio de frontera.
