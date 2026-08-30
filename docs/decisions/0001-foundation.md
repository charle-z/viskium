# ADR 0001 — Identidad y fundamento técnico

Estado: `accepted`

Fecha: 2026-08-30

## Contexto

El proyecto necesita una identidad estable y una infraestructura capaz de evolucionar antes de definir su producto final. La auditoría adversarial demostró que acoplar marca, dominio, almacenamiento y modelo produciría decisiones prematuras.

## Decisión

- Nombre público: `Viskium`.
- Distribución Python provisional: `viskium`.
- Import package y CLI previstos: `viskium`.
- Schema IDs: `viskium.<contrato>` con `schema_version` entero independiente.
- Raíz de datos configurable con `VISKIUM_DATA_ROOT`.
- Candidata local: `D:\ViskiumData`, sin crear todavía.
- Monolito modular Python con functional core/imperative shell.
- El core comienza neutral y sin ontología de producto.
- Marca, distribución, import, schemas y paths permanecen desacoplados.

La carpeta actual `D:\Proyectos\vision` no necesita renombrarse para adoptar la identidad. No se inicializa Git ni se publica un paquete como parte de esta decisión.

## Alternativas rechazadas

- Usar el nombre público en cada tabla y ruta.
- Inventar un reverse-domain sin poseer dominio.
- Diseñar belief graph, planner o modelos antes del Product Gate.
- Guardar datos voluminosos en la ruta por defecto de `C:` en este equipo.
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
