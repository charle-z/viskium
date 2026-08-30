# Viskium

Viskium es el nombre de trabajo y la identidad pública del proyecto. El producto final todavía no está definido; por diseño, el repositorio comienza como una base de ingeniería neutral para captura, procesamiento temporal y observaciones estructuradas locales.

Estado actual: `prototyped`. Las fases fundacionales F1 y F2 tienen una ruta ejecutable y superan
los gates locales de lock, formato, lint, tipos, property/contract/replay tests, cobertura y wheel
aislado. Esto no representa un release, despliegue ni validación de cámara o rendimiento.

Actualmente existen el paquete instalable, la CLI read-only `doctor`/`config`, contratos
neutrales, fuente y processor sintéticos, almacenamiento acotado en memoria y replay
`exhaustive`/`faithful`. Todavía no existen acceso a cámara, modelos, SQLite, persistencia en
disco, vista de producto ni un producto definido.

## Ruta ejecutable actual

Con Python 3.13 y el lock reproducible del repositorio:

```console
uv sync --locked --group dev
uv run viskium doctor --json
uv run viskium config show --effective --json
uv run viskium replay --mode exhaustive --frames 12 --json
uv run viskium replay --mode faithful --frames 12 --json
```

`doctor` y la carga de configuración inspeccionan rutas y recursos sin crear la raíz de datos ni
abrir hardware. Los dos modos de replay operan únicamente con datos sintéticos en esta fase.
El replay sintético rechaza más de 10 000 frames por ejecución para mantener acotadas CPU, RAM y
salida; no es un límite futuro para una sesión de cámara.

## Licencia

Viskium se publica bajo la Apache License 2.0. Consulta [LICENSE](LICENSE) para los términos
completos.

## Identidad técnica

- Nombre público: `Viskium`.
- Distribución Python provisional: `viskium`.
- Paquete importable estable: `viskium`.
- CLI: `viskium`.
- Schema ID: `viskium.<contrato>` con versión entera independiente.
- Variable para la raíz de datos: `VISKIUM_DATA_ROOT`.
- Raíz candidata para este equipo: `D:\ViskiumData` — no creada todavía.

La marca, la distribución, el paquete Python, los schemas y la ubicación de datos están desacoplados para que un cambio futuro no obligue a migrar todo el sistema.

## Documentación fundacional

- [Constitución de ingeniería](docs/engineering-constitution.md)
- [Arquitectura neutral](docs/architecture/overview.md)
- [Política de recursos y datos](docs/architecture/resource-and-data-policy.md)
- [Resolución de la auditoría adversarial](docs/adversarial-resolution.md)
- [ADR 0001: identidad y fundamento](docs/decisions/0001-foundation.md)
- [Matriz de capacidades](docs/capabilities.md)

## Gate de producto

Antes de elegir modelos, ontología, resolución, FPS o UI final se requieren tres escenarios concretos. Cada escenario debe declarar:

1. Entrada exacta.
2. Salida visible esperada.
3. Evento mínimo que no puede perderse.
4. Latencia y frescura máximas útiles.
5. Error más costoso: omitir, inventar o llegar tarde.
6. Observaciones que deben persistirse, durante cuánto tiempo y con qué sensibilidad.
7. Jornada de uso y ejecución en segundo plano.

Hasta entonces pueden seguir endureciéndose el núcleo sintético y las pruebas de recursos. La
persistencia SQLite, la captura física y cualquier modelo continúan detrás de sus gates.
