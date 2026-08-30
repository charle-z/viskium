# Contribuir a Viskium

Viskium se desarrolla mediante cortes verticales pequeños, reproducibles y medibles. Antes de
cambiar código, lee la [constitución de ingeniería](docs/engineering-constitution.md), la
[arquitectura](docs/architecture/overview.md) y la
[política de recursos y datos](docs/architecture/resource-and-data-policy.md).

## Licencia y contribuciones

El proyecto se distribuye bajo Apache License 2.0. Salvo que se indique explícitamente lo
contrario, una contribución enviada intencionalmente para incluirse en Viskium se ofrece bajo los
mismos términos, conforme a la sección 5 de [LICENSE](LICENSE). Quien contribuye debe tener los
derechos necesarios sobre el material enviado.

## Preparar el entorno

Se requiere Python 3.13 y [uv](https://docs.astral.sh/uv/). El archivo `uv.lock` es la fuente
reproducible de dependencias y debe permanecer versionado.

```console
uv python install 3.13
uv sync --locked --group dev
```

Solo quien cambie deliberadamente las dependencias debe regenerar el lock con `uv lock` y
revisar el diff completo antes de incluirlo.

## Comprobaciones locales

Ejecuta el mismo conjunto de gates que CI:

```console
uv run ruff format --check .
uv run ruff check .
uv run mypy
uv run pytest
uv run python -m build --no-isolation --outdir dist
```

Las pruebas normales y CI no acceden a cámaras ni a otro hardware físico. Las pruebas marcadas
como `hardware` son opt-in y requieren autorización explícita, un equipo identificado y un
procedimiento que permita verificar el cierre de handles.

## Forma de trabajar

Cada cambio debe declarar:

- El escenario o riesgo que resuelve.
- Entradas, salidas y contrato afectado.
- Clasificación y vida útil de los datos.
- Presupuesto de CPU, RAM, disco, colas o latencia cuando aplique.
- Modos de fallo, recuperación y prueba de aceptación.

Mantén los pull requests pequeños. No añadas dependencias, modelos, persistencia, fuentes
físicas, red, concurrencia ni abstracciones especulativas sin el gate y ADR correspondientes.
Los frames, tensores y otros contenidos visuales no pueden aparecer en fixtures, logs o
artefactos salvo que exista una política explícita y consentimiento verificable.

## Dependencias y artefactos

- La aplicación no tiene dependencias de runtime en la fase fundacional.
- Toda dependencia nueva necesita consumidor inmediato y revisión de licencia y procedencia.
- Los modelos y datasets requieren checksum, fuente, licencia y contrato de entrada/salida.
- Nunca subas secretos, capturas privadas, bases de datos operativas o resultados locales.
- No publiques paquetes en PyPI desde ramas o pull requests.

## Definition of Done

Un cambio debe pasar formato, lint, tipos, pruebas y build del wheel. El comportamiento se prueba
desde una instalación construida, no solamente mediante imports accidentales desde el checkout.
Actualiza la matriz de capacidades y la documentación cuando cambie una capacidad observable.
