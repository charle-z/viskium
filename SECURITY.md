# Seguridad de Viskium

## Versiones soportadas

Viskium está en fase pre-alpha y todavía no tiene releases soportadas. Los reportes se evalúan
contra la rama principal vigente; esto no implica un SLA ni una garantía de corrección.

## Reportar una vulnerabilidad

No abras un issue público con detalles explotables, datos personales, capturas o secretos. Usa
la opción privada **Report a vulnerability** de la pestaña **Security** del repositorio de
GitHub. Si esa opción aún no está habilitada, comunica únicamente que necesitas un canal privado
mediante un issue sin detalles técnicos; una persona mantenedora indicará el canal apropiado.

Incluye, cuando sea seguro hacerlo:

- Versión o commit afectado y sistema operativo.
- Condiciones previas y pasos mínimos de reproducción.
- Impacto observado y alcance estimado.
- Logs redactados y sin contenido visual o identificable.
- Si el problema toca cámara, disco, memoria, retención o recuperación.

No accedas a equipos, cámaras o datos ajenos para investigar. No provoques deliberadamente
agotamiento de RAM o disco en un sistema real; utiliza límites simulados y fixtures controlados.

## Alcance especialmente sensible

Se consideran reportes de seguridad, entre otros:

- Acceso a cámara sin una acción o estado visible correspondiente.
- Frames, tensores u observaciones persistidos fuera de la política declarada.
- Evasión de TTL, cuotas, clasificación o borrado solicitado.
- Escrituras fuera de la raíz de datos configurada.
- Crecimiento no acotado de RAM, colas, logs, SQLite o temporales.
- Carga de modelos o artefactos sin verificar procedencia e integridad.
- Exposición de observaciones sensibles en logs, errores o telemetría.
- Ejecución de código mediante configuración, modelos o archivos importados.

Los problemas de rendimiento sin impacto de disponibilidad o integridad pueden tratarse como
bugs normales, pero un agotamiento reproducible y no acotado sí pertenece a este proceso.

## Divulgación

Permite que el equipo confirme el alcance y prepare una corrección antes de publicar detalles.
No se ofrecen recompensas ni plazos de respuesta en esta fase.

## Estado de licencia

El repositorio todavía no tiene una licencia de software adoptada. Su visibilidad pública no
autoriza automáticamente el uso, modificación o redistribución del código. La política de
licencia y contribución debe resolverse antes de aceptar contribuciones externas. Este aviso no
constituye asesoría legal.
