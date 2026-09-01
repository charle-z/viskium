# Prueba de visión con OpenCode

Viskium expone una prueba sintética, opt-in por llamada, para comprobar que los
claims enviados coinciden con un secreto visual de la imagen. El resultado
`claims_verified` no prueba la atención interna, causalidad ni el renderizado
de la interfaz. La prueba no abre la cámara ni guarda imágenes.
`viskium_vision_challenge_v1` devuelve dos bloques MCP: una imagen
`ImageContent` PNG de 512×384 y un recibo con su hash y dimensiones. El recibo
no contiene el token ni las figuras esperadas.

Usa este prompt exacto en OpenCode:

```text
Solicita una sola llamada a viskium_vision_challenge_v1. Examina el bloque de imagen, no el texto del recibo, y antes de llamar a la verificación muestra al usuario un JSON con exactamente estos campos: {"token":"...","shape_a":"...","shape_b":"...","relation":"..."}. Usa token en mayúsculas; shape_a y shape_b deben ser exactamente CIRCLE, TRIANGLE, SQUARE, DIAMOND o STAR; relation debe ser exactamente LEFT_OF, RIGHT_OF, ABOVE o BELOW. Después llama a viskium_verify_vision_challenge_v1 con ese JSON, el challenge_id del recibo y image_sha256 igual al sha256 del recibo. Finalmente muestra el proof card textual devuelto por la verificación. No inventes valores ni afirmes que viste una imagen si el bloque ImageContent no estuvo disponible.
```

El flujo esperado es:

1. El modelo debe exponer sus claims visuales al usuario **antes** de la
   segunda llamada.
2. El host ejecuta `viskium_verify_vision_challenge_v1`; cada challenge tiene
   un único intento y el resultado nunca revela la respuesta esperada.
3. `PASS` prueba que los claims enviados coinciden con la imagen sintética y
   que el hash de entrada coincidió (`claims_verified`). No prueba la atención
   interna del modelo, su causalidad ni que la interfaz haya renderizado la
   imagen. `FAIL` prueba que hubo un intento con claims incorrectos.
   `rejected` es deliberadamente uniforme para challenge ausente, expirado,
   reutilizado. Un challenge_id válido con claims mal formados consume su único
   intento y devuelve `FAIL`; un ID ausente, inválido, expirado o reutilizado
   devuelve `rejected` de forma uniforme.

El estado esperado del challenge permanece hasta 120 segundos, con un máximo
global de 32 challenges activos; la imagen no se persiste.

OpenCode entrega los bloques MCP al modelo si el cliente negocia el contenido
estándar. La interfaz del host puede no renderizar la imagen aunque el modelo
la reciba; por eso el JSON de claims debe aparecer antes de `verify` y el
resultado debe conservar el proof card. Referencias oficiales: [MCP Tools](https://modelcontextprotocol.io/docs/concepts/tools)
y [OpenCode documentation](https://opencode.ai/docs/).

No interpretes el hash o las dimensiones como evidencia de visión. Un cliente
que solo lee texto debe marcar la prueba como `vision_unsupported`, no como
`claims_verified` o `PASS`.

Los valores de `shape_a` y `shape_b` están cerrados a `CIRCLE`, `TRIANGLE`,
`SQUARE`, `DIAMOND` y `STAR`; `relation` está cerrado a `LEFT_OF`, `RIGHT_OF`,
`ABOVE` y `BELOW`.

El schema de entrada de `verify` acepta tipos amplios intencionalmente: el
middleware limita el tamaño JSON y el verificador sanitiza tipos, longitudes,
ASCII y enums antes de calcular `claims_sha256`. Así, un ID válido con un
claim inválido llega al store y consume el único intento, en lugar de ser
rechazado prematuramente por el SDK.
