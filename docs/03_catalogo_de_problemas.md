# 03 · Catálogo de problemas inyectados

Cada problema tiene un **ID**, se puede **activar o desactivar** y tiene que tener: cómo aparece, cómo se detecta,
dónde se resuelve y qué regla de calidad (o qué métrica de ingesta) lo cubre. La columna "MVP" dice si entra en el
perfil `normal` del caos; el resto queda para el perfil `dificil`.

## A · Problemas de las APIs (tarjetas y espejo FX)

| ID | problema | cómo aparece | cómo se resuelve | MVP |
|---|---|---|---|---|
| API-01 | **Rate limit** | 429 con `Retry-After` cuando hay más de N pedidos por minuto | respetar `Retry-After` + backoff exponencial con jitter; nunca más de N pedidos en paralelo | ✅ |
| API-02 | **Errores 5xx intermitentes** | 500/502/503 en ~2 % de los pedidos | reintentos con tope; si se agotan, la corrida falla **sin** marcar la ventana como cargada | ✅ |
| API-03 | **Timeouts** | la respuesta tarda 30-60 s | timeout del cliente + reintento; el pedido es idempotente | ✅ |
| API-04 | **Token que vence a mitad de la paginación** | 401 en la página 37 de 50 | renovar el token y repetir **la misma página** (mismo cursor) | ✅ |
| API-05 | **Página repetida** | el cursor devuelve una página que ya vino | dedupe por `auth_id` en Bronze/Silver; métrica de duplicados por corrida | ✅ |
| API-06 | **Página salteada** | `next_cursor` saltea una página | conciliar contra `total_estimate` y contra el endpoint de totales diarios; re-pedir la ventana | ✅ |
| API-07 | **Orden inestable** | con offset (v1), filas que entran durante la paginación desplazan las páginas | usar v2 (cursor) o paginar ventanas cerradas de tiempo; explicar por qué offset no sirve | ✅ |
| API-08 | **Datos tardíos** | autorizaciones que aparecen en una ventana que ya se cargó | re-leer las últimas 72 h en cada corrida (ventana deslizante) + MERGE idempotente | ✅ |
| API-09 | **Cambio de versión v1 → v2** | campos renombrados, montos de centavos a decimales | contrato por versión en Bronze; normalizar en Silver | ✅ |
| API-10 | **Respuesta truncada o JSON inválido** | el cuerpo se corta | validar el JSON; reintentar; guardar la respuesta cruda fallida en un Volume de errores | dificil |
| API-11 | **Campo nuevo sin aviso** | aparece `wallet_type` (Apple/Google Pay) | Bronze guarda el JSON crudo + `rescued_data`; alerta de schema drift | dificil |
| API-12 | **200 con error adentro** | `200 {"status": "error"}` | validar el cuerpo, no sólo el código HTTP | dificil |

## B · Problemas en los datos

| ID | problema | cómo aparece | dónde se resuelve | MVP |
|---|---|---|---|---|
| DAT-01 | **Duplicados** | movimientos re-enviados en dos archivos | dedupe por `txn_id` (gana la primera llegada) | ✅ |
| DAT-02 | **Formato argentino** | `1.234,56`, `dd/MM/yyyy`, DNI con puntos | funciones de parseo en Silver | ✅ |
| DAT-03 | **Dos formatos de archivo del core** | antes de jul-2022 `latin1` con `;`; después UTF-8 con `,` | dos contratos de Bronze, una sola tabla Silver | ✅ |
| DAT-04 | **Montos en centavos** | API v1 de tarjetas | normalizar en Silver según la versión | ✅ |
| DAT-05 | **Monedas mezcladas** | cuenta en USD con un movimiento en ARS sin tipo de cambio | marcar, convertir con el FX del día y registrar `fx_source` | ✅ |
| DAT-06 | **Husos horarios** | el procesador manda UTC; el core, hora local sin offset | todo a UTC en Silver; `fecha_local` derivada para el negocio | ✅ |
| DAT-07 | **Identificadores inválidos** | CUIT con dígito verificador mal, CBU de 21 dígitos | validación con el algoritmo real; cuarentena | ✅ |
| DAT-08 | **Cliente duplicado entre bancos fusionados** | misma persona como `CUY-` y `NOA-` | resolución de identidad por DNI/CUIT + nombre; tabla `customer_xref` | ✅ |
| DAT-09 | **CRM que no matchea con el core** | `Core_Customer_Id__c` vacío o con el ID del banco viejo | match por CUIT/DNI; métrica "% de cuentas de Salesforce sin match" | ✅ |
| DAT-10 | **Saldos que no concilian** | suma de movimientos ≠ saldo de cierre | tabla de diferencias con causa (movimiento faltante, duplicado, ajuste) | ✅ |
| DAT-11 | **PII en texto plano** | email y teléfono en una columna `observaciones` | detección por regex; máscara en Unity Catalog | ✅ |
| DAT-12 | **CDC con borrados y reaperturas** | cliente dado de baja y vuelto a dar de alta | SCD2 en clientes y cuentas | ✅ |
| DAT-13 | **Feriados sin cotización** | el BCRA no publica fines de semana ni feriados | `dim_date.fx_oficial` repite el último publicado | ✅ |
| DAT-14 | **Montos imposibles** | compra de ARS 999.999.999 con débito | regla de calidad + cuarentena | dificil |
| DAT-15 | **Autorización sin liquidación (y viceversa)** | la liquidación llega días después o nunca | conciliación tarjetas, con antigüedad | dificil |

## C · Problemas en la ingesta

| ID | problema | cómo aparece | cómo se resuelve | MVP |
|---|---|---|---|---|
| ING-01 | **Archivo que llega dos veces** | mismo nombre o mismo contenido | Auto Loader no re-procesa el mismo archivo; hash de contenido para el resto | ✅ |
| ING-02 | **Archivo vacío o sólo con encabezado** | 0 filas | métrica por archivo y alerta | ✅ |
| ING-03 | **Archivo corrupto** | Parquet truncado, CSV con comillas abiertas | `badRecordsPath` / cuarentena de archivos | ✅ |
| ING-04 | **Un día que no llega** | falta el archivo del core del martes | control de completitud por fecha esperada; el gate frena Gold | ✅ |
| ING-05 | **Schema drift en CSV** | columna nueva `canal_digital` desde 2025 | `schemaHints` + `addNewColumns` | ✅ |
| ING-06 | **Archivo fuera de orden** | el lunes llega después del martes | el procesamiento no depende del orden de llegada | dificil |
| ING-07 | **Corrida interrumpida a la mitad** | el job se cae en la página 300 de la API | checkpoint de la ingesta por ventana; re-ejecutar sin duplicar | ✅ |

## Cómo se mide (para Genie y para el posteo)

Cada corrida de ingesta escribe una fila en `ops.ingestion_runs` y cada pedido HTTP una fila en `ops.api_calls`
(endpoint, código, intentos, latencia, filas). El Genie Agent **Plataforma de datos** responde sobre esas tablas:
*"¿cuántos 429 dio la API de tarjetas esta semana y cuánto tiempo perdimos esperando?"*.
