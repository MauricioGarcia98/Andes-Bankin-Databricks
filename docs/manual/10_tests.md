# 10 · Tests

> Carpeta `tests/`. 49 tests que prueban el pipeline **sin nube y sin red**: la API de Azure corre en el mismo
> proceso, Azure Blob es un diccionario en memoria y el reloj del caos es simulado. La idea central: **el lakehouse
> tiene que recuperar la verdad** que generó el generador.

## 1. Cómo se corren

```bash
pytest -m "not spark"     # rápidos: API, cliente, CRM, ids, config, sql_runner (segundos a pocos minutos)
pytest -m spark           # con Spark local + Delta OSS: generador, exportes, subida, e2e (decenas de minutos)
pytest                    # todo
```

Necesitan Java 17 y `pip install -e ".[dev,azure]"`. La marca `spark` (definida en `pyproject.toml`) separa los lentos.

## 2. La infraestructura de los tests

### `conftest.py`

| fixture | alcance | qué da |
|---|---|---|
| `spark` | sesión | un Spark local con Delta (`build_local_spark`) y un warehouse temporal; se crea **una vez** para todos los tests |
| `cfg` | test | una `ProjectConfig` local con **esquemas de prefijo único** (`t12345678_`) y su Volume temporal: cada test tiene su propio "catálogo" y no se pisan |
| `truth` | sesión | la verdad del banco a **1.000 clientes** (SF 0,002), generada **una sola vez** y compartida |

`conftest.py` también agrega `src/` al `sys.path` (no hace falta instalar el paquete para correr los tests).

### `fake_http.py`

Una `requests.Session` falsa: en vez de ir por red, convierte cada `get`/`post` en un `Request` y llama a
`CardApi.handle()` **en el mismo proceso**. Agrega `azure/card_api/` al path para importar `api`, `chaos` y `store`.
Gracias a esto, el `ApiClient` real habla con la API real (con su caos real) sin Azure.

## 3. Qué prueba cada archivo

| archivo | tests | qué asegura |
|---|---|---|
| `test_config.py` | 3 | nombres de 3 niveles y rutas; escala (SF 0,1 = 50 mil clientes, mínimo de comercios); que `setup` crea las tablas de `ops` |
| `test_ids.py` | 4 | CUIT de un caso conocido; que todo CUIT generado es válido; CBU; **que la versión Spark y la Python dan lo mismo** (marcado `spark`) |
| `test_sql_runner.py` | 2 | el reemplazo de variables y el corte de sentencias (incluido `$$`); que **todos los `.sql` del proyecto** se parten bien |
| `test_card_api.py` | 9 | tokens (vencimiento, firma); sin token o credenciales malas → 401; paginación completa sin caos; 422 antes de la v2; **página repetida (duplica sin perder) y salteada (pierde: hay que conciliar)**; 429 con `Retry-After`, 5xx y token vencido; 200 con error y cuerpo truncado; v1 en centavos y offset inestable; espejo del BCRA con su suciedad |
| `test_crm.py` | 3 | `crm_system`: Ids con prefijo, relaciones padre-hijo y fechas coherentes; el endpoint pagina de a 500, filtra por `modified_since` e incluye borrados; el fetcher **sobrevive al caos** y trae todo |
| `test_ingestion.py` | 6 | el cliente recupera días completos con caos `normal` y `dificil`; reintentos agotados → `ApiError`; **el borde**: un CORRUPT, duplicados y vacíos detectados, y la segunda sincronización no trae nada; bronze de archivos (dos contratos, `canal_digital`, formato argentino tal cual, nada se pierde, segunda corrida = 0 filas); bronze de la API con watermark (sin duplicados, re-correr no trae nada); **R14**: el día con página salteada se completa en la ronda diferida |
| `test_truth.py` | 7 | el generador: reproducible y con volumen; ids únicos y válidos; **cajas de ahorro nunca negativas**; las compras con débito están en el core; el saldo de cierre coincide con el último movimiento; los duplicados de la fusión; el fraude se concentra en tarjetas comprometidas |
| `test_exports.py` | 4 | formato viejo y nuevo del core; incidentes de entrega (archivo tarde, vacío); la migración re-envía a todos los clientes; v1 y v2 se solapan |
| `test_upload.py` | 4 | nombres de archivo del core; los archivos rotos (vacío, truncado, copia y reenvíos); páginas de 1.000 y manifiesto; nombres de la integración vieja |
| `test_e2e.py` | 7 | **de punta a punta contra la verdad** (abajo) |

## 4. El test de punta a punta (`test_e2e.py`)

La fixture `lake` arma todo el pipeline en local, una vez para los 7 tests:

1. Genera la verdad (1.000 clientes) y la exporta sucia.
2. "Sube" a un Azure en memoria (`core_uploads`, `cards_legacy_uploads`) y sincroniza al Volume con `run_sync`.
3. Bronze de archivos.
4. Levanta la API **con caos `normal`**, un reloj simulado (la espera entre rondas lo adelanta: R14) y una serie del
   dólar con forma de BCRA; ingesta autorizaciones, liquidaciones y el espejo FX con el `ApiClient` real. Exige que no
   quede ningún día incompleto.
5. Publica el CRM en la API, lo ingesta, **borra un lead** y lo ingesta de nuevo: la segunda corrida tiene que traer
   sólo lo nuevo (menos de 10 cuentas) y 1 borrado.
6. Silver, gold, marts y CRM en gold.

Después, cada test contrasta con `sim.*`:

| test | qué verifica contra la verdad |
|---|---|
| `test_movimientos_recuperan_la_verdad` | sin duplicados (DAT-01); **nada inventado** (todo movimiento de silver existe en la verdad); lo que falta es menos del 0,1 % (sólo lo que el core no mandó, DAT-10); importe con signo exacto (DAT-02); hora UTC exacta (DAT-06); moneda exacta (DAT-05) y que hubo correcciones |
| `test_movimientos_faltantes_detectados` | `balance_breaks` detecta los faltantes (hay quiebres y no más que los faltantes) |
| `test_identidad_y_scd2` | `customer_xref` encuentra **exactamente** los duplicados de la fusión; una versión vigente por cliente; la recarga de la migración **no** genera versiones (las versiones extra están acotadas por los cambios reales) |
| `test_tarjetas_v1_y_v2` | una autorización por id, ni una de más ni de menos; en el solapamiento gana la v2; importes en pesos exactos; aparece `billetera` (API-11) |
| `test_gold_y_gate` | todo movimiento tiene cliente; todo día tiene dólar (DAT-13); el mart 360 tiene exactamente los clientes vigentes y activos; **el quality gate pasa** y las reglas de aviso ven lo esperado (1 archivo corrupto, CUIT inválidos, personas duplicadas) |
| `test_crm_match_con_el_core_contra_la_verdad` | ninguna cuenta del CRM se une a **otra** persona; más del 95 % de los clientes matchea; ningún prospecto matchea; cada duplicada quedó marcada y apunta a otra principal |
| `test_crm_gold_y_borrados` | el lead borrado no está; se detectan leads que ya son clientes (DAT-18); los casos no tienen PII (DAT-11); todas las oportunidades llegan a gold |

## 5. Lo que los tests encontraron (y lo que no)

- **Encontraron:** R06 (deadlock del lock no reentrante), R08 (versiones SCD2 falsas por los acentos), R09 (Parquet en
  vez de Delta en local).
- **No encontraron:** R14 (la página salteada estable 10 minutos). En los tests el caos dependía del reloj real y la
  ventana de 10 minutos casi nunca cambiaba durante un test. Lo encontró el **quality gate** en la primera corrida
  contra Azure. Desde entonces el reloj es inyectable y hay un test específico.

La lección: los tests y el quality gate son complementarios. Los tests prueban la lógica contra una verdad conocida; el
gate prueba los datos reales de cada corrida.
