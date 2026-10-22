# 14 · Etapa 10 · El cliente de API robusto

## 🎯 Objetivo

Un cliente HTTP que **sobreviva** a la API con caos del capítulo 09 y que nunca dé por bueno un día incompleto:

- reintentos con *backoff* y *jitter*, respetando `Retry-After`;
- renovación del token sin perder la página;
- validación del **cuerpo**, no sólo del código HTTP;
- conciliación de cada día contra los totales, y una segunda vuelta si falta algo;
- cada pedido (también los fallidos) registrado en `ops.api_calls`.

**Archivos:** `src/andes_bank/ingestion/api_client.py`, `tests/fake_http.py`, `tests/test_ingestion.py`.

![La API con caos y el cliente que la sobrevive](../docs/diagramas/07_api_caos_cliente.svg)

## 📚 Conceptos

- **Backoff exponencial con *jitter*:** esperar 1, 2, 4, 8... segundos, multiplicado por un azar entre 0,5 y 1,5.
  Sin el azar, todos los clientes que fallaron juntos vuelven juntos y vuelven a tirar la API.
- **Idempotencia:** un `GET` se puede repetir sin efectos. Por eso un timeout se reintenta sin miedo.
- **Conciliar contra totales:** la API publica cuántos registros tiene cada día (`daily-totals`). Si recibiste
  menos, falta algo, **aunque todas las respuestas hayan sido 200**.
- **`FakeSession`:** una `requests.Session` falsa que llama a `CardApi.handle` en el mismo proceso. Los tests del
  cliente corren contra la API **de verdad**, con caos, sin red.

## 🧱 Paso a paso

### 1. Un pedido, con todas sus fallas (`ApiClient.get`)

```python
if r.status_code == 401 and token_refreshes < 3:              # API-04
    self._bearer(force=True)
    attempt -= 1                                                # renovar no gasta un intento
    continue
if r.status_code == 429:                                       # API-01
    wait = float(r.headers.get("Retry-After", 1)) + self.rng.random()
    self.sleep(wait)
    continue
if r.status_code >= 500:                                       # API-02
    self.sleep(self._backoff(attempt))
    continue
try:
    body = r.json()
except ValueError:                                             # API-10: JSON cortado
    self.sleep(self._backoff(attempt))
    continue
if isinstance(body, dict) and body.get("status") == "error":   # API-12: 200 con error adentro
    self.sleep(self._backoff(attempt))
    continue
```

Cada rama llama antes a `self._log(...)`: el pedido queda en memoria y `flush_calls` lo vuelca a `ops.api_calls`.

### 2. Un día, completo (`fetch_day`)

```python
expected = self.get(f"v2/{resource}/daily-totals", {date_param: day})["records"]
while passes < max_passes and len(records) < expected:
    passes += 1
    params = {date_param: day}
    while True:
        body = self.get(f"v2/{resource}", params)
        for rec in body["data"]:
            if rec[id_field] in records:
                dups += 1                                      # API-05: página repetida
            records[rec[id_field]] = rec                       # dedupe por id
        if not body.get("has_more"):
            break
        params = {"cursor": body["next_cursor"]}
```

### 3. El token, sin carreras

`_bearer()` renueva el token cuando le quedan menos de 60 segundos. Como varios hilos piden días en paralelo, la
renovación va con un lock... **reentrante** (ver R06 abajo).

## 🤔 Por qué así

- **¿Por qué no `requests` con `Retry` de urllib3?** Porque no sabe de tokens que vencen, ni de "200 con error", ni
  de conciliar contra totales. Esas son reglas del negocio de esta API.
- **¿Por qué registrar también los pedidos fallidos?** Porque "¿cuántos 429 nos devolvió la API esta semana?" es una
  pregunta del equipo de datos, y el Genie de Plataforma la responde con `ops.api_calls`.

## 🧪 Probalo

```bash
pytest -q tests/test_ingestion.py -k api
pytest -q tests/test_crm.py -k caos              # el mismo cliente contra el CRM con caos "dificil"
```

## 🔥 Si falla

| síntoma | causa | arreglo |
|---|---|---|
| Los tests quedan colgados, al 0 % de CPU | **deadlock**: `_bearer()` toma el lock y al renovar, `_log()` lo vuelve a tomar | `threading.RLock()` (R06) |
| `Reintentos agotados` en días puntuales | API caída un rato largo | el job tiene `max_retries: 2` con 5 minutos de espera; el watermark retoma |
| Un día sigue incompleto aunque se reintenta | página salteada estable 10 minutos | seguir con los demás días y volver en rondas (capítulo 15, R14) |

## ✅ Punto de control

`git diff etapa-10 -- src/andes_bank/ingestion/api_client.py`

## 🏋️ Ejercicio

Agregá un *circuit breaker*: si 10 pedidos seguidos dan 5xx, el cliente deja de pedir 60 segundos. ¿Cómo lo testeás
con `FakeSession` sin esperar 60 segundos de verdad?
