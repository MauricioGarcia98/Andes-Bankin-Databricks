-- =============================================================================
-- VISTAS ANCHAS: fuente de las metric views. Unen el hecho con sus dimensiones
-- una sola vez, acá, para que nadie (ni Genie) tenga que adivinar los joins.
-- =============================================================================

CREATE OR REPLACE VIEW ${semantic}.v_movimientos
COMMENT 'Movimientos de cuenta con cliente (segmento, región, sucursal) y calendario'
AS SELECT m.*, c.segmento, c.tipo_persona, c.region, c.banco_origen, s.sucursal, f.anio, f.mes, f.anio_mes, f.es_dia_habil
FROM ${gold}.fact_movimiento m
LEFT JOIN ${gold}.dim_cliente c ON c.cliente_id = m.cliente_id AND c.is_current
LEFT JOIN ${gold}.dim_sucursal s ON s.sucursal_id = m.sucursal_id
LEFT JOIN ${gold}.dim_fecha f ON f.fecha = m.fecha_contable;

CREATE OR REPLACE VIEW ${semantic}.v_autorizaciones
COMMENT 'Autorizaciones de tarjeta con comercio (rubro, exterior), cliente (segmento, región) y calendario'
AS SELECT a.*, co.comercio, co.rubro, co.es_exterior, co.rubro_riesgoso, c.segmento, c.region, f.anio_mes, f.dia_semana
FROM ${gold}.fact_autorizacion a
LEFT JOIN ${gold}.dim_comercio co ON co.comercio_id = a.comercio_id
LEFT JOIN ${gold}.dim_cliente c ON c.cliente_id = a.cliente_id AND c.is_current
LEFT JOIN ${gold}.dim_fecha f ON f.fecha = a.fecha;

CREATE OR REPLACE VIEW ${semantic}.v_saldos_fin_de_mes
COMMENT 'Saldo de cada cuenta a fin de mes (stock de depósitos) con cliente y valuación en pesos y dólares'
AS SELECT s.*, c.segmento, c.region, c.tipo_persona, date_format(s.fecha, 'yyyy-MM') AS anio_mes
FROM ${gold}.fact_saldo_diario s
LEFT JOIN ${gold}.dim_cliente c ON c.cliente_id = s.cliente_id AND c.is_current
WHERE s.es_fin_de_mes;

-- Plataforma de datos: la operación del lakehouse, para que el equipo de datos también le pregunte a Genie
CREATE OR REPLACE VIEW ${semantic}.v_api_llamadas
COMMENT 'Cada pedido HTTP a las APIs (procesador de tarjetas, espejo del BCRA) con su resultado, intento y latencia'
AS SELECT run_id, called_at, to_date(called_at) AS fecha, method, endpoint,
          CASE WHEN endpoint LIKE '%authorizations%' THEN 'autorizaciones'
               WHEN endpoint LIKE '%settlements%' THEN 'liquidaciones'
               WHEN endpoint LIKE 'fx/%' THEN 'cotizaciones (espejo)'
               WHEN endpoint LIKE '%oauth%' THEN 'token' ELSE endpoint END AS recurso,
          status_code,
          CASE WHEN status_code = 200 AND error IS NULL THEN 'OK'
               WHEN status_code = 429 THEN 'RATE_LIMIT'
               WHEN status_code = 401 THEN 'TOKEN_VENCIDO'
               WHEN status_code >= 500 THEN 'ERROR_SERVIDOR'
               WHEN status_code IS NULL THEN 'TIMEOUT_O_RED'
               WHEN status_code = 200 THEN 'RESPUESTA_INVALIDA'
               ELSE 'ERROR_CLIENTE' END AS resultado,
          attempt, latency_ms, rows, error
FROM ${ops}.api_calls;

CREATE OR REPLACE VIEW ${semantic}.v_calidad
COMMENT 'Resultado de cada regla de calidad en cada corrida (ver docs/03 para el catálogo de problemas)'
AS SELECT run_id, checked_at, to_date(checked_at) AS fecha, rule AS regla, table_name AS tabla, severity AS severidad,
          catalog_id AS problema, description AS descripcion, failed_rows AS filas_que_fallan, total_rows AS filas_totales,
          passed AS paso, error
FROM ${ops}.dq_results;

CREATE OR REPLACE VIEW ${semantic}.v_archivos_recibidos
COMMENT 'Archivos que llegaron de los sistemas origen (Azure Blob) y qué se hizo con cada uno en el borde'
AS SELECT container AS sistema, blob_name AS archivo, split(blob_name, '/')[0] AS fuente,
          to_date(concat_ws('-', split(blob_name, '/')[1], split(blob_name, '/')[2], split(blob_name, '/')[3])) AS fecha_entrega,
          status AS estado, data_rows AS filas, size_bytes, detail AS detalle, synced_at
FROM ${ops}.landing_files;
