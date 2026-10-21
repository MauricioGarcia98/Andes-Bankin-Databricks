-- =============================================================================
-- FUNCIONES CERTIFICADAS (trusted assets): preguntas importantes con UNA respuesta oficial.
-- Genie las usa en vez de inventar el SQL; el resultado es siempre el mismo.
-- Ojo: los parámetros de fecha van como STRING 'yyyy-MM-dd'. Con DATE, Genie acepta el deploy pero después rechaza
-- TODAS las preguntas del agent (INVALID_CERTIFIED_ANSWER_FUNCTION_EXCEPTION, lección de Andes Viento Austral).
-- =============================================================================

CREATE OR REPLACE FUNCTION ${semantic}.fn_resumen_cliente(id_cliente STRING)
RETURNS TABLE (cliente_id STRING, nombre STRING, segmento STRING, region STRING, sucursal STRING, productos INT,
               saldo_total_ars DECIMAL(38,2), saldo_total_usd DECIMAL(38,2), consumo_tarjetas_90d_ars DECIMAL(38,2),
               porcentaje_digital_90d DOUBLE, cobra_sueldo BOOLEAN, ultimo_movimiento DATE)
COMMENT 'Ficha de un cliente: productos, saldos, consumo y actividad. Uso: SELECT * FROM fn_resumen_cliente(''CUY-0000502'')'
RETURN SELECT cliente_id, nombre_completo, segmento, region, sucursal, CAST(cantidad_productos AS INT), saldo_total_ars,
              saldo_total_usd, consumo_tarjetas_90d_ars, porcentaje_digital_90d, cobra_sueldo, ultimo_movimiento
       FROM ${gold}.mart_cliente_360 WHERE cliente_id = id_cliente;

CREATE OR REPLACE FUNCTION ${semantic}.fn_alertas_fraude(desde STRING, hasta STRING)
RETURNS TABLE (token_tarjeta STRING, cliente_id STRING, fecha DATE, compras INT, compras_rubro_riesgoso INT,
               compras_exterior INT, monto_ars DECIMAL(38,2), rechazos_por_sospecha INT)
COMMENT 'Tarjetas con patrón de fraude: 3 o más compras en un día en rubros riesgosos o en el exterior. Uso: SELECT * FROM fn_alertas_fraude(''2026-01-01'', ''2026-01-31'')'
RETURN SELECT a.token_tarjeta, a.cliente_id, a.fecha, CAST(count(*) AS INT),
              CAST(sum(CASE WHEN co.rubro_riesgoso THEN 1 ELSE 0 END) AS INT),
              CAST(sum(CASE WHEN co.es_exterior THEN 1 ELSE 0 END) AS INT),
              CAST(sum(a.importe_ars) AS DECIMAL(38,2)), CAST(sum(CASE WHEN a.rechazo_por_sospecha THEN 1 ELSE 0 END) AS INT)
       FROM ${gold}.fact_autorizacion a JOIN ${gold}.dim_comercio co ON co.comercio_id = a.comercio_id
       WHERE a.fecha BETWEEN to_date(desde) AND to_date(hasta) AND (co.rubro_riesgoso OR co.es_exterior)
       GROUP BY a.token_tarjeta, a.cliente_id, a.fecha
       HAVING count(*) >= 3;

CREATE OR REPLACE FUNCTION ${semantic}.fn_posicion_dolares(al_dia STRING)
RETURNS TABLE (fecha DATE, cuentas BIGINT, depositos_usd DECIMAL(38,2), usd_oficial DECIMAL(18,4), valuacion_ars DECIMAL(38,2))
COMMENT 'Depósitos en dólares al último fin de mes anterior o igual a la fecha, valuados al oficial. Uso: SELECT * FROM fn_posicion_dolares(''2025-06-30'')'
RETURN SELECT s.fecha, count(*), CAST(sum(s.saldo) AS DECIMAL(38,2)), max(f.usd_oficial), CAST(sum(s.saldo_ars) AS DECIMAL(38,2))
       FROM ${gold}.fact_saldo_diario s JOIN ${gold}.dim_fecha f ON f.fecha = s.fecha
       WHERE s.moneda = 'USD' AND s.es_fin_de_mes
         AND s.fecha = (SELECT max(fecha) FROM ${gold}.fact_saldo_diario WHERE es_fin_de_mes AND fecha <= to_date(al_dia))
       GROUP BY s.fecha;

CREATE OR REPLACE FUNCTION ${semantic}.fn_salud_ingesta(dias INT)
RETURNS TABLE (recurso STRING, pedidos BIGINT, ok BIGINT, rate_limit BIGINT, token_vencido BIGINT, error_servidor BIGINT,
               timeout_o_red BIGINT, respuesta_invalida BIGINT, latencia_p95_ms DOUBLE)
COMMENT 'Salud de las APIs en los últimos N días: pedidos y fallas por tipo. Uso: SELECT * FROM fn_salud_ingesta(7)'
RETURN SELECT recurso, count(*), sum(CASE WHEN resultado = 'OK' THEN 1 ELSE 0 END),
              sum(CASE WHEN resultado = 'RATE_LIMIT' THEN 1 ELSE 0 END), sum(CASE WHEN resultado = 'TOKEN_VENCIDO' THEN 1 ELSE 0 END),
              sum(CASE WHEN resultado = 'ERROR_SERVIDOR' THEN 1 ELSE 0 END), sum(CASE WHEN resultado = 'TIMEOUT_O_RED' THEN 1 ELSE 0 END),
              sum(CASE WHEN resultado = 'RESPUESTA_INVALIDA' THEN 1 ELSE 0 END), percentile_approx(latency_ms, 0.95)
       FROM ${semantic}.v_api_llamadas WHERE fecha >= current_date() - dias GROUP BY recurso;
