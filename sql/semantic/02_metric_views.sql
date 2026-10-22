-- =============================================================================
-- METRIC VIEWS: las métricas OFICIALES de Andes Bank. Se definen UNA vez (YAML) y las usan
-- Genie, los dashboards y el SQL:  SELECT `Segmento`, MEASURE(`Clientes`) FROM ... GROUP BY ALL
-- Nombres en castellano "de negocio" (como los dice la gente), con sinónimos.
-- =============================================================================

CREATE OR REPLACE VIEW ${semantic}.mv_clientes
WITH METRICS LANGUAGE YAML
COMMENT 'Métricas oficiales de clientes: cantidad, personas, productos, saldos y actividad'
AS $$
version: 1.1
comment: "Una fila por cliente vigente (gold.mart_cliente_360)"
source: ${gold}.mart_cliente_360
dimensions:
  - name: Segmento
    expr: segmento
    synonyms: [segmento comercial, tipo de cliente]
  - name: Región
    expr: region
    synonyms: [zona]
  - name: Provincia
    expr: provincia
  - name: Sucursal
    expr: sucursal
  - name: Tipo de persona
    expr: tipo_persona
  - name: Banco de origen
    expr: banco_origen
  - name: Rango de edad
    expr: rango_edad
  - name: Estado
    expr: estado
  - name: Tiene tarjeta de crédito
    expr: tiene_tarjeta_credito
  - name: Tiene caja en dólares
    expr: tiene_caja_usd
  - name: Cobra el sueldo en el banco
    expr: cobra_sueldo
    synonyms: [plan sueldo, acredita haberes]
measures:
  - name: Clientes
    expr: COUNT(1)
    synonyms: [cantidad de clientes, número de clientes]
  - name: Personas
    expr: COUNT(DISTINCT persona_id)
    comment: "Personas reales (un cliente de Cuyo y uno del NOA que son la misma persona cuentan una vez)"
  - name: Productos por cliente
    expr: AVG(cantidad_productos)
    synonyms: [cross selling, penetración de productos]
  - name: Saldo total (ARS)
    expr: SUM(saldo_total_ars)
    synonyms: [depósitos, plata en el banco]
  - name: Saldo total (USD)
    expr: SUM(saldo_total_usd)
  - name: Consumo con tarjetas 90 días (ARS)
    expr: SUM(consumo_tarjetas_90d_ars)
  - name: Porcentaje digital
    expr: AVG(porcentaje_digital_90d)
    comment: "Promedio del % de movimientos por app u homebanking en los últimos 90 días"
  - name: Clientes con tarjeta de crédito (%)
    expr: AVG(CASE WHEN tiene_tarjeta_credito THEN 1.0 ELSE 0 END)
$$;

CREATE OR REPLACE VIEW ${semantic}.mv_movimientos
WITH METRICS LANGUAGE YAML
COMMENT 'Métricas oficiales de movimientos de cuenta'
AS $$
version: 1.1
source: ${semantic}.v_movimientos
dimensions:
  - name: Fecha
    expr: fecha_contable
  - name: Mes
    expr: anio_mes
  - name: Año
    expr: anio
  - name: Concepto
    expr: concepto
    synonyms: [tipo de movimiento, operación]
  - name: Canal
    expr: canal
  - name: Digital
    expr: canal_digital
  - name: Moneda
    expr: moneda
  - name: Tipo de cuenta
    expr: tipo_cuenta
  - name: Segmento
    expr: segmento
  - name: Región
    expr: region
  - name: Sucursal
    expr: sucursal
measures:
  - name: Movimientos
    expr: COUNT(1)
    synonyms: [cantidad de operaciones, transacciones]
  - name: Créditos (ARS)
    expr: SUM(CASE WHEN importe_ars > 0 THEN importe_ars ELSE 0 END)
    synonyms: [ingresos, entradas]
  - name: Débitos (ARS)
    expr: -SUM(CASE WHEN importe_ars < 0 THEN importe_ars ELSE 0 END)
    synonyms: [egresos, salidas]
  - name: Flujo neto (ARS)
    expr: SUM(importe_ars)
  - name: Flujo neto (USD)
    expr: SUM(importe_usd)
  - name: Clientes activos
    expr: COUNT(DISTINCT cliente_id)
  - name: Movimientos digitales (%)
    expr: AVG(CASE WHEN canal_digital THEN 1.0 ELSE 0 END)
$$;

CREATE OR REPLACE VIEW ${semantic}.mv_tarjetas
WITH METRICS LANGUAGE YAML
COMMENT 'Métricas oficiales de tarjetas: consumo, aprobación y fraude'
AS $$
version: 1.1
source: ${semantic}.v_autorizaciones
dimensions:
  - name: Fecha
    expr: fecha
  - name: Mes
    expr: anio_mes
  - name: Hora
    expr: hora_local
  - name: Día de la semana
    expr: dia_semana
  - name: Rubro
    expr: rubro
    synonyms: [MCC, categoría de comercio]
  - name: Comercio
    expr: comercio
  - name: Exterior
    expr: es_exterior
    synonyms: [compra en el exterior, internacional]
  - name: Rubro riesgoso
    expr: rubro_riesgoso
  - name: Tipo de tarjeta
    expr: tipo_tarjeta
  - name: Marca
    expr: marca
  - name: Modo de ingreso
    expr: modo_ingreso
  - name: Billetera
    expr: billetera
  - name: Código de respuesta
    expr: codigo_respuesta
  - name: Segmento
    expr: segmento
  - name: Región
    expr: region
measures:
  - name: Autorizaciones
    expr: COUNT(1)
  - name: Compras aprobadas
    expr: SUM(CASE WHEN aprobada THEN 1 ELSE 0 END)
  - name: Tasa de aprobación
    expr: AVG(CASE WHEN aprobada THEN 1.0 ELSE 0 END)
  - name: Rechazos por sospecha de fraude
    expr: SUM(CASE WHEN rechazo_por_sospecha THEN 1 ELSE 0 END)
    synonyms: [rechazos antifraude, código 59]
  - name: Tasa de rechazo por sospecha
    expr: AVG(CASE WHEN rechazo_por_sospecha THEN 1.0 ELSE 0 END)
  - name: Consumo aprobado (ARS)
    expr: SUM(CASE WHEN aprobada THEN importe_ars ELSE 0 END)
    synonyms: [consumo, gasto con tarjeta, facturación]
  - name: Ticket promedio (ARS)
    expr: AVG(CASE WHEN aprobada THEN importe_ars END)
  - name: Tarjetas activas
    expr: COUNT(DISTINCT token_tarjeta)
  - name: Pendientes de liquidar
    expr: SUM(CASE WHEN aprobada AND NOT liquidada THEN 1 ELSE 0 END)
$$;

CREATE OR REPLACE VIEW ${semantic}.mv_depositos
WITH METRICS LANGUAGE YAML
COMMENT 'Métricas oficiales de depósitos (stock a fin de mes)'
AS $$
version: 1.1
source: ${semantic}.v_saldos_fin_de_mes
dimensions:
  - name: Mes
    expr: anio_mes
  - name: Fecha
    expr: fecha
  - name: Moneda
    expr: moneda
  - name: Tipo de cuenta
    expr: tipo_cuenta
  - name: Segmento
    expr: segmento
  - name: Región
    expr: region
measures:
  - name: Depósitos (ARS)
    expr: SUM(saldo_ars)
    comment: "Stock a fin de mes; las cuentas en dólares valuadas al oficial de ese día"
    synonyms: [stock de depósitos, captaciones, saldos]
  - name: Depósitos (USD)
    expr: SUM(saldo_usd)
  - name: Depósitos en moneda original
    expr: SUM(saldo)
    comment: "Sumar sólo dentro de una misma moneda"
  - name: Cuentas con saldo
    expr: COUNT(1)
$$;

CREATE OR REPLACE VIEW ${semantic}.mv_plataforma_apis
WITH METRICS LANGUAGE YAML
COMMENT 'Métricas de la plataforma de datos: salud de las APIs que consume el lakehouse'
AS $$
version: 1.1
source: ${semantic}.v_api_llamadas
dimensions:
  - name: Fecha
    expr: fecha
  - name: Recurso
    expr: recurso
  - name: Resultado
    expr: resultado
    synonyms: [estado del pedido, error]
  - name: Código HTTP
    expr: status_code
  - name: Corrida
    expr: run_id
measures:
  - name: Pedidos
    expr: COUNT(1)
    synonyms: [llamadas, requests]
  - name: Pedidos fallidos
    expr: SUM(CASE WHEN resultado <> 'OK' THEN 1 ELSE 0 END)
  - name: Tasa de error
    expr: AVG(CASE WHEN resultado <> 'OK' THEN 1.0 ELSE 0 END)
  - name: Rate limits (429)
    expr: SUM(CASE WHEN status_code = 429 THEN 1 ELSE 0 END)
  - name: Latencia promedio (ms)
    expr: AVG(latency_ms)
  - name: Latencia p95 (ms)
    expr: PERCENTILE_APPROX(latency_ms, 0.95)
  - name: Filas recibidas
    expr: SUM(rows)
$$;
