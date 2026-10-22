-- =============================================================================
-- COMENTARIOS: cada tabla y columna de gold explicada en el idioma del negocio.
-- Es lo PRIMERO que lee Genie para entender qué significa cada dato. Un comentario
-- vale más que diez instrucciones en el agent.
-- =============================================================================

COMMENT ON TABLE ${gold}.dim_fecha IS 'Calendario 2023-2026 con feriados nacionales y el dólar oficial del BCRA. Fines de semana y feriados repiten la última cotización publicada.';
ALTER TABLE ${gold}.dim_fecha ALTER COLUMN usd_oficial COMMENT 'Pesos por dólar, cotización oficial del BCRA (Com. A 3500) del día o la última publicada';
ALTER TABLE ${gold}.dim_fecha ALTER COLUMN cotizacion_publicada COMMENT 'Verdadero si el BCRA publicó cotización ese día (falso en fines de semana y feriados)';
ALTER TABLE ${gold}.dim_fecha ALTER COLUMN es_dia_habil COMMENT 'Ni fin de semana ni feriado nacional';
ALTER TABLE ${gold}.dim_fecha ALTER COLUMN anio_mes COMMENT 'Año-mes en formato yyyy-MM';

COMMENT ON TABLE ${gold}.dim_sucursal IS 'Las 62 sucursales de Andes Bank (Cuyo, NOA y Centro) más la sucursal virtual 999 de clientes 100% digitales';
ALTER TABLE ${gold}.dim_sucursal ALTER COLUMN region COMMENT 'CUYO (banco original, 1998), NOA (banco adquirido en 2008), CENTRO (desde 2016) o DIGITAL';

COMMENT ON TABLE ${gold}.dim_cliente IS 'Clientes con historia (SCD2): una fila por versión. Para el estado actual filtrar is_current = true. persona_id agrupa los números de cliente que son la misma persona (fusión Cuyo/NOA de 2008).';
ALTER TABLE ${gold}.dim_cliente ALTER COLUMN cliente_id COMMENT 'Número de cliente del core: CUY- (Cuyo), NOA- (banco del NOA adquirido en 2008) o AB- (clientes nuevos)';
ALTER TABLE ${gold}.dim_cliente ALTER COLUMN persona_id COMMENT 'Persona real: si alguien tiene dos números de cliente (CUY- y NOA-), los dos apuntan al mismo persona_id. Usar para contar PERSONAS';
ALTER TABLE ${gold}.dim_cliente ALTER COLUMN segmento COMMENT 'MASIVO, PLUS, PREMIUM o PYME';
ALTER TABLE ${gold}.dim_cliente ALTER COLUMN tipo_persona COMMENT 'HUMANA (persona física) o JURIDICA (empresa)';
ALTER TABLE ${gold}.dim_cliente ALTER COLUMN estado COMMENT 'ACTIVO o BAJA (cliente que cerró su relación con el banco)';
ALTER TABLE ${gold}.dim_cliente ALTER COLUMN is_current COMMENT 'Verdadero en la versión vigente del cliente';
ALTER TABLE ${gold}.dim_cliente ALTER COLUMN valid_from COMMENT 'Desde cuándo vale esta versión (UTC)';
ALTER TABLE ${gold}.dim_cliente ALTER COLUMN valid_to COMMENT 'Hasta cuándo valió esta versión (nulo si es la vigente)';
ALTER TABLE ${gold}.dim_cliente ALTER COLUMN cuit_valido COMMENT 'Falso si el CUIT cargado en el core tiene el dígito verificador mal';
ALTER TABLE ${gold}.dim_cliente ALTER COLUMN banco_origen COMMENT 'De qué banco viene el número de cliente: Cuyo (original), NOA (adquirido 2008) o Andes Bank';

COMMENT ON TABLE ${gold}.dim_cuenta IS 'Cuentas vigentes: caja de ahorro en pesos (CA_ARS), en dólares (CA_USD) y cuenta corriente (CC_ARS)';
ALTER TABLE ${gold}.dim_cuenta ALTER COLUMN tipo_cuenta COMMENT 'CA_ARS = caja de ahorro en pesos, CA_USD = caja de ahorro en dólares, CC_ARS = cuenta corriente en pesos';
ALTER TABLE ${gold}.dim_cuenta ALTER COLUMN limite_descubierto COMMENT 'Descubierto acordado (sólo cuentas corrientes), en pesos';

COMMENT ON TABLE ${gold}.dim_tarjeta IS 'Tarjetas de débito y crédito (Visa, Mastercard) con su cliente y cuenta. El token es lo único que conoce el procesador.';
COMMENT ON TABLE ${gold}.dim_comercio IS 'Comercios donde compran los clientes, con su rubro (MCC). rubro_riesgoso marca los rubros con más fraude (electrónica, joyerías, apuestas, aerolíneas, servicios digitales)';

COMMENT ON TABLE ${gold}.fact_movimiento IS 'Movimientos de cuenta (un registro por movimiento). importe con signo: positivo = crédito (entra plata), negativo = débito (sale). importe_ars e importe_usd convertidos al dólar oficial del día contable.';
ALTER TABLE ${gold}.fact_movimiento ALTER COLUMN fecha_contable COMMENT 'Día contable del core (lo posterior a las 21 h va al día siguiente)';
ALTER TABLE ${gold}.fact_movimiento ALTER COLUMN concepto COMMENT 'ACREDITACION_SUELDO, TRANSFERENCIA_RECIBIDA, TRANSFERENCIA_ENVIADA, DEPOSITO_EFECTIVO, EXTRACCION_CAJERO, DEBITO_AUTOMATICO, PAGO_SERVICIOS, COMPRA_DEBITO, PAGO_TARJETA_CREDITO, CONSTITUCION_PLAZO_FIJO, COMISION_MANTENIMIENTO, INTERESES, SALDO_INICIAL';
ALTER TABLE ${gold}.fact_movimiento ALTER COLUMN importe COMMENT 'Importe con signo en la moneda de la cuenta (+ crédito, - débito)';
ALTER TABLE ${gold}.fact_movimiento ALTER COLUMN importe_ars COMMENT 'Importe en pesos (si la cuenta es en dólares, convertido al oficial del día)';
ALTER TABLE ${gold}.fact_movimiento ALTER COLUMN importe_usd COMMENT 'Importe en dólares al oficial del día';
ALTER TABLE ${gold}.fact_movimiento ALTER COLUMN canal_digital COMMENT 'Verdadero si se hizo por app o homebanking';
ALTER TABLE ${gold}.fact_movimiento ALTER COLUMN moneda_corregida COMMENT 'Verdadero si el core informó mal la moneda y se corrigió con la moneda de la cuenta';

COMMENT ON TABLE ${gold}.fact_autorizacion IS 'Autorizaciones de compra con tarjeta (aprobadas y rechazadas) del procesador Andes Card Processing, con su liquidación';
ALTER TABLE ${gold}.fact_autorizacion ALTER COLUMN fecha COMMENT 'Día de la compra en hora argentina';
ALTER TABLE ${gold}.fact_autorizacion ALTER COLUMN codigo_respuesta COMMENT '00_APROBADA, 05_NO_AUTORIZADA, 51_FONDOS_INSUFICIENTES, 54_TARJETA_VENCIDA, 55_PIN_INCORRECTO, 59_SOSPECHA_FRAUDE, 61_EXCEDE_LIMITE';
ALTER TABLE ${gold}.fact_autorizacion ALTER COLUMN rechazo_por_sospecha COMMENT 'Rechazada por el motor antifraude (código 59)';
ALTER TABLE ${gold}.fact_autorizacion ALTER COLUMN importe_ars COMMENT 'Importe en pesos que se le cobra al cliente (las compras en el exterior, al oficial)';
ALTER TABLE ${gold}.fact_autorizacion ALTER COLUMN llego_tarde COMMENT 'El procesador la informó días después de la compra';
ALTER TABLE ${gold}.fact_autorizacion ALTER COLUMN billetera COMMENT 'APPLE_PAY o GOOGLE_PAY (el procesador lo informa desde marzo 2026)';
ALTER TABLE ${gold}.fact_autorizacion ALTER COLUMN liquidada COMMENT 'Verdadero si el procesador ya la liquidó (normalmente 1 a 3 días después)';

COMMENT ON TABLE ${gold}.fact_saldo_diario IS 'Saldo de cierre informado por el core: cuentas con movimiento ese día y foto de TODAS las cuentas a fin de mes. Para stock de depósitos usar es_fin_de_mes = true';

COMMENT ON TABLE ${gold}.mart_cliente_360 IS 'Una fila por cliente vigente (estado ACTIVO; los dados de baja no están): productos, saldo total, consumo con tarjetas y actividad de los últimos 90 días. "Cuántos clientes" = count(*) de esta tabla; "cuántas personas" = COUNT(DISTINCT persona_id)';
ALTER TABLE ${gold}.mart_cliente_360 ALTER COLUMN tipo_persona COMMENT 'HUMANA (persona física) o JURIDICA (empresa, persona jurídica). Los clientes que son empresas: tipo_persona = JURIDICA';
ALTER TABLE ${gold}.mart_cliente_360 ALTER COLUMN cantidad_productos COMMENT 'Cuentas activas + tarjetas de crédito y débito';
ALTER TABLE ${gold}.mart_cliente_360 ALTER COLUMN porcentaje_digital_90d COMMENT 'Porcentaje de movimientos de los últimos 90 días hechos por app u homebanking';
ALTER TABLE ${gold}.mart_cliente_360 ALTER COLUMN cobra_sueldo COMMENT 'Tuvo una acreditación de sueldo en los últimos 90 días';
COMMENT ON TABLE ${gold}.mart_fraude_diario IS 'Por día, rubro y origen (exterior o no): autorizaciones, aprobadas, rechazos por sospecha de fraude y montos';
COMMENT ON TABLE ${gold}.mart_tesoreria_mensual IS 'Depósitos a fin de cada mes por moneda, tipo de cuenta, segmento y región, en moneda original, pesos y dólares';
COMMENT ON TABLE ${gold}.mart_conciliacion_tarjetas IS 'Por día: compras con débito según el procesador vs. débitos por compra según el core. solo_en_procesador > 0 = el core no registró compras aprobadas';
