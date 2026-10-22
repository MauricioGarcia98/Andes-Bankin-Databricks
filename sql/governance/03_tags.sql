-- =============================================================================
-- TAGS: dominio de cada tabla y clasificación de las columnas con datos personales.
-- Sirven para buscar ("¿dónde hay PII?") y para auditar.
-- =============================================================================

ALTER TABLE ${gold}.dim_cliente SET TAGS ('dominio' = 'clientes', 'contiene_pii' = 'si');
ALTER TABLE ${gold}.dim_cliente ALTER COLUMN dni SET TAGS ('pii' = 'documento');
ALTER TABLE ${gold}.dim_cliente ALTER COLUMN cuit SET TAGS ('pii' = 'documento');
ALTER TABLE ${gold}.dim_cliente ALTER COLUMN email SET TAGS ('pii' = 'contacto');
ALTER TABLE ${gold}.dim_cliente ALTER COLUMN telefono SET TAGS ('pii' = 'contacto');
ALTER TABLE ${gold}.dim_cliente ALTER COLUMN fecha_nacimiento SET TAGS ('pii' = 'personal');
ALTER TABLE ${gold}.dim_cuenta SET TAGS ('dominio' = 'cuentas');
ALTER TABLE ${gold}.dim_cuenta ALTER COLUMN cbu SET TAGS ('pii' = 'financiero');
ALTER TABLE ${gold}.dim_tarjeta SET TAGS ('dominio' = 'tarjetas');
ALTER TABLE ${gold}.fact_movimiento SET TAGS ('dominio' = 'cuentas');
ALTER TABLE ${gold}.fact_autorizacion SET TAGS ('dominio' = 'tarjetas');
ALTER TABLE ${gold}.fact_saldo_diario SET TAGS ('dominio' = 'tesoreria');
ALTER TABLE ${gold}.mart_cliente_360 SET TAGS ('dominio' = 'comercial');
ALTER TABLE ${gold}.mart_fraude_diario SET TAGS ('dominio' = 'fraude');
ALTER TABLE ${gold}.mart_tesoreria_mensual SET TAGS ('dominio' = 'tesoreria');
