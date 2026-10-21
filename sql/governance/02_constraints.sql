-- =============================================================================
-- CLAVES PRIMARIAS Y FORÁNEAS (informativas): Unity Catalog no las hace cumplir,
-- pero le dicen a Genie y a los dashboards cómo se unen las tablas.
-- =============================================================================

ALTER TABLE ${gold}.dim_fecha ALTER COLUMN fecha SET NOT NULL;
ALTER TABLE ${gold}.dim_fecha DROP CONSTRAINT IF EXISTS pk_dim_fecha;
ALTER TABLE ${gold}.dim_fecha ADD CONSTRAINT pk_dim_fecha PRIMARY KEY (fecha);

ALTER TABLE ${gold}.dim_sucursal ALTER COLUMN sucursal_id SET NOT NULL;
ALTER TABLE ${gold}.dim_sucursal DROP CONSTRAINT IF EXISTS pk_dim_sucursal;
ALTER TABLE ${gold}.dim_sucursal ADD CONSTRAINT pk_dim_sucursal PRIMARY KEY (sucursal_id);

ALTER TABLE ${gold}.dim_cuenta ALTER COLUMN cuenta_id SET NOT NULL;
ALTER TABLE ${gold}.dim_cuenta DROP CONSTRAINT IF EXISTS pk_dim_cuenta;
ALTER TABLE ${gold}.dim_cuenta ADD CONSTRAINT pk_dim_cuenta PRIMARY KEY (cuenta_id);

ALTER TABLE ${gold}.dim_tarjeta ALTER COLUMN token_tarjeta SET NOT NULL;
ALTER TABLE ${gold}.dim_tarjeta DROP CONSTRAINT IF EXISTS pk_dim_tarjeta;
ALTER TABLE ${gold}.dim_tarjeta ADD CONSTRAINT pk_dim_tarjeta PRIMARY KEY (token_tarjeta);

ALTER TABLE ${gold}.dim_comercio ALTER COLUMN comercio_id SET NOT NULL;
ALTER TABLE ${gold}.dim_comercio DROP CONSTRAINT IF EXISTS pk_dim_comercio;
ALTER TABLE ${gold}.dim_comercio ADD CONSTRAINT pk_dim_comercio PRIMARY KEY (comercio_id);

ALTER TABLE ${gold}.mart_cliente_360 ALTER COLUMN cliente_id SET NOT NULL;
ALTER TABLE ${gold}.mart_cliente_360 DROP CONSTRAINT IF EXISTS pk_mart_cliente_360;
ALTER TABLE ${gold}.mart_cliente_360 ADD CONSTRAINT pk_mart_cliente_360 PRIMARY KEY (cliente_id);

ALTER TABLE ${gold}.fact_movimiento DROP CONSTRAINT IF EXISTS fk_mov_cuenta;
ALTER TABLE ${gold}.fact_movimiento ADD CONSTRAINT fk_mov_cuenta FOREIGN KEY (cuenta_id) REFERENCES ${gold}.dim_cuenta;
ALTER TABLE ${gold}.fact_movimiento DROP CONSTRAINT IF EXISTS fk_mov_fecha;
ALTER TABLE ${gold}.fact_movimiento ADD CONSTRAINT fk_mov_fecha FOREIGN KEY (fecha_contable) REFERENCES ${gold}.dim_fecha;
ALTER TABLE ${gold}.fact_autorizacion DROP CONSTRAINT IF EXISTS fk_aut_tarjeta;
ALTER TABLE ${gold}.fact_autorizacion ADD CONSTRAINT fk_aut_tarjeta FOREIGN KEY (token_tarjeta) REFERENCES ${gold}.dim_tarjeta;
ALTER TABLE ${gold}.fact_autorizacion DROP CONSTRAINT IF EXISTS fk_aut_comercio;
ALTER TABLE ${gold}.fact_autorizacion ADD CONSTRAINT fk_aut_comercio FOREIGN KEY (comercio_id) REFERENCES ${gold}.dim_comercio;
ALTER TABLE ${gold}.fact_autorizacion DROP CONSTRAINT IF EXISTS fk_aut_fecha;
ALTER TABLE ${gold}.fact_autorizacion ADD CONSTRAINT fk_aut_fecha FOREIGN KEY (fecha) REFERENCES ${gold}.dim_fecha;
ALTER TABLE ${gold}.fact_saldo_diario DROP CONSTRAINT IF EXISTS fk_saldo_cuenta;
ALTER TABLE ${gold}.fact_saldo_diario ADD CONSTRAINT fk_saldo_cuenta FOREIGN KEY (cuenta_id) REFERENCES ${gold}.dim_cuenta;
