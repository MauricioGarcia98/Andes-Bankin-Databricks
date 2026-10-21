-- =============================================================================
-- MÁSCARAS DE COLUMNA Y FILTROS DE FILA (requisito de la auditoría BCRA 2026)
-- -----------------------------------------------------------------------------
--   * DNI, CUIT, email, teléfono y fecha de nacimiento: completos sólo para Cumplimiento y Comercial;
--     el resto ve una versión enmascarada (lo justo para reconocer el dato sin exponerlo).
--   * CBU: últimos 4 dígitos salvo Operaciones y Cumplimiento.
--   * Cada gerente regional ve SÓLO los clientes de su región (row filter con ops.region_access:
--     principal, region; '*' = todas). Los admins y quien corre el job ven todo.
-- =============================================================================

CREATE TABLE IF NOT EXISTS ${ops}.region_access (principal STRING, region STRING, area STRING, granted_at TIMESTAMP)
COMMENT 'Quién ve qué región en las tablas de clientes (row filter). region = * ve todas';

MERGE INTO ${ops}.region_access t
USING (SELECT current_user() AS principal, '*' AS region, 'Datos' AS area, current_timestamp() AS granted_at) s
ON t.principal = s.principal AND t.region = s.region
WHEN NOT MATCHED THEN INSERT *;

CREATE OR REPLACE FUNCTION ${ops}.puede_ver_pii()
RETURNS BOOLEAN
COMMENT 'Verdadero para Cumplimiento y Comercial (o admins)'
RETURN is_member('admins') OR is_account_group_member('andes_cumplimiento') OR is_account_group_member('andes_comercial');

CREATE OR REPLACE FUNCTION ${ops}.mask_documento(valor STRING)
RETURNS STRING
COMMENT 'DNI/CUIT: completo para quien puede ver PII; si no, sólo los últimos 3 dígitos'
RETURN CASE WHEN ${ops}.puede_ver_pii() THEN valor ELSE concat('*****', right(valor, 3)) END;

CREATE OR REPLACE FUNCTION ${ops}.mask_dni(valor BIGINT)
RETURNS BIGINT
COMMENT 'DNI numérico: nulo para quien no puede ver PII'
RETURN CASE WHEN ${ops}.puede_ver_pii() THEN valor ELSE NULL END;

CREATE OR REPLACE FUNCTION ${ops}.mask_email(email STRING)
RETURNS STRING
COMMENT 'juan.perez@gmail.com -> j*******@gmail.com'
RETURN CASE WHEN ${ops}.puede_ver_pii() THEN email ELSE regexp_replace(email, '^(.)[^@]*', '$1*******') END;

CREATE OR REPLACE FUNCTION ${ops}.mask_texto(valor STRING)
RETURNS STRING
COMMENT 'Teléfono u otro dato personal: [RESERVADO] para quien no puede ver PII'
RETURN CASE WHEN ${ops}.puede_ver_pii() THEN valor ELSE '[RESERVADO]' END;

CREATE OR REPLACE FUNCTION ${ops}.mask_fecha(valor DATE)
RETURNS DATE
COMMENT 'Fecha de nacimiento: sólo el año (1 de enero) para quien no puede ver PII'
RETURN CASE WHEN ${ops}.puede_ver_pii() THEN valor ELSE make_date(year(valor), 1, 1) END;

CREATE OR REPLACE FUNCTION ${ops}.mask_cbu(cbu STRING)
RETURNS STRING
COMMENT 'CBU: últimos 4 dígitos salvo Operaciones y Cumplimiento'
RETURN CASE WHEN is_member('admins') OR is_account_group_member('andes_operaciones')
                 OR is_account_group_member('andes_cumplimiento') THEN cbu
            ELSE concat('******************', right(cbu, 4)) END;

CREATE OR REPLACE FUNCTION ${ops}.filtro_region(region STRING)
RETURNS BOOLEAN
COMMENT 'Row filter: el usuario ve las regiones asignadas en ops.region_access (o todas si es admin o tiene *)'
RETURN is_member('admins')
    OR EXISTS (SELECT 1 FROM ${ops}.region_access a
               WHERE a.principal = current_user() AND (a.region = '*' OR a.region = region));

ALTER TABLE ${gold}.dim_cliente ALTER COLUMN dni SET MASK ${ops}.mask_dni;
ALTER TABLE ${gold}.dim_cliente ALTER COLUMN cuit SET MASK ${ops}.mask_documento;
ALTER TABLE ${gold}.dim_cliente ALTER COLUMN email SET MASK ${ops}.mask_email;
ALTER TABLE ${gold}.dim_cliente ALTER COLUMN telefono SET MASK ${ops}.mask_texto;
ALTER TABLE ${gold}.dim_cliente ALTER COLUMN fecha_nacimiento SET MASK ${ops}.mask_fecha;
ALTER TABLE ${gold}.dim_cuenta ALTER COLUMN cbu SET MASK ${ops}.mask_cbu;

ALTER TABLE ${gold}.dim_cliente SET ROW FILTER ${ops}.filtro_region ON (region);
ALTER TABLE ${gold}.mart_cliente_360 SET ROW FILTER ${ops}.filtro_region ON (region);
