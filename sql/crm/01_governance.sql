-- =============================================================================
-- CRM comercial: comentarios, claves y máscaras. Corre sólo si ya hay CRM en gold (etapa 08).
-- =============================================================================

COMMENT ON TABLE ${gold}.dim_cuenta_crm IS 'Cuentas del CRM comercial unidas a la persona del core. metodo_match: ID_CORE (por Core_Customer_Id__c), CUIT o SIN_MATCH. Las duplicadas (mismo CUIT) apuntan a cuenta_principal_id: para contar empresas usar COUNT(DISTINCT cuenta_principal_id).';
COMMENT ON TABLE ${gold}.fact_oportunidad IS 'Oportunidades comerciales del CRM (desde jun-2024), atribuidas a la cuenta principal y a la persona del core. Pipeline = cerrada = false.';
COMMENT ON TABLE ${gold}.fact_caso IS 'Casos (reclamos y consultas) del CRM comercial. La descripción viene sin emails ni teléfonos (se reemplazan por [EMAIL] y [TELEFONO]).';
COMMENT ON TABLE ${gold}.mart_pipeline_comercial IS 'Una fila por persona con actividad en el CRM: pipeline abierto, ganado y casos abiertos.';

ALTER TABLE ${gold}.dim_cuenta_crm ALTER COLUMN metodo_match COMMENT 'Cómo se unió al core: ID_CORE, CUIT o SIN_MATCH (prospectos o datos inservibles)';
ALTER TABLE ${gold}.dim_cuenta_crm ALTER COLUMN problema_id_core COMMENT 'Qué tenía mal el Core_Customer_Id__c: VACIO, BANCO_VIEJO (numeración anterior a la migración), INEXISTENTE o CONTRADICE_CUIT';
ALTER TABLE ${gold}.dim_cuenta_crm ALTER COLUMN segmento_desactualizado COMMENT 'El segmento cargado en el CRM no coincide con el del core';
ALTER TABLE ${gold}.fact_oportunidad ALTER COLUMN monto_ars COMMENT 'Monto estimado en pesos (vacío en productos sin monto, como tarjetas o cuenta sueldo)';
ALTER TABLE ${gold}.fact_caso ALTER COLUMN horas_resolucion COMMENT 'Horas entre la creación y el cierre del caso (vacío si sigue abierto)';

ALTER TABLE ${gold}.dim_cuenta_crm ALTER COLUMN cuenta_crm_id SET NOT NULL;
ALTER TABLE ${gold}.dim_cuenta_crm DROP CONSTRAINT IF EXISTS pk_dim_cuenta_crm CASCADE;
ALTER TABLE ${gold}.dim_cuenta_crm ADD CONSTRAINT pk_dim_cuenta_crm PRIMARY KEY (cuenta_crm_id);
ALTER TABLE ${gold}.fact_oportunidad DROP CONSTRAINT IF EXISTS fk_oportunidad_cuenta;
ALTER TABLE ${gold}.fact_oportunidad ADD CONSTRAINT fk_oportunidad_cuenta FOREIGN KEY (cuenta_principal_id) REFERENCES ${gold}.dim_cuenta_crm;
ALTER TABLE ${gold}.fact_caso DROP CONSTRAINT IF EXISTS fk_caso_cuenta;
ALTER TABLE ${gold}.fact_caso ADD CONSTRAINT fk_caso_cuenta FOREIGN KEY (cuenta_principal_id) REFERENCES ${gold}.dim_cuenta_crm;

ALTER TABLE ${gold}.dim_cuenta_crm ALTER COLUMN cuit SET MASK ${ops}.mask_documento;
ALTER TABLE ${gold}.dim_cuenta_crm SET TAGS ('dominio' = 'comercial', 'fuente' = 'crm');
ALTER TABLE ${gold}.fact_oportunidad SET TAGS ('dominio' = 'comercial', 'fuente' = 'crm');
ALTER TABLE ${gold}.fact_caso SET TAGS ('dominio' = 'comercial', 'fuente' = 'crm');
