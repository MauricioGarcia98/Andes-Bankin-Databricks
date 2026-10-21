# 01 · La empresa: Andes Bank S.A.

> Banco **ficticio**, construido con los problemas de un banco mediano real en la Argentina. Los números macro
> (inflación, dólar) están inspirados en 2023-2026 y redondeados: no son datos oficiales. Las cotizaciones del dólar,
> en cambio, salen de la **API real del BCRA**.

## Historia en hitos (cada uno deja una marca en los datos)

| año | hito | qué dejó en los datos |
|---|---|---|
| 1998 | Nace como banco cooperativo en **Mendoza**, con sucursales en Cuyo. | Los clientes viejos tienen el DNI con puntos (`20.345.678`) y domicilios sin código postal. |
| 2008 | Compra un banco regional del NOA (Salta, Jujuy, Tucumán). | Dos numeraciones de cliente: `CUY-000123` y `NOA-45678`. Hay personas duplicadas entre ambas. |
| 2015 | Lanza tarjetas de crédito propias con un **procesador externo** (Andes Card Processing). | El procesador manda los montos en **centavos** y las fechas en UTC. |
| 2019 | Cepo cambiario: cuentas en USD con restricciones, "dólar oficial" vs. otros tipos de cambio. | Movimientos en ARS y USD mezclados; el tipo de cambio aplicado no siempre se informa. |
| 2022 | **Migración del core bancario** a un sistema nuevo. | Antes de jul-2022 los archivos vienen en `latin1` con `;` y `dd/MM/yyyy`; después, UTF-8 con `,` e ISO. Saldos iniciales cargados con un movimiento "AJUSTE MIGRACION". |
| 2023 | Devaluación del 13 de diciembre (el oficial pasa de ~366 a ~800). | Montos en ARS que se multiplican; los reportes en USD cambian de golpe. |
| 2024 | Pasa del CRM casero a **Salesforce**. | Cuentas de Salesforce sin el ID del core; el *match* se hace por CUIT/DNI, con errores. |
| 2025 | El procesador de tarjetas publica la **API v2** (paginación por cursor, campos nuevos). | Conviven v1 (offset) y v2 (cursor) durante 3 meses: *schema drift* y duplicados. |
| 2026 | Auditoría del BCRA sobre **protección de datos** y conciliación de saldos. | Se exige enmascarar PII y explicar cada diferencia de saldo. |

## Áreas y quién pregunta qué

| área | persona (ficticia) | qué necesita saber | su Genie Agent |
|---|---|---|---|
| Comercial | Lucía Ferreyra, gerenta de banca individual | conversión de leads, productos por cliente, oportunidades por sucursal | Comercial y clientes |
| Tarjetas y fraude | Martín Ibarra, jefe de prevención de fraude | rechazos, montos atípicos, comercios riesgosos | Tarjetas y fraude |
| Finanzas y tesorería | Carolina Sosa, tesorera | depósitos en ARS y USD, posición en dólares, impacto del tipo de cambio | Finanzas y tesorería |
| Data Engineering | el equipo de datos (vos) | qué ingestas fallaron, cuántos 429 dio la API, cuántas filas en cuarentena | Plataforma de datos |
| Data Analytics | analistas de cada área | la métrica oficial, de qué tabla sale, cómo se calcula | todos (con el SQL visible) |
| Cumplimiento (post-MVP) | Rodrigo Paz, oficial de cumplimiento | operaciones inusuales, reportes al BCRA | Cumplimiento y AML |

## Sucursales y regiones (para filtros por fila)

| región | provincias | sucursales a SF 1 |
|---|---|---|
| Cuyo | Mendoza, San Juan, San Luis | 28 |
| NOA | Salta, Jujuy, Tucumán | 19 |
| Centro | Córdoba, CABA, Buenos Aires | 15 |
| Digital | sin sucursal (clientes 100 % app) | — |

Un gerente de la región Cuyo, por ejemplo, ve en Genie **sólo** los clientes de Cuyo (row filter de Unity Catalog).

## Productos (MVP)

- **Cuentas**: caja de ahorro en ARS y en USD y cuenta corriente.
- **Tarjetas**: débito (asociada a la caja de ahorro) y crédito (Visa y Master, procesadas por Andes Card Processing).
- **CRM**: leads y oportunidades para préstamos personales, tarjetas y paquetes (los préstamos se cargan post-MVP).
