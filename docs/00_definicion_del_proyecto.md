# 00 · Definición del proyecto

> **Estado:** aprobado. Este documento es el "contrato" del proyecto; cualquier cambio se registra como ADR en `docs/adr/`.

## En una frase

**Andes Bank** es un banco argentino ficticio, regulado por el BCRA, cuyo lakehouse en **Databricks Free Edition**
recibe datos de **fuentes reales con datos sintéticos** (APIs en Azure, Azure Blob Storage, un CRM comercial y la
API pública del BCRA), con **problemas inyectados a propósito** en las APIs, en los datos y en la ingesta, y donde
**Genie** es la forma en que cada área del banco le pregunta a los datos en castellano.

## Por qué existe

1. **Practicar con algo que se parezca a un trabajo real**: un banco tiene sistemas viejos, proveedores externos,
   APIs que fallan, datos sensibles y un regulador. Eso no aparece en los tutoriales.
2. **Exprimir Databricks Free Edition** hasta sus límites y documentar dónde están esos límites.
3. **Aprender a diseñar para la falla**: que una API devuelva 429 o una página repetida no tiene que romper el
   pipeline ni duplicar datos.
4. **Poner a Genie en el centro**: el objetivo final no es una tabla, es que un gerente comercial, un analista de
   fraude o un data engineer puedan preguntar y obtener una respuesta correcta y auditable.
5. **Poder contarlo**: el proyecto termina con un posteo de LinkedIn ([07](07_posteo_linkedin.md)) y una guía para
   reconstruirlo desde cero (`construir/`).

## Alcance

### MVP (lo que se construye primero)

| dominio | sistema origen | cómo llega a Databricks |
|---|---|---|
| **Core bancario**: clientes, cuentas, movimientos | archivos batch nocturnos del core | Azure Blob Storage → Auto Loader |
| **Tarjetas**: autorizaciones y liquidaciones | API REST del "procesador de tarjetas" (Azure Functions) | cliente Python robusto → Bronze |
| **CRM**: cuentas, contactos, leads, oportunidades, casos | **CRM comercial** (modelo de Salesforce, servido por la API de Azure) | REST incremental → Bronze |
| **FX y tasas**: cotizaciones oficiales | **API pública real del BCRA** + un espejo simulado con caos | cliente Python → Bronze |

### Después del MVP (para crecer)

Préstamos y mora · Alertas AML/fraude · Clickstream de app y homebanking · Reclamos y BCRA (régimen informativo) ·
Inversiones (plazos fijos, FCI).

### Fuera de alcance

- Datos reales de personas. **Todo dato personal es sintético** (nombres, DNI, CUIT, CBU), generado con semilla fija.
- Modelos de ML productivos (se puede agregar un scoring de fraude como extra, no es objetivo).
- Alta disponibilidad, DR y redes privadas: se documentan en "cómo sería en un banco real", no se construyen.

## Decisiones tomadas

| tema | decisión | por qué |
|---|---|---|
| Nombre | **Andes Bank**, argentino, regulado por BCRA | se parece a la realidad que conocés; da problemas reales (ARS/USD, CER/UVA, feriados, cepo) |
| Plataforma de datos | Databricks **Free Edition**, verificada con LinkedIn | la verificación desbloquea el acceso saliente a internet: Databricks puede llamar a las APIs (patrón *pull*) |
| Nube para las fuentes | Azure pago por uso, **tope USD 10/mes** con alertas | ver [04 · costos](04_arquitectura.md#costos-tope-usd-10mes) |
| CRM | simulado, con el modelo de datos de Salesforce, servido por la misma API de Azure que tarjetas | se probó con una org real de Salesforce y con HubSpot: el login (token atado a la contraseña, rangos de IP) y el registro trababan el proyecto sin enseñar nada del lakehouse ([bitácora R17](08_bitacora.md)) |
| Herramientas | Databricks CLI, Azure CLI, Python, GitHub | todo como código y reproducible; Claude Code las usa para probar y crear |
| Idioma | castellano rioplatense, igual que Andes Viento Austral | coherencia con el proyecto anterior |
| Estructura del repo | la misma que Viento Austral: `docs/`, `construir/` por etapas con tags, `genie/agents` | ya probada; el curso se puede seguir igual |
| Escala | `scale_factor` (SF): 0,1 por defecto, 1 para la prueba de volumen | Free Edition tiene cuota diaria de cómputo |
| Forma de trabajo | **Claude Code construye el proyecto completo y funcionando** (referencia), con un commit y un tag `etapa-NN` por etapa en la rama `curso`. Después **lo borrás entero** (Databricks, Azure y repo) y **lo volvés a construir solo** siguiendo `construir/` | igual que Andes Viento Austral: primero ves el resultado andando, después lo hacés vos; los tags son tu punto de control |

## Cómo se trabaja con la IA (regla del proyecto)

Claude Code **acompaña**, no reemplaza:
- **Vos decidís**: el negocio, el alcance, qué problemas inyectar, qué se acepta como "bien resuelto".
- **Claude Code propone y ejecuta con tu revisión**: borradores de diseño, código, comandos de CLI, diagnósticos de
  errores, explicaciones.
- **Todo queda escrito**: cada decisión en un ADR, cada error real en la bitácora (`docs/08_bitacora.md`), y la guía
  `construir/` está pensada para que lo vuelvas a hacer **sin** IA.

## Criterios de "terminado" del MVP

1. Las 4 fuentes llegan a Bronze con el caos activado y **sin duplicados ni pérdidas** (conciliado contra el total
   que publica cada fuente).
2. Silver y Gold pasan el *quality gate*; cada problema del [catálogo](03_catalogo_de_problemas.md) tiene su
   detección, su arreglo y su regla de calidad.
3. Los 4 Genie Agents del MVP responden bien al menos el **80 %** de sus benchmarks.
4. Todo se despliega con `databricks bundle deploy` y la infraestructura de Azure con un script de `az`.
5. El gasto de Azure del mes queda por debajo de USD 10.

6. Existe `construir/01_borrar_todo.md` con el orden para desmontar todo (Databricks, Azure, GitHub) sin
   perder la referencia, y `construir/` cubre cada etapa para rehacerlo sin IA.

## Estado

**MVP terminado** (22-oct-2026): se cumplen los 6 criterios de arriba (Genie entre 80 y 100 % por agent, Azure USD 0,02 en el mes, gate en verde).

Definición **aprobada** (21-oct-2026): volúmenes de [02](02_fuentes_mvp.md), perfil `normal` del
[catálogo](03_catalogo_de_problemas.md) para el MVP y la App como extra después del MVP
([05](05_genie.md#la-app-para-qué-sirve)).
