# 07 · El posteo de LinkedIn

> Primero el posteo terminado, con los números reales del proyecto (22-oct-2026). Abajo, las preguntas guía con las que
> se armó, respondidas.

![Andes Bank en números](diagramas/17_en_numeros.svg)

## El posteo

> 🏦 **Construí un banco argentino de mentira para aprender Databricks de verdad.**
>
> *(Borrador: ajustalo a tu voz antes de publicarlo.)*
>
> Andes Bank no existe, pero sus datos se portan como los de un banco real: 50 mil clientes, 17 millones de
> movimientos, una API de tarjetas que tira errores a propósito (rate limits, tokens que vencen, páginas que se
> repiten o se saltean), archivos del core que llegan tarde, vacíos o rotos, un CRM con clientes duplicados y la
> cotización oficial del dólar desde la API real del BCRA.
>
> Arriba de eso armé el lakehouse completo en **Databricks Free Edition**: Auto Loader, un cliente de API que
> concilia cada día contra los totales, SCD2 para la historia de los clientes, resolución de identidad (la misma
> persona con dos números de cliente por una fusión de 2008), un modelo dimensional con montos en pesos y en dólares,
> 22 reglas de calidad y gobierno en Unity Catalog con máscaras de PII.
>
> Y en el centro, **Genie**: cuatro agentes (Comercial, Tarjetas y fraude, Finanzas, Plataforma de datos) que
> responden en castellano. Los medí: aciertan entre el **80 y el 100 %** de sus benchmarks. Cuando uno no llegaba,
> casi nunca era Genie: era el modelo de datos (una tabla que decía "clientes vigentes" y traía también a los dados de
> baja). Arreglé sobre todo la base (el grano de esa tabla y los comentarios de las columnas), sumé dos aclaraciones
> al agente, y Comercial pasó de 70 % a 90 %.
>
> El error que más me enseñó: en la primera corrida real, el quality gate frenó porque a dos días les faltaban
> **exactamente 1.000 registros**: una página que la API salteaba durante 10 minutos. Los tests no lo habían visto. El
> quality gate, sí.
>
> Lo construí con **Claude Code** como compañero: yo decidía el diseño y qué medir; le pedía código, tests y
> explicaciones; y corregía lo que no me cerraba (como cuando el CRM tuvo que dejar de ser Salesforce porque el login
> por API no había forma de destrabarlo).
>
> Costo de Azure del mes: **USD 0,02**. Todo es código y está en el repo, con un curso para reconstruirlo desde cero.
>
> #Databricks #DataEngineering #Genie #Lakehouse #Azure

**Imagen:** `docs/diagramas/17_en_numeros.svg` (o `00_panorama.svg`).

## Las preguntas guía, respondidas

| # | pregunta | respuesta corta (con el número) |
|---|---|---|
| 1 | ¿Qué proyecto hice? | Un banco argentino simulado, de punta a punta en Databricks Free Edition: 4 fuentes, medallion completo, 4 Genie Agents |
| 2 | ¿Por qué? | Para practicar con problemas reales (37 catalogados, cada uno con su arreglo y su regla) y exprimir Free Edition |
| 3 | ¿Cómo usé la IA? | Claude Code construyó la referencia; yo decidí, medí y corregí. La bitácora tiene 18 errores reales con su causa |
| 6 | El problema más difícil | La página salteada estable 10 minutos (R14): reintentar enseguida no sirve; hay que seguir y volver en rondas |
| 7 | ¿Qué pasa cuando la API falla? | 6.151 pedidos: 163 rate limits, 106 errores 5xx, 78 tokens vencidos, y ningún día quedó incompleto |
| 8 | Límites de Free Edition | Sin *external locations*: se copia de Blob a un Volume con una SAS de sólo lectura (ADR 0002). A SF 1 (172 M de movimientos) el generador corre en 43 min |
| 9 | Costo de Azure | USD 0,02 en el mes (tope: USD 10) |
| 10 | PII | Máscaras de columna y filtros por región en Unity Catalog; la PII escrita a mano en los casos del CRM se redacta en silver |
| 12 | ¿Dónde se equivocó Genie? | En "clientes vigentes": la tabla traía también a los dados de baja. Se arregló primero el dato (el grano y los comentarios) y después dos instrucciones |
| 13 | ¿Cómo lo medí? | `scripts/genie_benchmark.py`: 66 benchmarks, 59 aciertos (89 %); cada agent entre 80 y 100 % |
| 14 | Una decisión que cambié | Salesforce real → CRM simulado servido por la misma API de Azure (bitácora R17) |
| 15 | El número que muestra la escala | 17,2 M de movimientos a SF 0,1 y 172 M a SF 1; 8.747 archivos validados en el borde |
| 18 | ¿Cómo se reproduce? | El repo + `construir/`: 28 capítulos con el código, los errores reales y un examen final |

Las preguntas completas, para la próxima versión del posteo:

## Las preguntas centrales

1. **¿Qué proyecto hice?** Un banco argentino simulado, de punta a punta en Databricks, con fuentes reales (APIs en
   Azure, BCRA) y datos sintéticos.
2. **¿Por qué lo hice?** Para practicar con problemas reales y para exprimir Databricks Free Edition al máximo.
3. **¿Cómo usé la IA?** Claude Code como acompañante: qué decidí yo, qué le pedí, qué corregí de lo que propuso y qué
   aprendí de sus explicaciones. No "la IA lo hizo".
4. **¿Qué aprendí de Databricks?**

## Para darle más valor

5. ¿Por qué un **banco** y no otra industria? (regulación, PII, conciliación, dos monedas)
6. ¿Cuál fue el **problema inyectado más difícil** de resolver y cómo lo resolví?
7. ¿Qué le pasa a un pipeline cuando la **API falla** (429, token vencido, página repetida) y cómo lo diseñé para
   que no duplique ni pierda datos?
8. ¿Hasta dónde llegué con **Free Edition** y qué límites encontré?
9. ¿Cuánto me costó **Azure**? (el número real del mes)
10. ¿Cómo protegí la **PII** aunque los datos fueran sintéticos?
11. ¿Qué le puede preguntar un **gerente** a Genie? ¿Y un **data engineer**?
12. ¿Dónde se **equivocó Genie** y cómo lo arreglé? (casi siempre era el modelo de datos, no las instrucciones)
13. ¿Cómo **medí** que Genie responde bien? (benchmarks y porcentaje de aciertos)
14. ¿Qué **decisión de arquitectura** cambié a mitad de camino y por qué?
15. ¿Qué **número** muestra la escala? (filas, GB, pedidos a la API, tiempo de la corrida)
16. ¿Qué haría **distinto en un banco real**?
17. ¿Qué le recomendaría a alguien que **arranca** con Databricks?
18. ¿Cómo lo puede **reproducir** otra persona? (repo y guía `construir/`)
19. ¿Qué parte **no** le delegaría nunca a una IA?
20. ¿Qué **sigue**? (AML, préstamos, streaming, un modelo de fraude)

## Material para juntar mientras construís

- Una captura de Genie respondiendo con el SQL visible.
- Un gráfico de `ops.api_calls`: pedidos, reintentos y 429 por día.
- El diagrama de arquitectura.
- La tabla de "problema → dónde se resuelve".
- La factura de Azure del mes.
