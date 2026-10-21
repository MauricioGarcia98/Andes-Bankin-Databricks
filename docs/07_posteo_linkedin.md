# 07 · El posteo de LinkedIn (preguntas guía)

> Estas preguntas se responden **al final**, con números reales del proyecto. Mientras construís, anotá respuestas
> sueltas en la bitácora: un error que te costó, una decisión que cambiaste, una pregunta de Genie que te sorprendió.

## Las preguntas centrales

1. **¿Qué proyecto hice?** Un banco argentino simulado, de punta a punta en Databricks, con fuentes reales (APIs en
   Azure, Salesforce, BCRA) y datos sintéticos.
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
