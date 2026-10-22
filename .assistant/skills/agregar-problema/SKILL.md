---
name: agregar-problema
description: Cómo inyectar un problema nuevo en los datos o en las APIs de Andes Bank y cerrarlo de punta a punta.
---
# Agregar un problema al catálogo

1. Darle un ID (`DAT-NN`, `API-NN`, `ING-NN`) y una fila en `docs/03_catalogo_de_problemas.md`: cómo aparece, cómo se
   detecta, dónde se resuelve, si entra en el perfil `normal` o `dificil`.
2. Inyectarlo: datos → `generator/exports.py` (con una constante de proporción); API → `azure/card_api/chaos.py`
   (transitorio con `roll`, estructural con `structural`).
3. Resolverlo en silver con una función propia que lleve el ID en el comentario.
4. Medirlo: una `Rule` en `quality/gate.py` con `catalog_id` = el ID.
5. Probarlo: un test que lo provoque y verifique que el pipeline recupera la verdad.
