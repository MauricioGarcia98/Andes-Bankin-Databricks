"""Perfiles de caos de la API (docs/03, sección A).

Hay dos tipos de falla:
  * TRANSITORIAS (429, 5xx, timeouts, token vencido antes de tiempo): azar puro en cada pedido. Un reintento bien
    hecho las supera.
  * ESTRUCTURALES (página repetida, página salteada): dependen de (fecha, página, ventana de 10 minutos). Pedir la
    misma página enseguida da el mismo resultado; volver a recorrer el día un rato después, no. Así se aprende a
    conciliar contra los totales y a re-pedir la ventana, en vez de confiar en que "si dio 200 está bien".
"""

from __future__ import annotations

import hashlib
import random
import time
from dataclasses import dataclass


@dataclass(frozen=True)
class ChaosProfile:
    rate_limit: float = 0.0          # API-01: 429 con Retry-After
    server_error: float = 0.0        # API-02: 500/502/503
    slow: float = 0.0                # API-03: tarda más que el timeout del cliente
    early_expiry: float = 0.0        # API-04: 401 token_expired antes de tiempo
    duplicate_page: float = 0.0      # API-05: la página se sirve dos veces seguidas
    skipped_page: float = 0.0        # API-06: next_cursor saltea una página
    unstable_offset: float = 0.0     # API-07: (v1) filas que se corren entre páginas
    truncated: float = 0.0           # API-10: cuerpo cortado
    error_in_200: float = 0.0        # API-12: 200 con {"status": "error"}
    slow_seconds: float = 35.0


PROFILES = {
    "off": ChaosProfile(),
    "normal": ChaosProfile(rate_limit=0.03, server_error=0.02, slow=0.003, early_expiry=0.01,
                           duplicate_page=0.01, skipped_page=0.005, unstable_offset=0.3),
    "dificil": ChaosProfile(rate_limit=0.08, server_error=0.05, slow=0.01, early_expiry=0.03, duplicate_page=0.03,
                            skipped_page=0.015, unstable_offset=0.6, truncated=0.005, error_in_200=0.005),
}


class Chaos:
    def __init__(self, profile: str = "normal", seed: int = 1998, rng: random.Random | None = None,
                 clock=time.time):
        self.profile = PROFILES.get(profile, PROFILES["normal"])
        self.seed = seed
        self.rng = rng or random.Random()
        self.clock = clock

    def roll(self, rate: float) -> bool:
        """Falla transitoria: azar de este pedido."""
        return rate > 0 and self.rng.random() < rate

    def structural(self, rate: float, *keys) -> bool:
        """Falla estructural: estable durante 10 minutos para la misma (fecha, página)."""
        if rate <= 0:
            return False
        bucket = int(self.clock() // 600)
        h = hashlib.sha256("|".join(str(k) for k in (self.seed, bucket, *keys)).encode()).digest()
        return int.from_bytes(h[:8], "big") / 2**64 < rate
