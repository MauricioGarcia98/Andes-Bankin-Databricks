"""Mini-DSL para escribir diagramas de Excalidraw como código (sin dependencias).

Cada diagrama es una función que dibuja con `Diagram`: zonas, cajas con texto, flechas entre cajas y textos sueltos.
`Diagram.save()` escribe un `.excalidraw` válido (se abre y se edita en https://excalidraw.com o con la extensión de
VS Code). Las coordenadas son a mano, en una grilla de 20 px: así el diagrama queda igual en cada regeneración.
"""

from __future__ import annotations

import json
import random
from dataclasses import dataclass, field
from pathlib import Path

# paleta (rellenos pastel + bordes) — la misma en todos los diagramas
FILL = {"azul": "#a5d8ff", "verde": "#b2f2bb", "naranja": "#ffd8a8", "violeta": "#d0bfff", "rojo": "#ffc9c9",
        "amarillo": "#fff3bf", "turquesa": "#c3fae8", "rosa": "#eebefa", "gris": "#e9ecef", "blanco": "#ffffff"}
STROKE = {"azul": "#1971c2", "verde": "#2f9e44", "naranja": "#e8590c", "violeta": "#6741d9", "rojo": "#e03131",
          "amarillo": "#f08c00", "turquesa": "#0c8599", "rosa": "#a61e4d", "gris": "#495057", "blanco": "#495057"}
ZONE = {"azul": "#e7f5ff", "verde": "#ebfbee", "naranja": "#fff4e6", "violeta": "#f3f0ff", "gris": "#f8f9fa",
        "amarillo": "#fff9db", "turquesa": "#e6fcf5", "rosa": "#fff0f6", "rojo": "#fff5f5"}
INK = "#1e1e1e"
MUTED = "#495057"
FONT = 1           # 1 = Virgil (a mano), 2 = Helvetica, 3 = Cascadia (código)
LINE = 1.25


def _text_size(text: str, size: float, family: int = FONT) -> tuple[float, float]:
    lines = text.split("\n")
    k = 0.6 if family == 3 else 0.55
    return max(len(line) for line in lines) * size * k, len(lines) * size * LINE


@dataclass
class Box:
    id: str
    x: float
    y: float
    w: float
    h: float

    @property
    def cx(self) -> float:
        return self.x + self.w / 2

    @property
    def cy(self) -> float:
        return self.y + self.h / 2

    def anchor(self, side: str) -> tuple[float, float]:
        return {"r": (self.x + self.w, self.cy), "l": (self.x, self.cy),
                "b": (self.cx, self.y + self.h), "t": (self.cx, self.y)}[side]


@dataclass
class Diagram:
    name: str
    title: str
    elements: list[dict] = field(default_factory=list)
    boxes: dict[str, Box] = field(default_factory=dict)

    def __post_init__(self):
        self._rng = random.Random(self.name)
        self._n = 0

    # ------------------------------------------------------------------------------------- base
    def _id(self, prefix: str) -> str:
        self._n += 1
        return f"{prefix}{self._n}"

    def _base(self, kind: str, eid: str, x, y, w, h, **kw) -> dict:
        el = {"id": eid, "type": kind, "x": x, "y": y, "width": w, "height": h, "angle": 0,
              "strokeColor": INK, "backgroundColor": "transparent", "fillStyle": "solid", "strokeWidth": 2,
              "strokeStyle": "solid", "roughness": 1, "opacity": 100, "groupIds": [], "frameId": None,
              "roundness": None, "seed": self._rng.randrange(1, 2**31), "version": 1,
              "versionNonce": self._rng.randrange(1, 2**31), "isDeleted": False, "boundElements": [],
              "updated": 1, "link": None, "locked": False}
        el.update(kw)
        self.elements.append(el)
        return el

    def _label(self, container: dict, text: str, size: float, color: str, family: int = FONT,
               align: str = "center", valign: str = "middle") -> dict:
        tw, th = _text_size(text, size, family)
        x = container["x"] + (container["width"] - tw) / 2 if align == "center" else container["x"] + 12
        y = container["y"] + (container["height"] - th) / 2 if valign == "middle" else container["y"] + 10
        tid = self._id("t")
        container["boundElements"].append({"type": "text", "id": tid})
        return self._base("text", tid, x, y, tw, th, strokeColor=color, text=text, originalText=text,
                          fontSize=size, fontFamily=family, textAlign=align, verticalAlign=valign,
                          containerId=container["id"], lineHeight=LINE, autoResize=True)

    # --------------------------------------------------------------------------------- dibujar
    def text(self, x, y, text: str, size: float = 20, color: str = INK, family: int = FONT) -> dict:
        tw, th = _text_size(text, size, family)
        return self._base("text", self._id("t"), x, y, tw, th, strokeColor=color, text=text, originalText=text,
                          fontSize=size, fontFamily=family, textAlign="left", verticalAlign="top",
                          containerId=None, lineHeight=LINE, autoResize=True)

    def heading(self, x, y, subtitle: str | None = None) -> None:
        self.text(x, y, self.title, 36)
        if subtitle:
            self.text(x, y + 52, subtitle, 20, MUTED)

    def zone(self, x, y, w, h, title: str, color: str = "gris", size: float = 22) -> None:
        self._base("rectangle", self._id("z"), x, y, w, h, backgroundColor=ZONE[color], strokeColor=STROKE[color],
                   strokeWidth=1, strokeStyle="dashed", roundness={"type": 3}, roughness=0)
        self.text(x + 16, y + 12, title, size, STROKE[color])

    def box(self, bid: str, x, y, w, h, text: str, color: str = "azul", size: float = 18, shape: str = "rectangle",
            family: int = FONT, dashed: bool = False) -> Box:
        el = self._base(shape, bid, x, y, w, h, backgroundColor=FILL[color], strokeColor=STROKE[color],
                        roundness={"type": 3} if shape == "rectangle" else {"type": 2},
                        strokeStyle="dashed" if dashed else "solid")
        self._label(el, text, size, INK, family)
        self.boxes[bid] = Box(bid, x, y, w, h)
        return self.boxes[bid]

    def note(self, x, y, w, h, text: str, color: str = "amarillo", size: float = 16) -> None:
        el = self._base("rectangle", self._id("n"), x, y, w, h, backgroundColor=FILL[color],
                        strokeColor=STROKE[color], strokeWidth=1, roundness={"type": 3})
        self._label(el, text, size, MUTED, align="left", valign="top")

    def arrow(self, a: str, b: str, label: str | None = None, sides: str | None = None, color: str = INK,
              dashed: bool = False, via: list[tuple[float, float]] | None = None, size: float = 16,
              both: bool = False, seg: int | None = None) -> None:
        """Flecha de la caja `a` a la caja `b`. `sides` = lado de salida + lado de llegada ("rl", "bt"...);
        si falta, se elige por la posición relativa. `via` agrega codos (coordenadas absolutas) y `seg` elige en qué
        tramo va el texto."""
        A, B = self.boxes[a], self.boxes[b]
        if not sides:
            dx, dy = B.cx - A.cx, B.cy - A.cy
            sides = ("rl" if dx > 0 else "lr") if abs(dx) * 0.6 > abs(dy) else ("bt" if dy > 0 else "tb")
        p0, p1 = A.anchor(sides[0]), B.anchor(sides[1])
        pts = [p0, *(via or []), p1]
        rel = [[px - p0[0], py - p0[1]] for px, py in pts]
        xs, ys = [p[0] for p in rel], [p[1] for p in rel]
        el = self._base("arrow", self._id("a"), p0[0], p0[1], max(xs) - min(xs), max(ys) - min(ys),
                        strokeColor=color, strokeStyle="dashed" if dashed else "solid", roundness=None,
                        points=rel, lastCommittedPoint=None, startArrowhead="arrow" if both else None,
                        endArrowhead="arrow", elbowed=False,
                        startBinding={"elementId": a, "focus": 0, "gap": 4},
                        endBinding={"elementId": b, "focus": 0, "gap": 4})
        for bid in (a, b):
            next(e for e in self.elements if e["id"] == bid)["boundElements"].append({"type": "arrow", "id": el["id"]})
        if label:
            # el texto va en el medio del tramo `seg` (por defecto, el más largo)
            i = seg if seg is not None else max(
                range(len(pts) - 1), key=lambda k: abs(pts[k + 1][0] - pts[k][0]) + abs(pts[k + 1][1] - pts[k][1]))
            mx, my = (pts[i][0] + pts[i + 1][0]) / 2, (pts[i][1] + pts[i + 1][1]) / 2
            tw, th = _text_size(label, size)
            horizontal = abs(pts[i + 1][0] - pts[i][0]) >= abs(pts[i + 1][1] - pts[i][1])
            # al costado de la línea (arriba si es horizontal, a la derecha si es vertical), no encima
            x, y = (mx - tw / 2, my - th - 6) if horizontal else (mx + 8, my - th / 2)
            self._base("text", self._id("t"), x, y, tw, th, strokeColor=color, text=label, originalText=label,
                       fontSize=size, fontFamily=FONT, textAlign="left", verticalAlign="top", containerId=None,
                       lineHeight=LINE, autoResize=True)

    # ------------------------------------------------------------------------------------ salida
    def save(self, folder: Path) -> Path:
        doc = {"type": "excalidraw", "version": 2, "source": "https://github.com/MauricioGarcia98/Andes-Bankin-Datbricks",
               "elements": self.elements,
               "appState": {"viewBackgroundColor": "#ffffff", "gridSize": 20, "exportBackground": True,
                            "exportWithDarkMode": False, "exportPadding": 30},
               "files": {}}
        path = folder / f"{self.name}.excalidraw"
        path.write_text(json.dumps(doc, ensure_ascii=False, indent=1) + "\n", encoding="utf-8")
        return path
