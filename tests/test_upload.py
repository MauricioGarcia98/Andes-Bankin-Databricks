"""Nombres de archivo e incidentes de entrega que arma el uploader (sin subir nada)."""

import gzip
import json

import pytest

from andes_bank.generator.exports import CORRUPT_FILE, DUPLICATE_FILE, EMPTY_FILE, LATE_FILE, export_all
from andes_bank.generator.upload import api_uploads, cards_legacy_uploads, core_uploads

pytestmark = pytest.mark.spark


@pytest.fixture(scope="module")
def staged(spark, truth):
    cfg, _ = truth
    export_all(spark, cfg)
    return cfg


def test_nombres_del_core(staged):
    names = [u.name for u in core_uploads(staged)]
    assert "movimientos/2023/06/15/MOVIMIENTOS_20230615.csv" in names
    assert "movimientos/2024/06/10/MOVIMIENTOS_20240610.csv.gz" in names
    late_from, late_to = LATE_FILE
    assert f"movimientos/{late_to:%Y/%m/%d}/MOVIMIENTOS_{late_from:%Y%m%d}.csv.gz" in names
    assert any(n.startswith("clientes/2023/01/01/CLIENTES_20230101") for n in names)


def test_incidentes_de_entrega(staged):
    ups = list(core_uploads(staged))
    by_name = {}
    for u in ups:
        by_name.setdefault(u.name, []).append(u)
    empty = by_name[f"movimientos/{EMPTY_FILE:%Y/%m/%d}/MOVIMIENTOS_{EMPTY_FILE:%Y%m%d}.csv.gz"][0]
    assert gzip.decompress(empty.data).count(b"\n") == 1                      # sólo encabezado
    assert any(n.endswith(f"MOVIMIENTOS_{EMPTY_FILE:%Y%m%d}_REENVIO.csv.gz") for n in by_name)
    corrupt = by_name[f"movimientos/{CORRUPT_FILE:%Y/%m/%d}/MOVIMIENTOS_{CORRUPT_FILE:%Y%m%d}.csv.gz"]
    assert len(corrupt) == 2 and len(corrupt[1].data) < len(corrupt[0].data)  # el último (truncado) gana
    with pytest.raises((EOFError, OSError)):
        gzip.decompress(corrupt[1].data)
    assert any(n.endswith(f"MOVIMIENTOS_{CORRUPT_FILE:%Y%m%d}_REENVIO.csv.gz") for n in by_name)
    assert f"movimientos/{DUPLICATE_FILE:%Y/%m/%d}/MOVIMIENTOS_{DUPLICATE_FILE:%Y%m%d}_copia.csv.gz" in by_name


def test_paginas_y_manifiesto(staged):
    ups = list(api_uploads(staged))
    manifests = [u for u in ups if u.name.endswith("_manifest.json")]
    assert manifests
    m = json.loads(manifests[0].data)
    day = manifests[0].name.rsplit("/", 1)[0]
    pages = [u for u in ups if u.name.startswith(day + "/page-")]
    assert m["pages"] == len(pages) and m["records"] == sum(len(json.loads(p.data)) for p in pages)
    assert all(len(json.loads(p.data)) <= 1000 for p in pages)


def test_legacy(staged):
    names = [u.name for u in cards_legacy_uploads(staged)]
    assert "autorizaciones/2024/01/10/autorizaciones_20240110.json" in names
