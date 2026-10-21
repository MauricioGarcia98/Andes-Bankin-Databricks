from andes_bank.config import ProjectConfig
from andes_bank.sql_runner import SQL_DIR, render, split_statements


def test_render_y_split():
    cfg = ProjectConfig()
    text = render("SELECT 1 FROM ${gold}.t;\n-- comentario;\nCREATE VIEW v AS $$\na: 1;\nb: 2\n$$;\nSELECT 2", cfg)
    stmts = split_statements(text)
    assert stmts[0] == "SELECT 1 FROM andes_bank.gold.t"
    assert stmts[1].startswith("CREATE VIEW v AS $$") and "a: 1;" in stmts[1]
    assert stmts[2] == "SELECT 2"


def test_los_archivos_del_proyecto_se_parten_bien():
    import os

    cfg = ProjectConfig()
    for folder in ("governance", "semantic"):
        for f in os.listdir(os.path.join(SQL_DIR, folder)):
            with open(os.path.join(SQL_DIR, folder, f), encoding="utf-8") as fh:
                stmts = split_statements(render(fh.read(), cfg))
            assert stmts and all("${" not in s for s in stmts), f
            assert all(s.count("$$") % 2 == 0 for s in stmts), f
