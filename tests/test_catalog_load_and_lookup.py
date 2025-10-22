"""Tests for catalog loading and lookup helpers."""
from __future__ import annotations

import types

import pytest

import catalog_store
import db_integration


@pytest.fixture(autouse=True)
def _reset_store() -> None:
    catalog_store.clear()
    yield
    catalog_store.clear()


def test_catalog_load_and_lookup(monkeypatch: pytest.MonkeyPatch) -> None:
    schema = {
        "materials_table": "MATERIAL",
        "code_id_col": "MATERIALCODE",
        "code_name_col": "MATERIAL",
        "barcode_table": "BARCODE",
        "code_col": "CODE",
        "fk_col": "STORAGEMATERIALCODE",
    }
    monkeypatch.setattr(db_integration, "detect_catalog_schema", lambda cur: schema, raising=False)

    def fake_fetch_all(query: str, params: tuple, *, cur=None):
        if "FROM MATERIAL" in query:
            return [("1001", "Кафе Арабика"), ("1002", "Чай Зелен")]
        if "FROM BARCODE" in query:
            return [("3801234567890", "1001")]
        return []

    monkeypatch.setattr(db_integration, "fetch_all", fake_fetch_all, raising=False)

    session = types.SimpleNamespace(cur=object())
    materials_count, barcodes_count = db_integration._load_catalog_for_profile(session, "Local TEST")

    assert materials_count == 2
    assert barcodes_count == 1
    assert catalog_store.is_loaded_for("Local TEST")

    by_code = catalog_store.get_material("1002")
    assert by_code and by_code["name"] == "Чай Зелен"

    by_barcode = catalog_store.get_material_by_barcode("3801234567890")
    assert by_barcode and by_barcode["code"] == "1001"

    fuzzy = catalog_store.find_best_match("кафе арабика")
    assert fuzzy and fuzzy["code"] == "1001"
    assert int(fuzzy["score"]) >= 85
