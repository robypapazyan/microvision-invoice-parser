"""Tests for simple catalog detection logic."""

from __future__ import annotations

from typing import Any, Dict, List, Tuple

import mistral_db


class _MetaCursor:
    def __init__(self, columns: Dict[str, List[str]]) -> None:
        self._columns = {key.upper(): [col.upper() for col in value] for key, value in columns.items()}
        self._rows: List[Tuple[str]] = []

    def execute(self, sql: str, params: Tuple[Any, ...] | None = None) -> None:
        table = (params or ("",))[0]
        table_name = str(table).upper()
        if "FROM RDB$RELATION_FIELDS" in sql.upper():
            cols = self._columns.get(table_name, [])
            self._rows = [(col,) for col in cols]
        else:
            self._rows = []

    def fetchall(self) -> List[Tuple[str]]:
        return list(self._rows)

    def fetchone(self) -> Tuple[str] | None:
        return self._rows[0] if self._rows else None


def test_detect_schema_prefers_fk_storagematerialcode() -> None:
    cursor = _MetaCursor(
        {
            "MATERIAL": ["MATERIALCODE", "MATERIAL"],
            "BARCODE": ["CODE", "FK_STORAGEMATERIALCODE"],
        }
    )
    schema = mistral_db.detect_catalog_schema(cursor, force_refresh=True)
    assert schema["code_col"] == "CODE"
    assert schema["fk_col"] == "FK_STORAGEMATERIALCODE"


def test_detect_schema_falls_back_to_storage_column() -> None:
    cursor = _MetaCursor(
        {
            "MATERIAL": ["MATERIALCODE", "MATERIAL"],
            "BARCODE": ["CODE", "STORAGEMATERIALCODE"],
        }
    )
    schema = mistral_db.detect_catalog_schema(cursor, force_refresh=True)
    assert schema["fk_col"] == "STORAGEMATERIALCODE"

