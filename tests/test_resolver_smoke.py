"""Smoke tests for the database resolver and mapping helpers."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import db_integration


FIXTURES_DIR = Path(__file__).with_name("fixtures")


class _ResolverCursor:
    def __init__(self, material_rows: List[Tuple[Any, ...]]) -> None:
        self.material_rows = material_rows
        self.metadata: Dict[str, List[str]] = {
            "MATERIAL": ["MATERIALCODE", "MATERIAL"],
            "BARCODE": ["CODE", "FK_STORAGEMATERIALCODE"],
        }
        self.last_sql: Optional[str] = None
        self.last_params: Tuple[Any, ...] = ()
        self._rows: List[Tuple[Any, ...]] = []

    def execute(self, sql: str, params: Tuple[Any, ...] = ()) -> None:
        self.last_sql = sql
        self.last_params = params
        sql_upper = sql.upper()
        if "RDB$RELATION_FIELDS" in sql_upper:
            table = str(params[0]).upper()
            cols = self.metadata.get(table, [])
            self._rows = [(col,) for col in cols]
        elif "JOIN" in sql_upper or "FROM MATERIAL" in sql_upper:
            self._rows = list(self.material_rows)
        else:
            self._rows = []

    def fetchone(self) -> Optional[Tuple[Any, ...]]:
        return self._rows[0] if self._rows else None

    def fetchall(self) -> List[Tuple[Any, ...]]:
        return list(self._rows)


def _load_sample_name() -> str:
    content = FIXTURES_DIR.joinpath("firsttenmaterialbookstore.TXT").read_text(
        encoding="cp1251", errors="ignore"
    )
    for line in content.splitlines():
        line = line.strip()
        if line and not line.startswith("=") and not line.startswith("MATERIAL"):
            return line.split()[0]
    return "ТЕТРАДКА"


def test_resolver_generates_expected_barcode_sql() -> None:
    cursor = _ResolverCursor([("42", "Примерен артикул")])
    resolver = db_integration.DbItemResolver(cursor)
    hit = resolver.resolve_by_barcode("1234567890123")
    assert hit == {"code": "42", "name": "Примерен артикул"}
    sql_text = (cursor.last_sql or "").upper()
    assert "JOIN BARCODE" in sql_text
    assert "B.CODE" in sql_text
    assert cursor.last_params == ("1234567890123",)


def test_resolver_generates_name_query() -> None:
    sample = _load_sample_name()
    cursor = _ResolverCursor([("77", sample)])
    resolver = db_integration.DbItemResolver(cursor)
    results = resolver.resolve_by_name("тетрадка", limit=5)
    assert results[0]["code"] == "77"
    assert "LIKE UPPER(?)" in (cursor.last_sql or "")
    assert cursor.last_params == ("%тетрадка%",)


def test_mapping_normalization_and_persistence(tmp_path: Path) -> None:
    mapping_path = tmp_path / "mapping.json"
    mapping = db_integration.Mapping(mapping_path)
    sample_text = "  Тетрадка   линия   "
    mapping.set_mapped_text("Книжарница", sample_text, "105")
    mapping.set_mapped_barcode("Книжарница", "0123456789", "105")
    data = json.loads(mapping_path.read_text(encoding="utf-8"))
    normalized = db_integration.Mapping.normalize_text(sample_text)
    supplier = data["suppliers"]["Книжарница"]
    assert supplier["by_text"][normalized] == "105"
    assert supplier["by_barcode"]["0123456789"] == "105"

