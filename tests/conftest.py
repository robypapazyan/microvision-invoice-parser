"""Test configuration for path adjustments."""

from __future__ import annotations

import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

import pytest

import mistral_db

collect_ignore_glob = ["samples/db/test*.txt"]


@pytest.fixture(autouse=True)
def _fake_connection() -> None:
    previous_conn = getattr(mistral_db, "_CONN", None)
    previous_cur = getattr(mistral_db, "_CUR", None)
    previous_schema = getattr(mistral_db, "_CATALOG_SCHEMA", None)
    sentinel = object()
    mistral_db._CONN = sentinel  # type: ignore[attr-defined]
    mistral_db._CUR = sentinel  # type: ignore[attr-defined]
    mistral_db._CATALOG_SCHEMA = None  # type: ignore[attr-defined]
    try:
        yield
    finally:
        mistral_db._CONN = previous_conn  # type: ignore[attr-defined]
        mistral_db._CUR = previous_cur  # type: ignore[attr-defined]
        mistral_db._CATALOG_SCHEMA = previous_schema  # type: ignore[attr-defined]

