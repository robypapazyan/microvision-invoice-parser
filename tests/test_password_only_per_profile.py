"""Tests for profile-scoped password-only login."""
from __future__ import annotations

import types

import pytest

import catalog_store
import db_integration


class DummyConn:
    def close(self) -> None:  # pragma: no cover - stub
        pass


@pytest.fixture(autouse=True)
def _reset_state(monkeypatch: pytest.MonkeyPatch) -> None:
    catalog_store.clear()
    monkeypatch.setattr(db_integration, "_PROFILE_CACHE", None, raising=False)
    monkeypatch.setattr(db_integration, "_PASSWORD_ONLY_CACHE", None, raising=False)
    yield
    catalog_store.clear()
    monkeypatch.setattr(db_integration, "_PROFILE_CACHE", None, raising=False)
    monkeypatch.setattr(db_integration, "_PASSWORD_ONLY_CACHE", None, raising=False)


@pytest.fixture
def profiles(monkeypatch: pytest.MonkeyPatch) -> dict[str, dict[str, object]]:
    payload = {
        "Local TEST": {
            "database": "test.fdb",
            "password_only": {"4321": {"username": "test", "id": 1}},
        },
        "Книжарница": {
            "database": "shop.fdb",
            "password_only": {"9999": {"username": "shop", "id": 2}},
        },
    }

    monkeypatch.setattr(db_integration, "_load_profiles", lambda: payload, raising=False)
    monkeypatch.setattr(
        db_integration,
        "_load_profile",
        lambda key: dict(payload[key]),
        raising=False,
    )
    return payload


@pytest.fixture
def patched_dependencies(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(db_integration, "get_catalog_preview", lambda: {}, raising=False)
    monkeypatch.setattr(db_integration, "catalog_tables_loaded", lambda: False, raising=False)
    monkeypatch.setattr(db_integration, "get_catalog_counts", lambda: {}, raising=False)
    monkeypatch.setattr(
        db_integration,
        "_load_catalog_for_profile",
        lambda session, profile: (0, 0),
        raising=False,
    )
    monkeypatch.setattr(
        db_integration,
        "_ensure_connection",
        lambda session, label, profile: (DummyConn(), DummyConn()),
        raising=False,
    )


def test_password_only_success_and_scoped(monkeypatch: pytest.MonkeyPatch, profiles, patched_dependencies) -> None:
    login_calls: list[tuple[str, str]] = []

    def fake_login_user(username: str, password: str, *, pc_id=None):
        login_calls.append((username, password))
        return 101, username

    monkeypatch.setattr(db_integration, "login_user", fake_login_user, raising=False)

    session = types.SimpleNamespace(profile_name="Local TEST", profile_data=profiles["Local TEST"], output_logger=None)

    result = db_integration.perform_login(session, "", "4321", profile_key="Local TEST")
    assert result["login"] == "test"
    assert session.profile_label == "Local TEST"
    assert login_calls == [("test", "4321")]

    session_fail = types.SimpleNamespace(profile_name="Книжарница", profile_data=profiles["Книжарница"], output_logger=None)
    failure = db_integration.perform_login(session_fail, "", "4321", profile_key="Книжарница")
    assert "error" in failure
    assert failure["error"] == "Невалидна парола."
