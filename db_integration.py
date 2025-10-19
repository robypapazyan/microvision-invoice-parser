"""Интеграционен слой между GUI и Mistral DB."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from mistral_db import (  # type: ignore[attr-defined]
    MistralDBError,
    connect,
    create_open_delivery,
    get_last_login_trace,
    logger,
    login_user,
    push_items_to_mistral,
    _require_cursor,
)


_CLIENTS_FILE = Path(__file__).with_name("mistral_clients.json")
_PROFILE_CACHE: Optional[Dict[str, Dict[str, Any]]] = None


def _profile_label_from_profile(profile: Dict[str, Any], fallback: Optional[str] = None) -> str:
    for key in ("label", "name", "client", "profile", "profile_name"):
        value = profile.get(key)
        if value:
            return str(value)
    if fallback:
        return fallback
    database = profile.get("database")
    if database:
        return str(database)
    return "неизвестен"


def _load_profiles() -> Dict[str, Dict[str, Any]]:
    global _PROFILE_CACHE
    if _PROFILE_CACHE is not None:
        return _PROFILE_CACHE

    if not _CLIENTS_FILE.exists():
        raise MistralDBError("Липсва mistral_clients.json – няма как да се осъществи връзка.")

    try:
        with _CLIENTS_FILE.open("r", encoding="utf-8") as fh:
            data = json.load(fh)
    except json.JSONDecodeError as exc:  # pragma: no cover - защитно
        raise MistralDBError("mistral_clients.json съдържа невалиден JSON.") from exc

    profiles: Dict[str, Dict[str, Any]] = {}
    if isinstance(data, dict):
        for key, value in data.items():
            if isinstance(value, dict):
                profiles[str(key)] = value
    elif isinstance(data, list):
        for idx, item in enumerate(data):
            if not isinstance(item, dict):
                continue
            name = item.get("name") or item.get("client") or item.get("profile") or item.get("label")
            if not name:
                name = f"Профил {idx + 1}"
            profiles[str(name)] = item
    else:
        raise MistralDBError("mistral_clients.json е в неочакван формат (очаква се list или dict).")

    if not profiles:
        raise MistralDBError("В mistral_clients.json няма валидно описани профили.")

    _PROFILE_CACHE = profiles
    return profiles


def _load_profile(profile_key: str) -> Dict[str, Any]:
    profiles = _load_profiles()
    if profile_key in profiles:
        return profiles[profile_key]
    raise MistralDBError(f"Профил '{profile_key}' не е намерен в mistral_clients.json.")


def _resolve_profile(session: Any) -> Tuple[str, Dict[str, Any]]:
    profile_label = getattr(session, "profile_name", None) or getattr(session, "profile_label", None)
    profile: Optional[Dict[str, Any]] = getattr(session, "profile_data", None)

    if profile_label and (not profile or not isinstance(profile, dict)):
        profile = _load_profile(str(profile_label))

    if not profile_label:
        if profile:
            profile_label = _profile_label_from_profile(profile)
        else:
            raise MistralDBError("Липсва избран профил за връзка към база.")

    if not profile or not isinstance(profile, dict):
        profile = _load_profile(str(profile_label))

    session.profile_label = str(profile_label)
    session.profile_data = profile
    return str(profile_label), profile


def initialize_session(session: Any, profile_key: str) -> Tuple[Any, Any]:
    profile = _load_profile(profile_key)
    logger.info("Инициализация на сесия за профил: %s", profile_key)
    conn, cur = connect(profile)
    session.conn = conn
    session.cur = cur
    session.profile_label = profile_key
    session.profile_data = profile
    logger.info("Успешно свързване за профил: %s", profile_key)
    return conn, cur


def _ensure_connection(session: Any, profile_label: str, profile: Dict[str, Any]) -> Tuple[Any, Any]:
    conn = getattr(session, "conn", None)
    cur = getattr(session, "cur", None)
    if conn is not None and cur is not None:
        try:
            _require_cursor(conn, cur, profile_label)
            logger.debug("Използваме съществуваща връзка за профил: %s", profile_label)
            return conn, cur
        except MistralDBError:
            pass

    logger.info("Повторно свързване към профил: %s", profile_label)
    conn, cur = connect(profile)
    session.conn = conn
    session.cur = cur
    session.profile_label = profile_label
    session.profile_data = profile
    return conn, cur


def perform_login(session: Any, username: str, password: str) -> Dict[str, Any]:
    username = username or ""
    password = password or ""
    try:
        profile_label, profile = _resolve_profile(session)
        _ensure_connection(session, profile_label, profile)
    except MistralDBError as exc:
        logger.error("Грешка при подготовка за логин: %s", exc)
        trace = get_last_login_trace()
        session.last_login_trace = trace
        return {"error": str(exc), "trace": trace}

    logger.info(
        "Опит за логин (профил: %s, потребител: %s)",
        profile_label,
        username or "<само парола>",
    )
    try:
        operator_id, operator_login = login_user(username, password)
    except MistralDBError as exc:
        message = str(exc)
        logger.warning(
            "Логинът беше неуспешен (профил: %s, потребител: %s): %s",
            profile_label,
            username or "<само парола>",
            message,
        )
        if "Няма активна връзка" in message:
            try:
                _ensure_connection(session, profile_label, profile)
                operator_id, operator_login = login_user(username, password)
            except MistralDBError as retry_exc:
                logger.error(
                    "Повторният опит за логин се провали (профил: %s): %s",
                    profile_label,
                    retry_exc,
                )
                trace = get_last_login_trace()
                session.last_login_trace = trace
                return {"error": str(retry_exc), "trace": trace}
        else:
            trace = get_last_login_trace()
            session.last_login_trace = trace
            return {"error": message, "trace": trace}

    session.profile_label = profile_label
    logger.info(
        "Успешен логин (профил: %s, потребител: %s, оператор ID: %s)",
        profile_label,
        operator_login,
        operator_id,
    )
    trace = get_last_login_trace()
    session.last_login_trace = trace
    return {"user_id": operator_id, "login": operator_login}


def start_open_delivery(session: Any) -> int:
    profile_label, profile = _resolve_profile(session)
    _ensure_connection(session, profile_label, profile)
    _require_cursor()

    operator_id = getattr(session, "user_id", None)
    if operator_id is None:
        raise MistralDBError("Липсва оператор за OPEN доставка.")

    delivery_id = create_open_delivery(int(operator_id))
    session.open_delivery_id = delivery_id
    logger.info(
        "Създадена е OPEN доставка (профил: %s, оператор ID: %s, доставка ID: %s)",
        profile_label,
        operator_id,
        delivery_id,
    )
    return delivery_id


def last_login_trace(session: Any | None = None) -> List[Dict[str, Any]]:
    trace = None
    if session is not None:
        trace = getattr(session, "last_login_trace", None)
    if isinstance(trace, list):
        return trace
    return get_last_login_trace()


def push_parsed_rows(session: Any, rows: List[Dict[str, Any]]) -> None:
    if not rows:
        return

    profile_label, profile = _resolve_profile(session)
    _ensure_connection(session, profile_label, profile)
    _require_cursor()

    delivery_id = getattr(session, "open_delivery_id", None)
    if delivery_id is None:
        operator_id = getattr(session, "user_id", None)
        if operator_id is None:
            raise MistralDBError("Липсват активна доставка и оператор за запис на редовете.")
        delivery_id = create_open_delivery(int(operator_id))
        session.open_delivery_id = delivery_id

    operator_id = getattr(session, "user_id", None)
    push_items_to_mistral(int(delivery_id), rows)
    logger.info(
        "Изпратени са артикули към Мистрал (профил: %s, оператор ID: %s, доставка ID: %s, редове: %s)",
        profile_label,
        operator_id,
        delivery_id,
        len(rows),
    )
