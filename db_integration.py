"""Интеграционен слой между GUI и Mistral DB."""
from __future__ import annotations

import csv
import json
import os
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

from mistral_db import (  # type: ignore[attr-defined]
    MistralDBError,
    connect,
    create_open_delivery,
    db_find_by_barcode,
    db_find_by_code,
    db_find_by_name_like,
    db_resolve_item,
    get_last_login_trace,
    logger,
    login_user,
    push_items_to_mistral,
    _require_cursor,
)


_CLIENTS_FILE = Path(__file__).with_name("mistral_clients.json")
_PROFILE_CACHE: Optional[Dict[str, Dict[str, Any]]] = None
_MATERIALS_FILE = Path(__file__).with_name("materials.csv")
_MAPPING_FILE = Path(__file__).with_name("mapping.json")

_MATERIALS_CACHE: Optional[Dict[str, Dict[str, Any]]] = None
_MATERIALS_BY_BARCODE: Optional[Dict[str, Dict[str, Any]]] = None
_MAPPING_CACHE: Optional[Dict[str, Dict[str, Any]]] = None


def _normalize_token(value: str | None) -> str:
    value = value or ""
    collapsed = " ".join(value.split())
    return collapsed.lower()


def _ensure_decimal(value: Any, default: Decimal) -> Decimal:
    if isinstance(value, Decimal):
        return value
    if value in (None, ""):
        return default
    try:
        return Decimal(str(value).replace(" ", "").replace(",", "."))
    except (InvalidOperation, ValueError):
        return default


def _load_materials() -> Tuple[Dict[str, Dict[str, Any]], Dict[str, Dict[str, Any]]]:
    global _MATERIALS_CACHE, _MATERIALS_BY_BARCODE
    if _MATERIALS_CACHE is not None and _MATERIALS_BY_BARCODE is not None:
        return _MATERIALS_CACHE, _MATERIALS_BY_BARCODE

    materials: Dict[str, Dict[str, Any]] = {}
    materials_by_barcode: Dict[str, Dict[str, Any]] = {}
    if not _MATERIALS_FILE.exists():
        logger.debug("materials.csv липсва – fallback ще бъде ограничен.")
        _MATERIALS_CACHE = materials
        _MATERIALS_BY_BARCODE = materials_by_barcode
        return materials, materials_by_barcode

    try:
        with _MATERIALS_FILE.open("r", encoding="cp1251", errors="ignore") as fh:
            reader = csv.DictReader(fh, delimiter=";")
            for row in reader:
                code = str(row.get("Номер") or row.get("code") or "").strip()
                name = str(row.get("Име на материал") or row.get("name") or "").strip()
                barcode = str(row.get("Баркод") or row.get("barcode") or "").strip()
                purchase_price = row.get("Последна покупна цена") or row.get("purchase_price")
                sale_price = row.get("Продажна цена") or row.get("sale_price")
                if not code:
                    continue
                material = {
                    "code": code,
                    "name": name,
                    "barcode": barcode or None,
                    "purchase_price": purchase_price,
                    "sale_price": sale_price,
                }
                materials[code] = material
                if barcode:
                    materials_by_barcode[barcode] = material
    except Exception as exc:
        logger.warning("Неуспешно зареждане на materials.csv: %s", exc)

    _MATERIALS_CACHE = materials
    _MATERIALS_BY_BARCODE = materials_by_barcode
    return materials, materials_by_barcode


def _load_mapping() -> Dict[str, Dict[str, Any]]:
    global _MAPPING_CACHE
    if _MAPPING_CACHE is not None:
        return _MAPPING_CACHE

    mapping: Dict[str, Dict[str, Any]] = {}
    if not _MAPPING_FILE.exists():
        logger.debug("mapping.json липсва – fallback ще бъде ограничен.")
        _MAPPING_CACHE = mapping
        return mapping

    try:
        with _MAPPING_FILE.open("r", encoding="utf-8") as fh:
            data = json.load(fh)
        if isinstance(data, dict):
            for key, value in data.items():
                if not isinstance(value, dict):
                    continue
                mapping[_normalize_token(str(key))] = value
    except Exception as exc:
        logger.warning("Неуспешно зареждане на mapping.json: %s", exc)

    _MAPPING_CACHE = mapping
    return mapping


def _extract_token_from_row(row: Dict[str, Any]) -> str:
    for key in ("token", "barcode", "code", "name", "description"):
        value = row.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return ""


def _fallback_from_material(code: str) -> Optional[Dict[str, Any]]:
    materials, _ = _load_materials()
    material = materials.get(code)
    if not material:
        return None
    return {
        "id": None,
        "code": material.get("code"),
        "name": material.get("name"),
        "barcode": material.get("barcode"),
        "source": "mapping",
        "match": "code",
        "purchase_price": material.get("purchase_price"),
        "sale_price": material.get("sale_price"),
    }


def _fallback_match(row: Dict[str, Any], token: str) -> Optional[Dict[str, Any]]:
    token_norm = _normalize_token(token)
    if not token_norm:
        return None

    materials, materials_by_barcode = _load_materials()
    if token in materials_by_barcode:
        candidate = materials_by_barcode[token]
        return {
            "id": None,
            "code": candidate.get("code"),
            "name": candidate.get("name"),
            "barcode": candidate.get("barcode"),
            "source": "mapping",
            "match": "barcode",
            "purchase_price": candidate.get("purchase_price"),
            "sale_price": candidate.get("sale_price"),
        }

    mapping = _load_mapping()
    entry = mapping.get(token_norm)
    if entry and isinstance(entry, dict):
        mapped_code = str(entry.get("code") or "").strip()
        mapped_name = str(entry.get("name") or "").strip()
        candidate = _fallback_from_material(mapped_code)
        if candidate:
            candidate["name"] = candidate.get("name") or mapped_name
            candidate["match"] = candidate.get("match") or "mapping"
            return candidate
        if mapped_code:
            return {
                "id": None,
                "code": mapped_code,
                "name": mapped_name,
                "barcode": None,
                "source": "mapping",
                "match": "mapping",
            }

    if token in materials:
        candidate = _fallback_from_material(token)
        if candidate:
            candidate["match"] = candidate.get("match") or "code"
        return candidate

    return None


def _extract_numeric(row: Dict[str, Any], keys: Iterable[str], default: Decimal) -> Decimal:
    for key in keys:
        if key not in row:
            continue
        value = row.get(key)
        result = _ensure_decimal(value, default)
        if result != default or value in ("0", 0, 0.0):
            return result
    return default


def _finalize_candidate(
    row: Dict[str, Any], candidate: Dict[str, Any], source: str
) -> Dict[str, Any]:
    qty = _extract_numeric(
        row,
        (
            "qty",
            "quantity",
            "Количество",
            "Кол-во",
            "count",
        ),
        Decimal("1"),
    )
    price = _extract_numeric(
        row,
        (
            "price",
            "unit_price",
            "purchase_price",
            "Ед. цена",
            "Цена",
        ),
        Decimal("0"),
    )
    vat = _extract_numeric(row, ("vat", "dds", "VAT"), Decimal("0"))
    sale_price = row.get("sale_price") or row.get("Продажна цена")
    sale_price_decimal = _ensure_decimal(sale_price, Decimal("0")) if sale_price is not None else None

    final_item = {
        "material_id": candidate.get("id"),
        "code": candidate.get("code") or row.get("code"),
        "name": candidate.get("name") or row.get("name"),
        "qty": qty,
        "price": price,
        "vat": vat,
        "barcode": candidate.get("barcode") or row.get("barcode"),
        "sale_price": sale_price_decimal,
        "source": source,
        "match_kind": candidate.get("match"),
    }
    return final_item


def apply_candidate_choice(row: Dict[str, Any], candidate: Dict[str, Any], source: str) -> Dict[str, Any]:
    final_item = _finalize_candidate(row, candidate, source)
    row["resolved"] = dict(candidate)
    row["resolved"]["source"] = source
    row["final_item"] = final_item
    return row


def resolve_items_from_db(session: Any, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if not rows:
        return []

    use_db = os.getenv("MV_USE_DB_ITEMS", "1").strip() != "0"
    stats = {
        "total": len(rows),
        "db": 0,
        "mapping": 0,
        "unresolved": 0,
        "multi": 0,
    }

    cur = getattr(session, "cur", None) if use_db else None
    resolved_rows: List[Dict[str, Any]] = []
    for row in rows:
        working = dict(row)
        token = _extract_token_from_row(working)
        working.setdefault("token", token)
        working.pop("resolved", None)
        working.pop("final_item", None)

        candidates: List[Dict[str, Any]] = []
        if use_db and token:
            try:
                candidates = db_resolve_item(cur, token)
            except MistralDBError as exc:
                logger.error("Грешка при търсене в базата: %s", exc)
                candidates = []
        if len(candidates) == 1:
            apply_candidate_choice(working, candidates[0], candidates[0].get("source", "db"))
            stats["db"] += 1
            logger.info(
                "DB resolve: еднозначно съвпадение → token=%s → код=%s",
                token,
                working["final_item"].get("code"),
            )
        elif len(candidates) > 1:
            working["resolved"] = {"candidates": candidates}
            stats["multi"] += 1
            logger.info(
                "DB resolve: multiple (%s) → need user decision",
                len(candidates),
            )
        else:
            fallback_candidate = _fallback_match(working, token)
            if fallback_candidate:
                apply_candidate_choice(working, fallback_candidate, fallback_candidate.get("source", "mapping"))
                stats["mapping"] += 1
                logger.info(
                    "DB resolve: fallback mapping → token=%s → код=%s",
                    token,
                    working["final_item"].get("code"),
                )
            else:
                working["resolved"] = None
                working["final_item"] = None
                stats["unresolved"] += 1
                logger.info("DB resolve: no match → unresolved → token=%s", token)

        resolved_rows.append(working)

    session.last_resolution_stats = stats
    return resolved_rows


def collect_db_diagnostics(session: Any) -> Dict[str, Any]:
    profile_label, profile = _resolve_profile(session)
    conn, cur = _ensure_connection(session, profile_label, profile)
    active_cur = _require_cursor(conn, cur, profile_label)
    diagnostics: Dict[str, Any] = {"profile": profile_label}

    try:
        active_cur.execute("SELECT COUNT(*) FROM MATERIAL")
        diagnostics["materials_count"] = active_cur.fetchone()[0]
    except Exception as exc:
        diagnostics["materials_error"] = str(exc)

    try:
        active_cur.execute("SELECT COUNT(*) FROM BARCODE")
        diagnostics["barcode_count"] = active_cur.fetchone()[0]
    except Exception as exc:
        diagnostics["barcode_error"] = str(exc)

    try:
        active_cur.execute(
            "SELECT FIRST 1 b.BARCODE FROM BARCODE b WHERE b.BARCODE IS NOT NULL"
        )
        barcode_row = active_cur.fetchone()
        if barcode_row and barcode_row[0]:
            sample_barcode = str(barcode_row[0]).strip()
            diagnostics["sample_barcode"] = sample_barcode
            diagnostics["sample_barcode_matches"] = db_find_by_barcode(active_cur, sample_barcode)
    except Exception as exc:
        diagnostics["sample_barcode_error"] = str(exc)

    try:
        active_cur.execute(
            "SELECT FIRST 1 m.CODE, mn.NAME FROM MATERIAL m "
            "LEFT JOIN MATERIALNAME mn ON mn.MATERIAL = m.ID AND mn.ISDEFAULT = 1 "
            "WHERE m.CODE IS NOT NULL ORDER BY m.CODE"
        )
        row = active_cur.fetchone()
        if row:
            code_sample = str(row[0]).strip()
            name_sample = str(row[1] or "").strip()
            diagnostics["sample_code"] = code_sample
            diagnostics["sample_name"] = name_sample
            diagnostics["sample_code_matches"] = db_find_by_code(active_cur, code_sample)
            if name_sample:
                diagnostics["sample_name_matches"] = db_find_by_name_like(
                    active_cur, name_sample, limit=3
                )
    except Exception as exc:
        diagnostics["sample_material_error"] = str(exc)

    return diagnostics


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
