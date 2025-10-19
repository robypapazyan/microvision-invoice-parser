#!/usr/bin/env python3
"""CLI диагностика за Mistral login."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict, List

from mistral_db import (  # type: ignore[attr-defined]
    MistralDBError,
    connect,
    detect_login_method,
    get_last_login_trace,
    logger,
    login_user,
)

CLIENTS_FILE = Path(__file__).with_name("mistral_clients.json")


def load_profiles() -> Dict[str, Dict[str, Any]]:
    if not CLIENTS_FILE.exists():
        raise SystemExit("Липсва mistral_clients.json – няма как да се изпълни диагностиката.")
    try:
        with CLIENTS_FILE.open("r", encoding="utf-8") as fh:
            data = json.load(fh)
    except json.JSONDecodeError as exc:
        raise SystemExit(f"mistral_clients.json е в невалиден формат: {exc}") from exc

    profiles: Dict[str, Dict[str, Any]] = {}
    if isinstance(data, dict):
        for key, value in data.items():
            if isinstance(value, dict):
                profiles[str(key)] = value
    elif isinstance(data, list):
        for idx, item in enumerate(data):
            if isinstance(item, dict):
                name = item.get("name") or item.get("client") or item.get("label")
                if not name:
                    name = f"Профил {idx + 1}"
                profiles[str(name)] = item
    else:
        raise SystemExit("mistral_clients.json трябва да описва dict или list от профили.")

    if not profiles:
        raise SystemExit("В mistral_clients.json няма валидни профили.")
    return profiles


def pick_profile(profiles: Dict[str, Dict[str, Any]], name: str | None) -> tuple[str, Dict[str, Any]]:
    if name:
        if name in profiles:
            return name, profiles[name]
        raise SystemExit(f"Профил '{name}' не е намерен в mistral_clients.json.")
    first_key = next(iter(profiles))
    return first_key, profiles[first_key]


def _format_field(field: Dict[str, Any]) -> str:
    name = field.get("name") or "?"
    type_name = field.get("type_name") or "?"
    position = field.get("position")
    if position is not None:
        return f"[{position}] {name} – {type_name}"
    return f"{name} – {type_name}"


def print_meta(meta: Dict[str, Any]) -> None:
    mode = meta.get("mode")
    if mode == "sp":
        name = meta.get("name")
        sp_kind = meta.get("sp_kind")
        print(f"Открит login механизъм: ПРОЦЕДУРА {name} ({sp_kind})")
        fields = meta.get("fields", {})
        inputs = fields.get("inputs") or []
        outputs = fields.get("outputs") or []
        if inputs:
            print("  Вход параметри:")
            for field in inputs:
                print(f"    - {_format_field(field)}")
        if outputs:
            print("  Изход параметри:")
            for field in outputs:
                print(f"    - {_format_field(field)}")
        return

    if mode == "table":
        table_name = meta.get("name")
        fields = meta.get("fields", {})
        print(f"Открит login механизъм: ТАБЛИЦА {table_name}")
        print("  Използвани полета:")
        print(f"    - ID: {fields.get('id')}")
        print(f"    - LOGIN: {fields.get('login') or '—'}")
        print(f"    - PASSWORD: {fields.get('password')}")
        print(f"    - SALT: {fields.get('salt') or '—'}")
        columns = meta.get("columns") or {}
        if columns:
            print("  Достъпни колони:")
            for name, info in columns.items():
                print(f"    - {name}: {info.get('type_name')}")
        candidates = [c.get("name") for c in meta.get("candidates", []) if c.get("name")]
        if candidates:
            print("  Алтернативни таблици: " + ", ".join(candidates))
        return

    print("Открит механизъм: непознат режим")


def print_trace(trace: List[Dict[str, Any]]) -> None:
    print("TRACE:")
    print(json.dumps(trace, ensure_ascii=False, indent=2))
        

def main() -> None:
    parser = argparse.ArgumentParser(description="Диагностика на Mistral login.")
    parser.add_argument("--profile", help="Име на профила от mistral_clients.json")
    parser.add_argument("--user", default="", help="Потребителско име (може да е празно)")
    parser.add_argument("--password", default="", help="Парола")
    parser.add_argument(
        "--list-profiles",
        action="store_true",
        help="Изброй наличните профили и излез",
    )
    args = parser.parse_args()

    profiles = load_profiles()
    if args.list_profiles:
        print("Налични профили:")
        for name in profiles:
            print(f"- {name}")
        return

    profile_name, profile = pick_profile(profiles, args.profile)
    print(f"Профил: {profile_name}")

    try:
        conn, cur = connect(profile)
    except MistralDBError as exc:
        raise SystemExit(f"Свързване: НЕУСПЕШНО – {exc}")

    print("Свързване: УСПЕШНО")
    try:
        meta = detect_login_method(cur)
    except MistralDBError as exc:
        conn.close()
        raise SystemExit(f"Откриване на логин механизъм: НЕУСПЕШНО – {exc}")

    print_meta(meta)

    if args.user or args.password:
        user_display = args.user if args.user else "<празно>"
        print(f"\nТестов вход с потребител='{user_display}'")
    else:
        print("\nТестов вход без потребителско име (само парола)")

    success = False
    error_message = ""
    operator_id: int | None = None
    operator_login = ""
    try:
        operator_id, operator_login = login_user(args.user or "", args.password or "")
    except MistralDBError as exc:
        error_message = str(exc)
    else:
        success = True

    trace = get_last_login_trace()

    if success:
        print(
            "LOGIN RESULT: SUCCESS "
            f"(operator_id={operator_id}, operator_login={operator_login})"
        )
    else:
        print(f"LOGIN RESULT: FAILURE ({error_message or 'неуспешен вход'})")

    print_trace(trace)

    try:
        cur.close()
    except Exception:  # pragma: no cover - защитно
        pass
    try:
        conn.close()
    except Exception:  # pragma: no cover - защитно
        pass

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    logger.info("Стартира диагностика на логин модул.")
    main()
