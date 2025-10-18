#!/usr/bin/env python3
"""CLI диагностика за Mistral login."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Dict, List

from loguru import logger

from mistral_db import (  # type: ignore[attr-defined]
    MistralDBError,
    connect,
    detect_login_method,
    get_last_login_trace,
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


def describe_meta(meta: Dict[str, Any]) -> str:
    mode = meta.get("mode")
    if mode == "sp":
        return f"Процедура {meta.get('name')} ({meta.get('sp_kind')})"
    if mode == "table":
        fields = meta.get("fields", {})
        name = meta.get("name")
        has_name = "да" if fields.get("has_name") else "не"
        has_pass = "да" if fields.get("has_pass") else "не"
        return f"Таблица {name} (NAME: {has_name}, PASS: {has_pass})"
    return "Непознат режим"


def print_trace(trace: List[Dict[str, Any]]) -> None:
    if not trace:
        print("\nНяма налична хронология от опита за логин.")
        return

    print("\nХронология на опита:")
    for step in trace:
        action = step.get("action")
        if action == "start":
            print(
                f"- Старт: профил {step.get('profile')} | потребител: {step.get('username')}"
            )
        elif action == "procedure_attempt":
            mode = step.get("mode")
            params = step.get("params", {})
            print(
                f"- Процедура ({mode}): {step.get('procedure')} | SQL: {step.get('sql')}"
            )
            print(
                f"    параметри: потребител={params.get('username')} парола={params.get('password')}"
            )
        elif action == "procedure_switch":
            print(
                f"- Превключване от {step.get('from')} към {step.get('to')} (причина: {step.get('reason')})"
            )
        elif action == "procedure_error":
            print(
                f"- Грешка при процедура ({step.get('mode')}): {step.get('procedure')} -> {step.get('error')}"
            )
        elif action == "procedure_result":
            print(
                f"- Резултат процедура ({step.get('mode')}): {step.get('procedure')} | редове: {step.get('rows')}"
            )
        elif action == "procedure_callproc":
            print(f"- Опит за callproc: {step.get('procedure')}")
        elif action == "table_attempt":
            params = step.get("params", {})
            print(
                f"- Таблица ({step.get('mode')}): {step.get('table')} | SQL: {step.get('sql')}"
            )
            print(
                f"    параметри: потребител={params.get('username')} парола={params.get('password')}"
            )
        elif action == "table_error":
            print(f"- Грешка при таблица {step.get('table')}: {step.get('error')}")
        elif action == "table_result":
            print(f"- Резултат от таблица {step.get('table')}: {step.get('rows')} ред(а)")
        elif action == "success":
            print(
                f"- Успех: оператор ID={step.get('operator_id')} login={step.get('operator_login')}"
            )
        elif action == "failure":
            print(f"- Неуспех: {step.get('message')}")


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

    print(f"Открит login режим: {describe_meta(meta)}")
    if meta.get("mode") == "table":
        candidates = [c.get("name") for c in meta.get("candidates", []) if c.get("name")]
        if candidates:
            print("Възможни таблици: " + ", ".join(candidates))

    result_text = ""
    try:
        operator_id, operator_login = login_user(args.user or "", args.password or "")
    except MistralDBError as exc:
        result_text = f"Краен резултат: НЕВАЛИДЕН – {exc}"
    else:
        result_text = (
            "Краен резултат: УСПЕХ – "
            f"оператор ID={operator_id}, потребител={operator_login}"
        )
    finally:
        trace = get_last_login_trace()
        print_trace(trace)
        try:
            cur.close()
        except Exception:  # pragma: no cover - защитно
            pass
        try:
            conn.close()
        except Exception:  # pragma: no cover - защитно
            pass

    print("\n" + result_text)


if __name__ == "__main__":
    logger.info("Стартира диагностика на логин модул.")
    main()
