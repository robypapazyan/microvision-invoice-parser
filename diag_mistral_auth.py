#!/usr/bin/env python3
"""CLI диагностика за Mistral login."""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, List

from mistral_db import (  # type: ignore[attr-defined]
    MistralDBError,
    connect,
    detect_catalog_schema,
    detect_login_method,
    find_material_candidates,
    get_last_login_status,
    get_last_login_trace,
    get_material_by_barcode,
    logger,
    check_login_credentials,
)

CLIENTS_FILE = Path(__file__).with_name("mistral_clients.json")
LOCAL_CLIENTS_FILE = Path(__file__).with_name("mistral_clients.local.json")
SUMMARY_PREFIX = "SUMMARY:"


def load_profiles() -> Dict[str, Dict[str, Any]]:
    if not CLIENTS_FILE.exists():
        raise SystemExit("Липсва mistral_clients.json – няма как да се изпълни диагностиката.")

    def _read(path: Path, label: str) -> Any:
        try:
            with path.open("r", encoding="utf-8-sig") as fh:
                return json.load(fh)
        except json.JSONDecodeError as exc:
            raise SystemExit(f"{label} е в невалиден формат: {exc}") from exc

    def _coerce(data: Any, label: str) -> Dict[str, Dict[str, Any]]:
        profiles_map: Dict[str, Dict[str, Any]] = {}
        if isinstance(data, dict):
            for key, value in data.items():
                if isinstance(value, dict):
                    profiles_map[str(key)] = dict(value)
        elif isinstance(data, list):
            for idx, item in enumerate(data):
                if not isinstance(item, dict):
                    continue
                name = (
                    item.get("name")
                    or item.get("client")
                    or item.get("profile")
                    or item.get("label")
                )
                if not name:
                    name = f"Профил {idx + 1}"
                profiles_map[str(name)] = dict(item)
        else:
            raise SystemExit(f"{label} трябва да описва dict или list от профили.")
        return profiles_map

    base_profiles = _coerce(
        _read(CLIENTS_FILE, "mistral_clients.json"),
        "mistral_clients.json",
    )

    if LOCAL_CLIENTS_FILE.exists():
        local_profiles = _coerce(
            _read(LOCAL_CLIENTS_FILE, "mistral_clients.local.json"),
            "mistral_clients.local.json",
        )
    else:
        local_profiles = {}

    def _deep_merge(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
        merged = dict(base)
        for key, value in override.items():
            if (
                key in merged
                and isinstance(merged[key], dict)
                and isinstance(value, dict)
            ):
                merged[key] = _deep_merge(merged[key], value)
            else:
                merged[key] = value
        return merged

    profiles: Dict[str, Dict[str, Any]] = {
        key: dict(value) for key, value in base_profiles.items()
    }
    for key, value in local_profiles.items():
        if key in profiles:
            profiles[key] = _deep_merge(profiles[key], value)
        else:
            profiles[key] = dict(value)

    if not profiles:
        raise SystemExit("В mistral_clients.json няма валидни профили.")

    for value in profiles.values():
        database_path = value.get("database")
        if isinstance(database_path, str) and database_path:
            value["database"] = os.path.normpath(os.fspath(database_path))
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


def _procedure_examples(meta: Dict[str, Any]) -> Dict[str, str]:
    name = meta.get("name") or "?"
    inputs = meta.get("fields", {}).get("inputs") or []
    placeholders = ", ".join(["?"] * len(inputs))
    select_sql = (
        f"SELECT * FROM {name}({placeholders})" if placeholders else f"SELECT * FROM {name}"
    )
    exec_sql = (
        f"EXECUTE PROCEDURE {name} {placeholders}" if placeholders else f"EXECUTE PROCEDURE {name}"
    )
    hints: List[str] = []
    if inputs:
        for field in inputs:
            fname = field.get("name") or "PARAM"
            hints.append(fname.strip() or "PARAM")
    mapping = ", ".join(hints) if hints else "без параметри"
    return {"select": select_sql, "execute": exec_sql, "hints": mapping}


def _table_example(meta: Dict[str, Any]) -> str:
    table_name = meta.get("name") or "USERS"
    fields = meta.get("fields", {})
    login_field = fields.get("login") or "NAME"
    pass_field = fields.get("password") or fields.get("password_hash") or "PASS"
    return (
        "SELECT COUNT(*) FROM "
        f"{table_name} WHERE UPPER(TRIM({login_field})) = UPPER(?) "
        f"AND TRIM({pass_field}) = TRIM(?)"
    )


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
        examples = _procedure_examples(meta)
        print("  Пробни заявки:")
        print(f"    - SELECT: {examples['select']}")
        print(f"    - EXECUTE: {examples['execute']}")
        print(f"    - Параметри: {examples['hints']}")
        fallback = meta.get("fallback_table")
        if isinstance(fallback, dict):
            print("  \n  Резервен табличен вход:")
            print(f"    - Таблица: {fallback.get('name')}")
            print(f"    - Пробна заявка: {_table_example(fallback)}")
        return

    if mode == "table":
        table_name = meta.get("name")
        fields = meta.get("fields", {})
        print(f"Открит login механизъм: ТАБЛИЦА {table_name}")
        print("  Използвани полета:")
        print(f"    - ID: {fields.get('id')}")
        print(f"    - LOGIN: {fields.get('login') or '—'}")
        pass_label = fields.get('password') or fields.get('password_hash') or '—'
        print(f"    - PASSWORD: {pass_label}")
        if fields.get('has_hash'):
            print("    - HASH режим: наличен (TODO конфигурация)")
        print(f"    - SALT: {fields.get('salt') or '—'}")
        probe_sql = _table_example(meta)
        print(f"  Пробна заявка: {probe_sql}")
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


def build_summary(
    meta: Dict[str, Any],
    trace: List[Dict[str, Any]],
    success: bool,
    operator_id: int | None,
    operator_login: str,
    error_message: str,
    forced_table: bool,
) -> List[str]:
    lines: List[str] = []
    connection_entries = [
        entry
        for entry in trace
        if isinstance(entry, dict)
        and entry.get("action") in {"connect_success", "connect_attempt"}
    ]
    connection_info = connection_entries[-1] if connection_entries else {}
    if connection_info:
        conn_parts: List[str] = []
        driver_name = connection_info.get("driver")
        if driver_name:
            conn_parts.append(f"драйвер {driver_name}")
        function_name = connection_info.get("function")
        if function_name:
            conn_parts.append(function_name)
        dsn_value = connection_info.get("dsn")
        if dsn_value:
            conn_parts.append(f"DSN {dsn_value}")
        else:
            host_value = connection_info.get("host")
            port_value = connection_info.get("port")
            database_value = connection_info.get("database")
            if host_value:
                conn_parts.append(f"host {host_value}")
            if port_value is not None:
                conn_parts.append(f"port {port_value}")
            if database_value:
                conn_parts.append(f"database {database_value}")
        charset_value = connection_info.get("charset")
        if charset_value:
            conn_parts.append(f"charset {charset_value}")
        if conn_parts:
            lines.append("Свързване: " + ", ".join(conn_parts) + ".")

    failure_entries = [
        entry for entry in trace if isinstance(entry, dict) and entry.get("action") == "connect_failure"
    ]
    if failure_entries:
        failure = failure_entries[-1]
        error_bits: List[str] = []
        function_name = failure.get("function")
        if function_name:
            error_bits.append(function_name)
        sqlcode = failure.get("sqlcode")
        if sqlcode is not None:
            error_bits.append(f"SQLCODE {sqlcode}")
        error_code = failure.get("error_code")
        if error_code is not None:
            error_bits.append(f"CODE {error_code}")
        error_message = failure.get("error_message") or failure.get("error")
        if error_message:
            error_bits.append(str(error_message))
        error_type = failure.get("error_type")
        if error_type and error_type not in (error_message,):
            error_bits.append(str(error_type))
        if error_bits:
            lines.append("Свързване: грешка – " + " | ".join(error_bits) + ".")

    mode = meta.get("mode")
    if mode == "sp":
        proc_name = meta.get("name") or "?"
        sp_kind = meta.get("sp_kind") or "неизвестна"
        lines.append(f"Механизъм: процедура {proc_name} ({sp_kind}).")
        examples = _procedure_examples(meta)
        lines.append(f"SP SELECT: {examples['select']}")
        lines.append(f"SP EXECUTE: {examples['execute']}")
        fallback = meta.get("fallback_table")
        if isinstance(fallback, dict):
            table_name = fallback.get("name") or "USERS"
            lines.append(f"Резервна таблица: {table_name}.")
            lines.append(f"Таблична проверка: {_table_example(fallback)}")
    elif mode == "table":
        table_name = meta.get("name") or "USERS"
        lines.append(f"Механизъм: таблица {table_name}.")
        lines.append(f"Таблична проверка: {_table_example(meta)}")
    else:
        lines.append("Механизъм: неуспешно откриване.")

    if forced_table:
        lines.append("Опция --force-table е активна – процедурата е пропусната.")

    actions = [entry.get("action") for entry in trace]
    if "sp_select" in actions:
        proc_entries = [e for e in trace if e.get("action") == "sp_select"]
        if proc_entries:
            proc = proc_entries[-1].get("procedure") or meta.get("name")
            lines.append(f"Опит: SELECT от процедура {proc}.")
    if "sp_execute" in actions:
        proc_entries = [e for e in trace if e.get("action") == "sp_execute"]
        if proc_entries:
            proc = proc_entries[-1].get("procedure") or meta.get("name")
            lines.append(f"Опит: EXECUTE PROCEDURE {proc}.")
    if "procedure_fallback_table" in actions:
        lines.append("Процедурата не върна резултат – преминахме към таблица.")

    table_entries = [e for e in trace if e.get("action") == "table_lookup"]
    if table_entries:
        entry = table_entries[-1]
        table_name = entry.get("table") or meta.get("name") or "USERS"
        mode_label = entry.get("mode") or "?"
        if mode_label == "username":
            match_text = "потребител + парола"
        elif mode_label == "password":
            match_text = "само парола"
        else:
            match_text = mode_label
        lines.append(f"Опит: Табличен логин ({match_text}) в {table_name}.")
    if "table_ambiguous" in actions:
        ambiguous = [e for e in trace if e.get("action") == "table_ambiguous"]
        if ambiguous:
            count = ambiguous[-1].get("matches")
            lines.append(f"Таблицата върна {count} съвпадения за паролата.")

    if success:
        lines.append(
            f"Резултат: УСПЕХ – оператор ID {operator_id}, вход '{operator_login}'."
        )
    else:
        reason = error_message or "неуспешен вход"
        lines.append(f"Резултат: НЕУСПЕХ – {reason}.")
    return lines


def main() -> None:
    parser = argparse.ArgumentParser(description="Диагностика на Mistral login.")
    parser.add_argument("--profile", help="Име на профила от mistral_clients.json")
    parser.add_argument("--user", default="", help="Потребителско име")
    parser.add_argument("--password", default="", help="Парола")
    parser.add_argument(
        "--pc-id",
        default="",
        help="PC/terminal ID, който да се подаде към login процедурата",
    )
    parser.add_argument(
        "--list-profiles",
        action="store_true",
        help="Изброй наличните профили и излез",
    )
    parser.add_argument(
        "--force-table",
        action="store_true",
        help="Пропусни процедурата и използвай директно табличен логин",
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

    diag_ok = True
    try:
        schema = detect_catalog_schema(cur)
    except MistralDBError as exc:
        print(f"\nКаталожна схема: НЕУСПЕШНО – {exc}")
        diag_ok = False
        schema = {}
    else:
        print("\nКаталожна схема: УСПЕШНО")
        materials_table = schema.get("materials_table")
        materials_name = schema.get("materials_name")
        barcode_table = schema.get("barcode_table")
        barcode_col = schema.get("barcode_col")
        print(f"  - Материали: {materials_table or 'не е открита'}")
        print(f"  - Име на материал: {materials_name or 'не е открита'}")
        if barcode_table:
            print(f"  - Баркодове: {barcode_table} (колона {barcode_col or '—'})")
        else:
            print("  - Баркодове: не е открита таблица")
        if materials_table:
            try:
                cur.execute(f"SELECT COUNT(*) FROM {materials_table}")
                count = cur.fetchone()[0]
                print(f"  - Брой материали: {count}")
            except Exception as exc:
                print(f"  - Брой материали: грешка ({exc})")
                diag_ok = False
        if barcode_table:
            try:
                cur.execute(f"SELECT COUNT(*) FROM {barcode_table}")
                barcode_count = cur.fetchone()[0]
                print(f"  - Брой баркодове: {barcode_count}")
            except Exception as exc:
                print(f"  - Брой баркодове: грешка ({exc})")
                diag_ok = False

        if barcode_table and barcode_col:
            try:
                cur.execute(
                    f"SELECT FIRST 1 TRIM({barcode_col}) FROM {barcode_table} "
                    f"WHERE TRIM({barcode_col}) <> ''"
                )
                row = cur.fetchone()
                if row and row[0]:
                    sample_barcode = str(row[0]).strip()
                    material = get_material_by_barcode(cur, sample_barcode)
                    if material:
                        print(f"Пример баркод: {sample_barcode} → {material.code} | {material.name}")
                    else:
                        print(f"Пример баркод: {sample_barcode} (няма намерен материал)")
                else:
                    print("Пример баркод: няма налични стойности")
            except Exception as exc:
                print(f"Пример баркод: НЕУСПЕШНО – {exc}")
                diag_ok = False
        else:
            print("Пример баркод: пропуснато – няма таблица за баркодове")

        if materials_table and materials_name:
            try:
                cur.execute(
                    f"SELECT FIRST 1 TRIM({materials_name}) FROM {materials_table} "
                    f"WHERE TRIM({materials_name}) <> ''"
                )
                row = cur.fetchone()
                if row and row[0]:
                    sample_name = str(row[0]).strip()
                    candidates = find_material_candidates(cur, sample_name, limit=3)
                    if candidates:
                        first = candidates[0]
                        print(f"Пример име '{sample_name}' → {first.code} | {first.name}")
                    else:
                        print(f"Пример име '{sample_name}': няма кандидати в каталога")
                else:
                    print("Пример име: няма налични стойности")
            except Exception as exc:
                print(f"Пример име: НЕУСПЕШНО – {exc}")
                diag_ok = False
        else:
            print("Пример име: пропуснато – няма колонa за име на материал")
    if args.force_table:
        os.environ["MV_FORCE_TABLE_LOGIN"] = "1"
        print("Активиран е принудителен табличен режим (--force-table).")

    username = (args.user or "").strip()
    password = args.password or ""
    pc_id = args.pc_id.strip() or None

    display_pc = pc_id if pc_id else "<липсва>"
    print(
        "\nТестов вход (потребител='{}', парола='{}', pc_id={})".format(
            username or "<само парола>",
            "***" if password else "<празна>",
            display_pc,
        )
    )

    object_id = profile.get("object_id") or profile.get("OBJECTID") or 1
    table_no = profile.get("table_no") or profile.get("TABLENO") or 1

    try:
        success = check_login_credentials(
            username,
            password,
            object_id=object_id,
            table_no=table_no,
        )
        error_message = "" if success else (get_last_login_status().get("error") or "неуспешен вход")
    except MistralDBError as exc:
        success = False
        error_message = str(exc)
    except Exception as exc:  # pragma: no cover - защитно
        success = False
        error_message = str(exc)

    status_meta = get_last_login_status()
    trace = get_last_login_trace()

    if success:
        print(
            "LOGIN RESULT: SUCCESS "
            f"(mode={status_meta.get('mode') or 'неизвестен'})"
        )
    else:
        print(
            "LOGIN RESULT: FAILURE ({} | mode={})".format(
                error_message or "неуспешен вход",
                status_meta.get("mode") or "неизвестен",
            )
        )

    operator_id = 1 if success else None
    operator_login = username or "1"

    summary_lines = build_summary(
        meta,
        trace,
        success,
        operator_id,
        operator_login,
        error_message,
        args.force_table,
    )
    print("\nSUMMARY:")
    for line in summary_lines:
        print(f"{SUMMARY_PREFIX} {line}")

    print_trace(trace)

    overall_ok = diag_ok and success
    print(f"\nDIAG STATUS: {'OK' if overall_ok else 'FAIL'}")

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
