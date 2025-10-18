"""Диагностика на Mistral login механизма."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List

from mistral_db import MistralDBError, connect


PROFILE_FILE = Path(__file__).with_name("mistral_clients.json")


def _field_type_name(
    field_type: int,
    sub_type: int | None,
    length: int | None,
    precision: int | None,
    scale: int | None,
    char_length: int | None,
) -> str:
    mapping = {
        7: "SMALLINT",
        8: "INTEGER",
        9: "QUAD",
        10: "FLOAT",
        11: "D_FLOAT",
        12: "DATE",
        13: "TIME",
        14: "CHAR",
        16: "BIGINT",
        17: "BOOLEAN",
        27: "DOUBLE",
        35: "TIMESTAMP",
        37: "VARCHAR",
        40: "CSTRING",
        261: "BLOB",
    }
    base = mapping.get(field_type, f"TYPE_{field_type}")
    if field_type in {14, 37, 40} and char_length:
        return f"{base}({char_length})"
    if field_type in {7, 8, 16, 27}:
        if scale and scale < 0:
            digits = precision if precision and precision > 0 else (length or 0)
            return f"NUMERIC({digits}, {abs(scale)})"
        return base
    if field_type == 261 and sub_type == 1:
        return "BLOB SUB_TYPE TEXT"
    return base


def _load_profile() -> Dict[str, Any]:
    if not PROFILE_FILE.exists():
        raise SystemExit("Липсва mistral_clients.json – няма как да се изпълни диагностиката.")
    with PROFILE_FILE.open("r", encoding="utf-8") as fh:
        data = json.load(fh)
    if isinstance(data, dict):
        if not data:
            raise SystemExit("mistral_clients.json е празен.")
        first_key = next(iter(data))
        profile = data[first_key]
        if not isinstance(profile, dict):
            raise SystemExit("Профилът трябва да е описан като обект.")
        return profile
    if isinstance(data, list):
        for item in data:
            if isinstance(item, dict):
                return item
        raise SystemExit("Списъкът с профили не съдържа валиден запис.")
    raise SystemExit("mistral_clients.json е в неочакван формат (list или dict са допустими).")


def _print_header(title: str) -> None:
    print(title)
    print("=" * len(title))


def _print_login_procedures(cur: Any) -> None:
    print("\nПроцедури, съдържащи 'LOGIN' в името:")
    cur.execute(
        """
        SELECT TRIM(p.rdb$procedure_name), COALESCE(p.rdb$procedure_type, 2)
        FROM rdb$procedures p
        WHERE (p.rdb$system_flag IS NULL OR p.rdb$system_flag = 0)
          AND UPPER(p.rdb$procedure_name) LIKE '%LOGIN%'
        ORDER BY 1
        """
    )
    rows = cur.fetchall()
    if not rows:
        print("  (не са открити подходящи процедури)")
        return
    for name, proc_type in rows:
        call_type = "SELECTABLE" if proc_type == 1 else "EXECUTE"
        print(f"- {name} [{call_type}]")
        cur.execute(
            """
            SELECT
                COALESCE(pp.rdb$parameter_type, 0) AS param_type,
                TRIM(pp.rdb$parameter_name) AS param_name,
                COALESCE(pp.rdb$parameter_number, 0) AS param_number,
                f.rdb$field_type,
                f.rdb$field_sub_type,
                f.rdb$field_length,
                f.rdb$field_precision,
                f.rdb$field_scale,
                f.rdb$character_length
            FROM rdb$procedure_parameters pp
            JOIN rdb$fields f ON f.rdb$field_name = pp.rdb$field_source
            WHERE pp.rdb$procedure_name = ?
            ORDER BY param_type, param_number
            """,
            (name,),
        )
        params = cur.fetchall()
        ins = [p for p in params if p[0] == 0]
        outs = [p for p in params if p[0] != 0]
        if ins:
            print("    Входни параметри:")
            for row in ins:
                print(f"      • {row[1]} : {_field_type_name(row[3], row[4], row[5], row[6], row[7], row[8])}")
        else:
            print("    Входни параметри: (няма)")
        if outs:
            print("    Изходни параметри:")
            for row in outs:
                print(f"      • {row[1]} : {_field_type_name(row[3], row[4], row[5], row[6], row[7], row[8])}")
        else:
            print("    Изходни параметри: (няма)")


def _fetch_table_columns(cur: Any, table: str) -> List[Dict[str, Any]]:
    cur.execute(
        """
        SELECT
            TRIM(rf.rdb$field_name) AS col_name,
            COALESCE(rf.rdb$null_flag, 0) AS null_flag,
            f.rdb$field_type,
            f.rdb$field_sub_type,
            f.rdb$field_length,
            f.rdb$field_precision,
            f.rdb$field_scale,
            f.rdb$character_length
        FROM rdb$relation_fields rf
        JOIN rdb$fields f ON f.rdb$field_name = rf.rdb$field_source
        WHERE rf.rdb$relation_name = ?
        ORDER BY rf.rdb$field_position
        """,
        (table,),
    )
    result = []
    for row in cur.fetchall():
        result.append(
            {
                "name": row[0],
                "not_null": bool(row[1]),
                "type_name": _field_type_name(row[2], row[3], row[4], row[5], row[6], row[7]),
            }
        )
    return result


def _print_table_info(cur: Any, table: str) -> None:
    print(f"\nТаблица {table}:")
    columns = _fetch_table_columns(cur, table)
    if not columns:
        print("  Таблицата не е открита или няма колони.")
        return
    has_name = any(col["name"].upper() == "NAME" for col in columns)
    has_pass = any(col["name"].upper() == "PASS" for col in columns)
    print(f"  Има колона NAME: {'да' if has_name else 'не'}")
    print(f"  Има колона PASS: {'да' if has_pass else 'не'}")
    print("  Колони:")
    for col in columns:
        required = []
        if col["name"].upper() == "NAME":
            required.append("← NAME")
        if col["name"].upper() == "PASS":
            required.append("← PASS")
        label = f" ({', '.join(required)})" if required else ""
        nullable = "NOT NULL" if col["not_null"] else "NULLABLE"
        print(f"    • {col['name']} : {col['type_name']} [{nullable}]{label}")


def _print_sample_queries() -> None:
    print("\nПримерни SELECT заявки, използвани от логин модула:")
    print("  - SELECT ID, NAME FROM USERS WHERE NAME=? AND PASS=?")
    print("  - SELECT ID, NAME FROM LOGUSERS WHERE NAME=? AND PASS=?")
    print("  - (само парола) SELECT ID, NAME FROM <TABLE> WHERE PASS=?")


def main() -> None:
    profile = _load_profile()
    try:
        conn, cur = connect(profile)
    except MistralDBError as exc:
        raise SystemExit(f"Неуспешно свързване: {exc}")

    label = profile.get("name") or profile.get("label") or profile.get("client") or profile.get("database")
    _print_header(f"Профил: {label}")
    _print_login_procedures(cur)
    _print_table_info(cur, "USERS")
    _print_table_info(cur, "LOGUSERS")
    _print_sample_queries()


if __name__ == "__main__":
    main()
