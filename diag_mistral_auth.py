"""Диагностичен скрипт за откриване на Mistral login механизма."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List

from mistral_db import connect, MistralDBError


PROFILE_FILE = Path(__file__).with_name("mistral_clients.json")

LOGIN_HINTS = {"LOGIN", "USERNAME", "USER_NAME", "CODE", "KOD", "NAME", "OPERATOR"}
PASS_HINTS = {"PASS", "PASSWORD", "PAROLA", "PASS_HASH", "PAROLA_HASH", "PWD"}
ID_HINTS = {"ID", "USER_ID", "OP_ID", "CODE", "KOD"}


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
    if not isinstance(data, list) or not data:
        raise SystemExit("mistral_clients.json трябва да съдържа поне един профил.")
    return data[0]


def _print_header(title: str) -> None:
    print(title)
    print("=" * len(title))


def _fetch_procedure_params(cur: Any, name: str) -> List[Dict[str, Any]]:
    q = cur.connection.cursor()
    q.execute(
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
    rows = q.fetchall()
    q.close()
    params: List[Dict[str, Any]] = []
    for row in rows:
        params.append(
            {
                "type": "IN" if row[0] == 0 else "OUT",
                "name": row[1],
                "order": int(row[2]),
                "type_name": _field_type_name(row[3], row[4], row[5], row[6], row[7], row[8]),
            }
        )
    return params


def _print_login_procedures(cur: Any) -> None:
    print("\nПроцедури с 'LOGIN' в името:")
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
        print("  (няма намерени процедури)")
        return
    for name, proc_type in rows:
        params = _fetch_procedure_params(cur, name)
        call_type = "SELECTABLE" if proc_type == 1 else "EXECUTE"
        print(f"- {name} [{call_type}]")
        ins = [p for p in params if p["type"] == "IN"]
        outs = [p for p in params if p["type"] == "OUT"]
        if ins:
            print("    Входни параметри:")
            for param in ins:
                print(f"      • {param['name']} : {param['type_name']}")
        else:
            print("    Входни параметри: (няма)")
        if outs:
            print("    Изходни параметри:")
            for param in outs:
                print(f"      • {param['name']} : {param['type_name']}")
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


def _print_user_tables(cur: Any) -> None:
    print("\nТаблици със записи за потребители:")
    cur.execute(
        """
        SELECT TRIM(r.rdb$relation_name)
        FROM rdb$relations r
        WHERE r.rdb$view_blr IS NULL
          AND COALESCE(r.rdb$system_flag, 0) = 0
          AND UPPER(r.rdb$relation_name) LIKE '%USER%'
        ORDER BY 1
        """
    )
    tables = [row[0] for row in cur.fetchall()]
    if not tables:
        print("  (няма намерени кандидат-таблици)")
        return

    for table in tables:
        cols = _fetch_table_columns(cur, table)
        login_cols = [c for c in cols if c["name"].upper() in LOGIN_HINTS]
        pass_cols = [c for c in cols if c["name"].upper() in PASS_HINTS]
        id_cols = [c for c in cols if c["name"].upper() in ID_HINTS]
        if not pass_cols or not id_cols:
            continue
        print(f"- {table}:")
        if id_cols:
            joined = ", ".join(f"{c['name']} ({c['type_name']})" for c in id_cols)
            print(f"    ID колони: {joined}")
        if login_cols:
            joined = ", ".join(f"{c['name']} ({c['type_name']})" for c in login_cols)
            print(f"    Колони за потребител: {joined}")
        else:
            print("    Колони за потребител: (не са намерени – вероятен login само по парола)")
        joined = ", ".join(f"{c['name']} ({c['type_name']})" for c in pass_cols)
        print(f"    Колони за парола: {joined}")


def main() -> None:
    profile = _load_profile()
    try:
        conn, cur = connect(profile)
    except MistralDBError as exc:
        raise SystemExit(f"Неуспешна връзка към Mistral: {exc}")

    _print_header("Диагностика на Mistral (автентикация)")
    _print_login_procedures(cur)
    _print_user_tables(cur)

    try:
        cur.close()
    except Exception:
        pass
    try:
        conn.close()
    except Exception:
        pass


if __name__ == "__main__":
    main()
