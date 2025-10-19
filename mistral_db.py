"""Utility helpers for talking to a Mistral (Firebird) database."""
from __future__ import annotations

import hashlib
import logging
import os
from contextlib import contextmanager
from datetime import datetime, date
from decimal import Decimal, ROUND_HALF_UP
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

try:  # pragma: no cover - loguru е предпочитан, но не задължителен
    from loguru import logger as _loguru_logger
except ImportError:  # pragma: no cover - fallback към logging
    _loguru_logger = None  # type: ignore


_LOG_CONFIGURED = False
_CONN: Any | None = None
_CUR: Any | None = None
_PROFILE: Dict[str, Any] | None = None
_PROFILE_LABEL: str | None = None
_LOGIN_META: Dict[str, Any] | None = None
_DELIVERY_TABLES: Dict[str, str] | None = None
_DELIVERY_GENERATORS: Dict[str, Optional[str]] | None = None
_TABLE_COLUMNS: Dict[str, Dict[str, Dict[str, Any]]] = {}
_DELIVERY_CONTEXT: Dict[int, Dict[str, Any]] = {}
_LOGIN_TRACE: List[Dict[str, Any]] = []


if _loguru_logger is not None:
    logger = _loguru_logger
else:  # pragma: no cover - при липса на loguru
    logger = logging.getLogger("microvision")


def _configure_logging() -> None:
    global _LOG_CONFIGURED
    if _LOG_CONFIGURED:
        return

    log_dir = Path(__file__).resolve().parent / "logs"
    try:
        log_dir.mkdir(parents=True, exist_ok=True)
    except Exception:  # pragma: no cover - защитно
        pass

    log_level_name = os.getenv("MICROVISION_LOG_LEVEL", "INFO").upper() or "INFO"
    log_file = log_dir / "app_{time:YYYYMMDD}.log"

    if _loguru_logger is not None:
        try:
            logger.add(
                log_file,
                rotation="5 MB",
                retention="7 files",
                level=log_level_name,
                encoding="utf-8",
            )
        except Exception:  # pragma: no cover - ако loguru е вече конфигуриран
            pass
    else:  # pragma: no cover - logging fallback
        level = getattr(logging, log_level_name, logging.INFO)
        logger.setLevel(logging.DEBUG)
        handler = RotatingFileHandler(
            log_dir / f"app_{datetime.now():%Y%m%d}.log",
            maxBytes=5 * 1024 * 1024,
            backupCount=7,
            encoding="utf-8",
        )
        handler.setLevel(level)
        formatter = logging.Formatter(
            "%(asctime)s [%(levelname)s] %(name)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        handler.setFormatter(formatter)
        if not any(isinstance(h, RotatingFileHandler) for h in logger.handlers):
            logger.addHandler(handler)
        logger.propagate = False
        logger.setLevel(level)

    _LOG_CONFIGURED = True


_configure_logging()


try:  # предпочитаме fdb (поддържа Firebird 2.5)
    import fdb  # type: ignore

    _FB_API = "fdb"
    _FB_ERROR = fdb.DatabaseError
except ImportError:  # pragma: no cover - fallback към firebird-driver
    fdb = None  # type: ignore
    try:
        from firebird.driver import connect as fb_connect  # type: ignore
        from firebird.driver import Error as _FB_ERROR  # type: ignore

        _FB_API = "firebird-driver"
    except ImportError as exc:  # pragma: no cover
        raise ImportError(
            "Не е открит Firebird драйвер. Инсталирайте 'fdb' или 'firebird-driver'."
        ) from exc


class MistralDBError(RuntimeError):
    """Базово контролирано изключение."""


class UnsupportedAuthSchema(MistralDBError):
    """Непозната auth схема."""


def _profile_label() -> str:
    profile = _PROFILE or {}
    for key in ("label", "name", "client", "profile", "profile_name"):
        value = profile.get(key)
        if value:
            return str(value)
    if _PROFILE_LABEL:
        return _PROFILE_LABEL
    database = profile.get("database")
    if database:
        return str(database)
    return "неизвестен"


def _require_connection() -> Any:
    if _CONN is None:
        raise MistralDBError(
            f"Няма активна връзка – опитайте отново (профил: {_profile_label()})."
        )
    return _CONN


def _require_cursor(
    conn: Any | None = None, cur: Any | None = None, profile_label: str | None = None
) -> Any:
    label = profile_label or _profile_label()
    active_conn = conn if conn is not None else _CONN
    active_cur = cur if cur is not None else _CUR
    if not active_conn or not active_cur:
        raise MistralDBError(f"Няма активна връзка – опитайте отново (профил: {label}).")
    return active_cur


def _record_login_step(step: Dict[str, Any]) -> None:
    step_copy = dict(step)
    step_copy.setdefault("timestamp", datetime.now().isoformat(timespec="seconds"))
    _LOGIN_TRACE.append(step_copy)


def get_last_login_trace() -> List[Dict[str, Any]]:
    return list(_LOGIN_TRACE)


@contextmanager
def _transaction() -> Iterable[Any]:
    conn = _require_connection()
    try:
        conn.begin()
    except AttributeError:  # firebird-driver автоматично стартира транзакция
        pass
    try:
        yield conn
        conn.commit()
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        raise


def _connect_raw(host: str, port: int, database: str, user: str, password: str, charset: str):
    if _FB_API == "fdb":  # pragma: no branch - основен сценарий при 2.5
        return fdb.connect(  # type: ignore[arg-type]
            host=host,
            port=port,
            database=database,
            user=user,
            password=password,
            charset=charset,
        )
    return fb_connect(  # type: ignore[misc]
        host=host,
        port=port,
        database=database,
        user=user,
        password=password,
        charset=charset,
    )


def _field_type_name(
    field_type: int,
    sub_type: Optional[int],
    length: Optional[int],
    precision: Optional[int],
    scale: Optional[int],
    char_length: Optional[int],
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
        45: "BLOB_ID",
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


def _table_columns(table: str) -> Dict[str, Dict[str, Any]]:
    table = table.upper()
    if table in _TABLE_COLUMNS:
        return _TABLE_COLUMNS[table]
    conn = _require_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT
            TRIM(rf.rdb$field_name) AS col_name,
            COALESCE(rf.rdb$null_flag, 0) AS null_flag,
            TRIM(rf.rdb$field_source) AS field_source,
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
    data: Dict[str, Dict[str, Any]] = {}
    for row in cur.fetchall():
        name = row[0]
        data[name] = {
            "not_null": bool(row[1]),
            "field_type": row[3],
            "field_sub_type": row[4],
            "field_length": row[5],
            "field_precision": row[6],
            "field_scale": row[7],
            "char_length": row[8],
            "type_name": _field_type_name(row[3], row[4], row[5], row[6], row[7], row[8]),
        }
    cur.close()
    _TABLE_COLUMNS[table] = data
    return data


def _next_id(table: str, generator_hint: Optional[str]) -> int:
    conn = _require_connection()
    cur = conn.cursor()
    if generator_hint:
        cur.execute(f"SELECT GEN_ID({generator_hint}, 1) FROM RDB$DATABASE")
        value = cur.fetchone()[0]
        cur.close()
        return int(value)
    cur.execute(f"SELECT COALESCE(MAX(ID), 0) + 1 FROM {table}")
    value = cur.fetchone()[0]
    cur.close()
    return int(value or 1)


def _hash_with_algo(plain: str, salt: Optional[str], algo: str) -> str:
    data = plain if salt in (None, "") else f"{plain}{salt}"
    raw = data.encode("utf-8")
    if algo == "PLAIN":
        return data
    if algo == "MD5":
        return hashlib.md5(raw).hexdigest()
    if algo == "SHA1":
        return hashlib.sha1(raw).hexdigest()
    if algo == "SHA256":
        return hashlib.sha256(raw).hexdigest()
    raise ValueError(f"Непознат hash алгоритъм: {algo}")


def _guess_algorithms(stored: str, field_name: str) -> List[str]:
    stored = stored.strip()
    if not stored:
        return []
    algos: List[str] = ["PLAIN"]
    is_hex = all(c in "0123456789abcdefABCDEF" for c in stored)
    if len(stored) == 32 and is_hex:
        algos.append("MD5")
    elif len(stored) == 40 and is_hex:
        algos.append("SHA1")
    elif len(stored) == 64 and is_hex:
        algos.append("SHA256")
    else:
        if "HASH" in field_name.upper() or is_hex:
            algos.extend(["MD5", "SHA1", "SHA256"])
    return algos


def _match_password(
    plain: str,
    stored: Any,
    salts: Sequence[Any],
    field_name: str,
) -> Tuple[bool, bool]:
    if stored is None:
        return False, False
    stored_str = str(stored).strip()
    if not stored_str:
        return False, False
    salts_clean = [str(s) for s in salts if s not in (None, "")]
    algos = _guess_algorithms(stored_str, field_name)
    if not algos:
        algos = ["PLAIN", "MD5", "SHA1", "SHA256"]
    for algo in algos:
        for salt in [None] + salts_clean:
            candidate = _hash_with_algo(plain, salt, algo)
            if candidate.lower() == stored_str.lower():
                return True, False
    looks_hex = all(c in "0123456789abcdefABCDEF" for c in stored_str)
    unknown = bool(salts_clean)
    if not unknown and looks_hex and len(stored_str) not in {32, 40, 64}:
        unknown = True
    if not unknown and "HASH" in field_name.upper():
        unknown = True
    return False, unknown


def _ensure_delivery_meta(cur: Any) -> Tuple[str, str]:
    global _DELIVERY_TABLES
    if _DELIVERY_TABLES:
        return _DELIVERY_TABLES["header"], _DELIVERY_TABLES["detail"]
    cur.execute(
        """
        SELECT TRIM(r.rdb$relation_name)
        FROM rdb$relations r
        WHERE r.rdb$view_blr IS NULL
          AND COALESCE(r.rdb$system_flag, 0) = 0
          AND UPPER(r.rdb$relation_name) LIKE 'TEMPDELIVERY%'
        ORDER BY 1
        """
    )
    names = [row[0] for row in cur.fetchall()]
    header = None
    detail = None
    for name in names:
        up = name.upper()
        if up.endswith("SDR") or "DETAIL" in up or "ITEM" in up:
            detail = name
        else:
            header = name
    if not header:
        raise MistralDBError("Не намирам таблица за OPEN доставка (TEMPDELIVERY).")
    if not detail:
        raise MistralDBError("Не намирам таблица за редове на OPEN доставка (TEMPDELIVERYSDR).")
    _DELIVERY_TABLES = {"header": header, "detail": detail}
    return header, detail


def _ensure_delivery_generators(cur: Any) -> Tuple[Optional[str], Optional[str]]:
    global _DELIVERY_GENERATORS
    if _DELIVERY_GENERATORS:
        return _DELIVERY_GENERATORS["header"], _DELIVERY_GENERATORS["detail"]
    cur.execute(
        """
        SELECT TRIM(rdb$generator_name)
        FROM rdb$generators
        WHERE UPPER(rdb$generator_name) LIKE '%TEMPDELIVERY%'
        """
    )
    header_gen: Optional[str] = None
    detail_gen: Optional[str] = None
    for row in cur.fetchall():
        name = row[0]
        up = name.upper()
        if "SDR" in up or "DETAIL" in up:
            detail_gen = name
        else:
            header_gen = name
    _DELIVERY_GENERATORS = {"header": header_gen, "detail": detail_gen}
    return header_gen, detail_gen


def connect(profile: Dict[str, Any]) -> Tuple[Any, Any]:
    """Установява връзка към Firebird и връща (connection, cursor)."""
    global _CONN, _CUR, _PROFILE, _PROFILE_LABEL, _LOGIN_META
    if "database" not in profile:
        raise MistralDBError("В профила липсва ключ 'database'.")

    host = profile.get("host", "localhost")
    port = int(profile.get("port", 3050))
    database = profile["database"]
    user = profile.get("user", "SYSDBA")
    password = profile.get("password", "masterkey")
    charset = profile.get("charset", "WIN1251") or "WIN1251"

    profile_label = str(
        profile.get("label")
        or profile.get("name")
        or profile.get("client")
        or profile.get("profile_name")
        or database
    )

    logger.info(
        f"Свързване към база (профил: {profile_label}, host={host}, database={database}, driver={_FB_API})."
    )
    try:
        conn = _connect_raw(host, port, database, user, password, charset)
        cur = conn.cursor()
    except Exception as exc:  # pragma: no cover - защитно
        logger.exception(
            "Неуспешно свързване към база (профил: %s). host=%s, database=%s", profile_label, host, database
        )
        raise MistralDBError(
            f"Грешка при свързване към база (профил: {profile_label}). Проверете хост/порт/права."
        ) from exc

    _CONN = conn
    _CUR = cur
    _PROFILE = dict(profile)
    _PROFILE_LABEL = profile_label
    _LOGIN_META = None
    _DELIVERY_TABLES = None
    _DELIVERY_GENERATORS = None
    _TABLE_COLUMNS.clear()
    _DELIVERY_CONTEXT.clear()
    logger.info("Свързването е успешно (профил: %s).", profile_label)
    return conn, cur


def detect_login_method(cur: Any | None = None) -> Dict[str, Any]:
    """Открива дали се ползва LOGIN процедура или USERS/LOGUSERS."""

    profile_label = _profile_label()
    cur = _require_cursor(_CONN, cur, profile_label)
    logger.debug("Откриване на login механизъм (профил: %s).", profile_label)

    cur.execute(
        """
        SELECT TRIM(p.rdb$procedure_name), COALESCE(p.rdb$procedure_type, 2)
        FROM rdb$procedures p
        WHERE (p.rdb$system_flag IS NULL OR p.rdb$system_flag = 0)
          AND UPPER(p.rdb$procedure_name) LIKE '%LOGIN%'
        ORDER BY 1
        """
    )
    procs = cur.fetchall()
    for raw_name, proc_type in procs:
        name = (raw_name or "").strip()
        if not name:
            continue
        conn = getattr(cur, "connection", None) or _require_connection()
        pcur = _require_cursor(conn, conn.cursor(), profile_label)
        pcur.execute(
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
        inputs: List[Dict[str, Any]] = []
        outputs: List[Dict[str, Any]] = []
        for row in pcur.fetchall():
            entry = {
                "name": row[1],
                "position": int(row[2]),
                "field_type": row[3],
                "field_scale": row[7],
                "type_name": _field_type_name(row[3], row[4], row[5], row[6], row[7], row[8]),
            }
            (inputs if row[0] == 0 else outputs).append(entry)
        pcur.close()
        source_cur = _require_cursor(conn, conn.cursor(), profile_label)
        source_cur.execute(
            "SELECT rdb$procedure_source FROM rdb$procedures WHERE rdb$procedure_name = ?",
            (name,),
        )
        source_row = source_cur.fetchone()
        source_cur.close()
        source_text = (source_row[0] or "") if source_row else ""
        source_upper = source_text.upper() if isinstance(source_text, str) else ""
        sp_kind = "selectable" if int(proc_type or 2) == 1 else "executable"
        if "SUSPEND" in source_upper:
            sp_kind = "selectable"
        if inputs:
            meta = {
                "mode": "sp",
                "name": name,
                "sp_kind": sp_kind,
                "fields": {
                    "inputs": inputs,
                    "outputs": outputs,
                },
            }
            logger.info(f"Открита login процедура: {name} ({sp_kind}).")
            return meta

    table_candidates: List[Dict[str, Any]] = []
    for table_name in ("USERS", "LOGUSERS"):
        cols = _table_columns(table_name)
        if not cols:
            continue
        upper_map = {col.upper(): col for col in cols}
        login_candidates = (
            "NAME",
            "LOGIN",
            "USERNAME",
            "USER_NAME",
            "CODE",
            "USERCODE",
            "OPERATOR",
        )
        login_col = None
        for candidate in login_candidates:
            if candidate in upper_map:
                login_col = upper_map[candidate]
                break
        has_name = login_col is not None
        has_pass = "PASS" in upper_map
        if not has_pass:
            continue
        id_col = None
        for candidate in ("ID", "CODE", "KOD", "USER_ID", "OP_ID"):
            if candidate in upper_map:
                id_col = upper_map[candidate]
                break
        if not id_col:
            continue
        salt_col = None
        for candidate in ("SALT", "PASS_SALT", "PASSWORD_SALT", "SALT1"):
            if candidate in upper_map:
                salt_col = upper_map[candidate]
                break
        entry = {
            "mode": "table",
            "name": table_name,
            "sp_kind": None,
            "fields": {
                "id": id_col,
                "login": login_col,
                "password": upper_map["PASS"],
                "salt": salt_col,
                "has_name": has_name,
                "has_pass": has_pass,
            },
            "columns": cols,
        }
        table_candidates.append(entry)

    if table_candidates:
        primary = dict(table_candidates[0])
        primary["candidates"] = table_candidates
        has_name = "да" if primary["fields"].get("has_name") else "не"
        has_pass = "да" if primary["fields"].get("has_pass") else "не"
        logger.info(
            f"Открит login чрез таблица: {primary['name']} (NAME={has_name}, PASS={has_pass})."
        )
        return primary

    logger.error("Не е открит механизъм за логин (профил: %s).", profile_label)
    raise UnsupportedAuthSchema(
        "Не успях да открия механизъм за логин. Нужна е допълнителна конфигурация."
    )


def login_user(username: str, password: str) -> Tuple[int, str]:
    """Връща (operator_id, operator_login) или вдига MistralDBError."""

    global _LOGIN_META, _LOGIN_TRACE
    cur = _require_cursor()
    username = username or ""
    password = password or ""
    _LOGIN_TRACE.clear()
    _record_login_step(
        {"action": "start", "profile": _profile_label(), "username": username or "<само парола>"}
    )
    logger.info(
        "Старт на логин (профил: %s, потребител: %s)",
        _profile_label(),
        username or "<само парола>",
    )

    if _LOGIN_META is None:
        _LOGIN_META = detect_login_method(cur)
    meta = _LOGIN_META

    _record_login_step(
        {
            "action": "detected_mode",
            "mode": meta.get("mode"),
            "name": meta.get("name"),
            "sp_kind": meta.get("sp_kind"),
        }
    )
    logger.info(
        "Използван механизъм за логин: %s (%s)",
        meta.get("mode"),
        meta.get("name"),
    )

    try:
        if meta.get("mode") == "sp":
            result = _login_via_procedure(meta, username, password)
        else:
            result = _login_via_table(meta, username, password)
    except MistralDBError as exc:
        _record_login_step({"action": "failure", "message": str(exc)})
        logger.warning(
            "Неуспешен вход (профил: %s, потребител: %s): %s",
            _profile_label(),
            username or "<само парола>",
            exc,
        )
        raise

    operator_id, operator_login = result
    _record_login_step(
        {
            "action": "success",
            "operator_id": operator_id,
            "operator_login": operator_login,
            "mode": meta.get("mode"),
        }
    )
    logger.info(
        "Успешен вход (профил: %s, потребител: %s, режим: %s).",
        _profile_label(),
        username or operator_login or "<само парола>",
        meta.get("mode"),
    )
    return operator_id, operator_login


def _build_procedure_args(inputs: List[Dict[str, Any]], username: str, password: str) -> List[Any]:
    login_param_names = {"LOGIN", "USERNAME", "USER_NAME", "CODE", "OPERATOR"}
    pass_param_names = {"PASS", "PASSWORD", "PAROLA", "PWD"}
    args: List[Any] = [None] * len(inputs)
    for field in inputs:
        pname = (field.get("name") or "").upper()
        pos = field.get("position", 0)
        if pname in login_param_names:
            args[pos] = username or None
        elif pname in pass_param_names:
            args[pos] = password
        else:
            args[pos] = None
    return args


def _is_no_result_set_error(exc: Exception) -> bool:
    message = str(exc).lower()
    return "does not produce result set" in message or "no result set" in message


def _extract_operator_from_row(
    row: Sequence[Any], outputs: List[Dict[str, Any]], username: str
) -> Tuple[int, str]:
    operator_id: Optional[int] = None
    operator_login: Optional[str] = None
    sequence = list(row)
    for idx, field in enumerate(outputs):
        value = sequence[idx] if idx < len(sequence) else None
        if value is None:
            continue
        if operator_id is None and isinstance(value, (int, float)):
            try:
                operator_id = int(value)
                continue
            except (TypeError, ValueError):
                pass
        name = (field.get("name") or "").upper()
        if operator_login is None and name and any(token in name for token in ("LOGIN", "USER", "CODE")):
            operator_login = str(value).strip()
    if operator_id is None and sequence:
        try:
            operator_id = int(sequence[0])
        except (TypeError, ValueError) as exc:
            raise MistralDBError(
                "Процедурата за логин не върна идентификатор. Нужна е доработка."
            ) from exc
    if operator_login is None and len(sequence) > 1:
        operator_login = str(sequence[1]).strip() if sequence[1] is not None else None
    if operator_login is None:
        operator_login = username or (str(operator_id) if operator_id is not None else "")
    if operator_id is None:
        raise MistralDBError("Невалиден потребител или парола")
    return operator_id, operator_login


def _login_via_procedure(meta: Dict[str, Any], username: str, password: str) -> Tuple[int, str]:
    cur = _require_cursor()
    name = meta.get("name")
    inputs = meta.get("fields", {}).get("inputs", [])
    outputs = meta.get("fields", {}).get("outputs", [])
    args = _build_procedure_args(inputs, username, password)
    placeholders = ", ".join(["?"] * len(inputs))
    sp_kind = meta.get("sp_kind") or "executable"

    def _log_params() -> Dict[str, Any]:
        return {
            "username": username or "<празно>",
            "password": "***" if password else "",
        }

    if sp_kind == "selectable":
        sql = f"SELECT * FROM {name}({placeholders})" if placeholders else f"SELECT * FROM {name}"
        _record_login_step(
            {
                "action": "procedure_attempt",
                "procedure": name,
                "mode": "select",
                "sql": sql,
                "params": _log_params(),
            }
        )
        logger.info("Login чрез процедура %s (SELECT режим).", name)
        try:
            cur.execute(sql, args)
            rows = cur.fetchall()
        except _FB_ERROR as exc:
            if _is_no_result_set_error(exc):
                logger.warning(
                    "Процедурата %s не връща резултат при SELECT. Превключваме към EXECUTE.",
                    name,
                )
                _record_login_step(
                    {
                        "action": "procedure_switch",
                        "procedure": name,
                        "from": "select",
                        "to": "execute",
                        "reason": "no-result-set",
                    }
                )
                meta["sp_kind"] = "executable"
            else:
                _record_login_step(
                    {
                        "action": "procedure_error",
                        "procedure": name,
                        "mode": "select",
                        "error": str(exc),
                    }
                )
                raise MistralDBError(f"Грешка при изпълнение на {name}: {exc}") from exc
        else:
            row_count = len(rows)
            _record_login_step(
                {
                    "action": "procedure_result",
                    "procedure": name,
                    "mode": "select",
                    "rows": row_count,
                }
            )
            logger.debug("Процедура %s (SELECT) върна %s ред(а).", name, row_count)
            if row_count == 0:
                raise MistralDBError("Невалиден потребител или парола")
            row = rows[0]
            if row_count > 1:
                logger.warning(
                    "Процедура %s върна повече от един ред (%s). Използваме първия.",
                    name,
                    row_count,
                )
            operator_id, operator_login = _extract_operator_from_row(row, outputs, username)
            return operator_id, operator_login

    exec_sql = (
        f"EXECUTE PROCEDURE {name} {placeholders}" if placeholders else f"EXECUTE PROCEDURE {name}"
    )
    _record_login_step(
        {
            "action": "procedure_attempt",
            "procedure": name,
            "mode": "execute",
            "sql": exec_sql,
            "params": _log_params(),
        }
    )
    logger.info("Login чрез процедура %s (EXECUTE режим).", name)
    row: Optional[Sequence[Any]] = None
    try:
        cur.execute(exec_sql, args)
        try:
            fetched = cur.fetchone()
        except _FB_ERROR as exc:
            if _is_no_result_set_error(exc):
                fetched = None
            else:
                _record_login_step(
                    {
                        "action": "procedure_error",
                        "procedure": name,
                        "mode": "execute",
                        "error": str(exc),
                    }
                )
                raise MistralDBError(f"Грешка при изпълнение на {name}: {exc}") from exc
        row = fetched
    except _FB_ERROR as exc:
        _record_login_step(
            {
                "action": "procedure_error",
                "procedure": name,
                "mode": "execute",
                "error": str(exc),
            }
        )
        raise MistralDBError(f"Грешка при изпълнение на {name}: {exc}") from exc

    if row is None and hasattr(cur, "callproc"):
        _record_login_step(
            {
                "action": "procedure_callproc",
                "procedure": name,
                "params": _log_params(),
            }
        )
        logger.debug("Опит за callproc върху %s.", name)
        try:
            call_result = cur.callproc(name, tuple(args))  # type: ignore[attr-defined]
        except Exception as exc:  # pragma: no cover - защитно
            _record_login_step(
                {
                    "action": "procedure_error",
                    "procedure": name,
                    "mode": "callproc",
                    "error": str(exc),
                }
            )
            raise MistralDBError(f"Грешка при изпълнение на {name}: {exc}") from exc
        if isinstance(call_result, (list, tuple)):
            row = list(call_result)
        else:
            row = [call_result]

    if row is None:
        _record_login_step(
            {
                "action": "procedure_result",
                "procedure": name,
                "mode": "execute",
                "rows": 0,
            }
        )
        raise MistralDBError("Невалиден потребител или парола")

    _record_login_step(
        {
            "action": "procedure_result",
            "procedure": name,
            "mode": "execute",
            "rows": 1,
        }
    )
    return _extract_operator_from_row(row, outputs, username)


def _login_via_table(meta: Dict[str, Any], username: str, password: str) -> Tuple[int, str]:
    cur = _require_cursor()
    candidates = meta.get("candidates") or [meta]
    username = username.strip()
    password = password.strip()

    for candidate in candidates:
        table = candidate.get("name")
        fields = candidate.get("fields", {})
        id_col = fields.get("id")
        login_col = fields.get("login")
        pass_col = fields.get("password")
        salt_col = fields.get("salt")
        if not (table and id_col and pass_col):
            continue

        base_select = [f"{id_col} AS ID_FIELD"]
        if login_col:
            base_select.append(f"COALESCE({login_col}, '') AS LOGIN_FIELD")
        else:
            base_select.append("'' AS LOGIN_FIELD")
        base_select.append(f"{pass_col} AS PASS_FIELD")
        if salt_col:
            base_select.append(f"{salt_col} AS SALT_FIELD")
        select_clause = ", ".join(base_select)

        use_username = bool(username and login_col)

        hashed_variants = []
        if not use_username:
            try:
                hashed_variants = list(
                    dict.fromkeys(
                        _hash_with_algo(password, None, algo)
                        for algo in ("PLAIN", "MD5", "SHA1", "SHA256")
                    )
                )
            except ValueError:
                hashed_variants = [password]

        queries: List[Tuple[str, Sequence[Any], str]] = []
        where_parts: List[str] = []
        params: List[Any] = []
        if use_username:
            where_parts.append(f"UPPER({login_col}) = UPPER(?)")
            params.append(username)
        if not use_username and hashed_variants:
            placeholders = ", ".join(["?"] * len(hashed_variants))
            where_parts.append(f"{pass_col} IN ({placeholders})")
            params.extend(hashed_variants)
        sql = f"SELECT {select_clause} FROM {table}"
        if where_parts:
            sql += " WHERE " + " AND ".join(where_parts)
        queries.append((sql, tuple(params), "filtered" if where_parts else "full"))

        if not use_username and salt_col:
            # За salted хешове може да се наложи пълен скан
            scan_sql = f"SELECT {select_clause} FROM {table}"
            queries.append((scan_sql, tuple(), "scan"))

        unknown_algorithms = False
        for query_sql, query_params, mode in queries:
            _record_login_step(
                {
                    "action": "table_attempt",
                    "table": table,
                    "mode": "username+password" if use_username else "password-only",
                    "sql": query_sql,
                    "params": {
                        "username": username if use_username else None,
                        "password": "***" if password else "",
                        "query_mode": mode,
                    },
                }
            )
            logger.info(
                "Login чрез таблица %s (%s режим, заявка: %s).",
                table,
                "потребител+парола" if use_username else "само парола",
                mode,
            )
            try:
                cur.execute(query_sql, query_params)
                rows = cur.fetchall()
            except _FB_ERROR as exc:
                _record_login_step(
                    {
                        "action": "table_error",
                        "table": table,
                        "error": str(exc),
                    }
                )
                raise MistralDBError(f"Грешка при четене от {table}: {exc}") from exc

            row_count = len(rows)
            _record_login_step({"action": "table_result", "table": table, "rows": row_count})
            logger.debug(
                "Запитване към %s (%s) върна %s ред(а).", table, mode, row_count
            )

            if row_count == 0:
                continue
            if use_username and row_count > 5:
                logger.warning(
                    "Намерени са повече от 5 реда за потребител %s в %s.", username, table
                )

            columns = [desc[0].strip().upper() for desc in cur.description]
            for row in rows:
                data = dict(zip(columns, row))
                stored_pass = data.get("PASS_FIELD")
                salts = [data.get("SALT_FIELD")] if "SALT_FIELD" in data else []
                match, unknown = _match_password(password, stored_pass, salts, pass_col)
                if match:
                    operator_id = int(data.get("ID_FIELD"))
                    operator_login = str(data.get("LOGIN_FIELD") or "").strip() or (
                        username if use_username else str(operator_id)
                    )
                    meta["name"] = table
                    return operator_id, operator_login
                if unknown:
                    unknown_algorithms = True

        if unknown_algorithms:
            raise MistralDBError("Нужна е информация за алгоритъма на паролите.")

    raise MistralDBError("Невалиден потребител или парола")


def get_item_info(code_or_name: str) -> Optional[Dict[str, Any]]:
    """Търсене на материал по код/баркод/име."""
    cur = _require_cursor()
    value = code_or_name.strip()
    if not value:
        return None

    base_select = (
        "SELECT m.ID, m.MATERIALCODE, m.UNIQUECODE, m.MATERIAL, m.SEARCHNAME, "
        "m.LASTDELIVERYPRICE, m.AVGDELIVERYPRICE, m.LASTDELIVERYPRICEWOTAX, "
        "m.AVGDELIVERYPRICEWOTAX, m.QTY, m.TAXGROUPID, b.CODE AS BARCODE, tg.TAXPERCENTAGE "
        "FROM MATERIAL m "
        "LEFT JOIN BARCODE b ON b.STORAGEMATERIALCODE = m.MATERIALCODE AND b.LOCATIONID = m.LOCATIONID "
        "LEFT JOIN TAXGROUP tg ON tg.ID = m.TAXGROUPID "
    )

    def _row_to_dict(row: Tuple[Any, ...], columns: Sequence[str]) -> Dict[str, Any]:
        data = dict(zip(columns, row))
        price = Decimal(str(data.get("LASTDELIVERYPRICE") or 0))
        price_wo_vat = Decimal(str(data.get("LASTDELIVERYPRICEWOTAX") or 0))
        vat = data.get("TAXPERCENTAGE")
        return {
            "id": int(data["ID"]),
            "code": int(data["MATERIALCODE"]),
            "unique_code": int(data.get("UNIQUECODE") or 0),
            "name": (data.get("MATERIAL") or data.get("SEARCHNAME") or "").strip(),
            "barcode": (data.get("BARCODE") or "").strip() or None,
            "price": price,
            "price_no_vat": price_wo_vat,
            "avg_price": Decimal(str(data.get("AVGDELIVERYPRICE") or 0)),
            "vat": Decimal(str(vat or 0)),
            "qty": Decimal(str(data.get("QTY") or 0)),
            "tax_group_id": data.get("TAXGROUPID"),
        }

    def _query(where: str, params: Sequence[Any]) -> Optional[Dict[str, Any]]:
        sql = base_select + where
        cur.execute(sql, params)
        row = cur.fetchone()
        if not row:
            return None
        columns = [desc[0].strip().upper() for desc in cur.description]
        return _row_to_dict(row, columns)

    # 1) точен match по материален код
    if value.isdigit():
        result = _query("WHERE m.MATERIALCODE = ?", (int(value),))
        if result:
            return result

    # 2) точен match по баркод
    result = _query("WHERE b.CODE = ?", (value,))
    if result:
        return result

    # 3) fallback по LIKE име
    like_param = f"%{value.upper()}%"
    result = _query("WHERE UPPER(m.MATERIAL) LIKE ? ORDER BY m.MATERIALCODE", (like_param,))
    return result


def create_open_delivery(operator_id: int) -> int:
    """Създава OPEN доставка и връща нейния ID."""
    if operator_id is None:
        raise MistralDBError("Липсва operator_id за OPEN доставка.")
    _require_cursor()
    conn = _require_connection()
    cur = conn.cursor()
    header_table, _ = _ensure_delivery_meta(cur)
    header_gen, _ = _ensure_delivery_generators(cur)
    columns = _table_columns(header_table)
    location_id = (_PROFILE or {}).get("location_id")
    storage_id = (_PROFILE or {}).get("storage_id")
    doc_type = (_PROFILE or {}).get("operation_doc_type")
    now = datetime.now()
    delivery_id = _next_id(header_table, header_gen)

    values: Dict[str, Any] = {"ID": delivery_id}
    if "OBEKTID" in columns and location_id is not None:
        values["OBEKTID"] = int(location_id)
    if "LOCATIONID" in columns and location_id is not None:
        values["LOCATIONID"] = int(location_id)
    if "STORAGEID" in columns and storage_id is not None:
        values["STORAGEID"] = int(storage_id)
    if "NOMER" in columns:
        cur.execute(
            f"SELECT COALESCE(MAX(NOMER), 0) + 1 FROM {header_table}"
            + (" WHERE OBEKTID = ?" if "OBEKTID" in values else ""),
            ((values.get("OBEKTID"),) if "OBEKTID" in values else ()),
        )
        nomer = cur.fetchone()[0]
        values["NOMER"] = int(nomer or delivery_id)
    if "USERSID" in columns:
        values["USERSID"] = int(operator_id)
    if "DTSAVE" in columns:
        values["DTSAVE"] = now
    if "DOCDATE" in columns:
        values["DOCDATE"] = date.today()
    if "DOCTYPEID" in columns and doc_type is not None:
        values["DOCTYPEID"] = int(doc_type)
    if "TYPEDB" in columns:
        values["TYPEDB"] = 0
    if "RAZCR" in columns:
        values["RAZCR"] = "O"
    if "CHRFORCHECK" in columns:
        values["CHRFORCHECK"] = "0"
    if "NOTE" in columns:
        values["NOTE"] = "MicroVision импорт от MicroVision Invoice Parser"

    column_names = list(values.keys())
    placeholders = ", ".join(["?"] * len(column_names))
    sql = f"INSERT INTO {header_table} ({', '.join(column_names)}) VALUES ({placeholders})"
    try:
        with _transaction():
            conn.cursor().execute(sql, [values[col] for col in column_names])
    except _FB_ERROR as exc:
        raise MistralDBError(f"Неуспешно създаване на OPEN доставка: {exc}") from exc

    _DELIVERY_CONTEXT[delivery_id] = {"nomer": values.get("NOMER"), "header_table": header_table}
    return delivery_id


def push_items_to_mistral(delivery_id: int, items: List[Dict[str, Any]]) -> None:
    """Вкарва редовете за доставка в TEMPDELIVERYSDR."""
    if not items:
        return
    _require_cursor()
    conn = _require_connection()
    cur = conn.cursor()
    header_table, detail_table = _ensure_delivery_meta(cur)
    _, detail_gen = _ensure_delivery_generators(cur)
    header_cols = _table_columns(header_table)
    detail_cols = _table_columns(detail_table)

    nomer = None
    if delivery_id in _DELIVERY_CONTEXT:
        nomer = _DELIVERY_CONTEXT[delivery_id].get("nomer")
    if nomer is None and "NOMER" in header_cols:
        cur.execute(f"SELECT NOMER FROM {header_table} WHERE ID = ?", (delivery_id,))
        row = cur.fetchone()
        if row:
            nomer = row[0]

    location_id = (_PROFILE or {}).get("location_id")
    storage_id = (_PROFILE or {}).get("storage_id")

    def _find_col(*candidates: str) -> Optional[str]:
        for name in candidates:
            if name in detail_cols:
                return name
        return None

    temp_id_col = _find_col("TEMPDELIVERYID", "TEMPDELIVERY_ID", "HEADERID")
    nomer_col = "NOMER" if "NOMER" in detail_cols else None
    obekt_col = _find_col("OBEKTID", "LOCATIONID")
    sklad_col = _find_col("CKLADID", "STORAGEID")
    art_col = _find_col("ARTNOMER", "MATERIALCODE", "ITEMCODE")
    qty_col = _find_col("QTY", "KOL", "KOLICHESTVO")
    price_col = _find_col("EDPRICE", "PRICE", "DELIVERYPRICE")
    price_vat_col = _find_col("EDPRICEDDS", "PRICEVAT")
    sum_col = _find_col("SUMA", "SUMPRICE")
    sum_vat_col = _find_col("SUMADDS", "SUMPRICEVAT")
    barcode_col = _find_col("BARCODE")
    sale_price_col = _find_col("SALESPRICE")
    sale_price_vat_col = _find_col("SALESPRICEDDS")
    sum_sale_col = _find_col("SUMASALESPRICE")
    sum_sale_vat_col = _find_col("SUMASALESPRICEDDS")

    try:
        with _transaction():
            for item in items:
                detail_id = _next_id(detail_table, detail_gen)
                qty = Decimal(str(item.get("qty", "0")))
                price = Decimal(str(item.get("price", "0")))
                vat = Decimal(str(item.get("vat", "0")))
                price_with_vat = (price * (Decimal("1") + vat / Decimal("100"))).quantize(
                    Decimal("0.0001"), rounding=ROUND_HALF_UP
                ) if vat else price
                sum_without_vat = (price * qty).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)
                sum_with_vat = (price_with_vat * qty).quantize(
                    Decimal("0.0001"), rounding=ROUND_HALF_UP
                ) if vat else sum_without_vat

                values: Dict[str, Any] = {"ID": detail_id}
                if temp_id_col:
                    values[temp_id_col] = delivery_id
                if nomer_col and nomer is not None:
                    values[nomer_col] = nomer
                if obekt_col and location_id is not None:
                    values[obekt_col] = int(location_id)
                if sklad_col and storage_id is not None:
                    values[sklad_col] = int(storage_id)
                if art_col:
                    values[art_col] = int(item.get("code") or item.get("material_code") or 0)
                if qty_col:
                    values[qty_col] = qty
                if price_col:
                    values[price_col] = price
                if price_vat_col:
                    values[price_vat_col] = price_with_vat
                if sum_col:
                    values[sum_col] = sum_without_vat
                if sum_vat_col:
                    values[sum_vat_col] = sum_with_vat
                if barcode_col and item.get("barcode"):
                    values[barcode_col] = item["barcode"]
                if sale_price_col and item.get("sale_price") is not None:
                    sale_price = Decimal(str(item.get("sale_price")))
                    values[sale_price_col] = sale_price
                    if sale_price_vat_col:
                        sale_price_vat = (sale_price * (Decimal("1") + vat / Decimal("100"))).quantize(
                            Decimal("0.0001"), rounding=ROUND_HALF_UP
                        ) if vat else sale_price
                        values[sale_price_vat_col] = sale_price_vat
                    if sum_sale_col:
                        sum_sale = (sale_price * qty).quantize(
                            Decimal("0.0001"), rounding=ROUND_HALF_UP
                        )
                        values[sum_sale_col] = sum_sale
                    if sum_sale_vat_col:
                        sum_sale_vat = (values.get(sale_price_vat_col, sale_price) * qty).quantize(
                            Decimal("0.0001"), rounding=ROUND_HALF_UP
                        )
                        values[sum_sale_vat_col] = sum_sale_vat

                cols = list(values.keys())
                placeholders = ", ".join(["?"] * len(cols))
                sql = f"INSERT INTO {detail_table} ({', '.join(cols)}) VALUES ({placeholders})"
                conn.cursor().execute(sql, [values[col] for col in cols])
    except _FB_ERROR as exc:
        raise MistralDBError(f"Грешка при запис на артикули: {exc}") from exc


def _looks_like_numeric(field: Dict[str, Any]) -> bool:
    return field.get("field_type") in {7, 8, 9, 16, 27}


# --- модулни наследени обвивки (поддръжка на стария клас базиран API) ---
class DBConfig:  # pragma: no cover - поддръжка за по-стария код
    def __init__(
        self,
        database: str,
        host: str = "localhost",
        port: int = 3050,
        user: str = "SYSDBA",
        password: str = "masterkey",
        charset: str = "WIN1251",
    ) -> None:
        self.database = database
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.charset = charset


class MistralDB:  # pragma: no cover - thin wrapper за обратна съвместимост
    def __init__(self, conf: DBConfig, auth_profile: Optional[dict] = None) -> None:
        profile = {
            "database": conf.database,
            "host": conf.host,
            "port": conf.port,
            "user": conf.user,
            "password": conf.password,
            "charset": conf.charset,
        }
        if auth_profile:
            profile.update(auth_profile)
        self.profile = profile

    def connect(self):
        return connect(self.profile)[0]

    def cursor(self):
        return _require_cursor()

    def authenticate_operator(self, login: str, password: str) -> Optional[int]:
        try:
            operator_id, _ = login_user(login, password)
            return operator_id
        except MistralDBError:
            return None

    def authenticate_operator_password_only(self, password: str) -> Optional[int]:
        try:
            operator_id, _ = login_user("", password)
            return operator_id
        except MistralDBError:
            return None
