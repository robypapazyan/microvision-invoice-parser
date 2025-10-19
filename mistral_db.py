"""Utility helpers for talking to a Mistral (Firebird) database."""
from __future__ import annotations

import hashlib
import logging
import os
from contextlib import contextmanager
from datetime import datetime, date
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
# logging handlers are imported lazily in the configuration helper
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple
import re

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
_CATALOG_SCHEMA: Dict[str, str | None] | None = None
_DELIVERY_CONTEXT: Dict[int, Dict[str, Any]] = {}
_FIELD_LENGTH_CACHE: Dict[tuple[str, str], int] = {}
_last_login_trace: List[Dict[str, Any]] = []


logger: Any
if _loguru_logger is not None:
    logger = _loguru_logger
else:  # pragma: no cover - при липса на loguru
    logger = logging.getLogger("microvision")


def _cleanup_old_logs(log_dir: Path, keep: int = 14) -> None:
    try:
        log_files = sorted(log_dir.glob("app_*.log"))
    except Exception:  # pragma: no cover - защитно
        return
    if keep <= 0:
        return
    excess = len(log_files) - keep
    if excess <= 0:
        return
    for old_file in log_files[:excess]:
        try:
            old_file.unlink()
        except Exception:
            continue


def _configure_logging() -> None:
    global _LOG_CONFIGURED, logger
    if _LOG_CONFIGURED:
        return

    log_dir = Path(__file__).resolve().parent / "logs"
    try:
        log_dir.mkdir(parents=True, exist_ok=True)
    except Exception:  # pragma: no cover - защитно
        pass

    log_level_name = (
        os.getenv("MV_LOG_LEVEL")
        or os.getenv("MICROVISION_LOG_LEVEL")
        or "INFO"
    ).upper() or "INFO"

    if _loguru_logger is not None:
        log_file = log_dir / "app_{time:YYYYMMDD}.log"
        try:
            logger.add(
                log_file,
                rotation="00:00",
                retention="14 days",
                level=log_level_name,
                encoding="utf-8",
            )
        except Exception:  # pragma: no cover - ако loguru е вече конфигуриран
            pass
    else:  # pragma: no cover - logging fallback
        level = getattr(logging, log_level_name, logging.INFO)
        logger.setLevel(level)
        log_file = log_dir / f"app_{datetime.now():%Y%m%d}.log"
        handler = logging.FileHandler(log_file, encoding="utf-8")
        handler.setLevel(level)
        formatter = logging.Formatter(
            "%(asctime)s [%(levelname)s] %(name)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        handler.setFormatter(formatter)
        if not any(getattr(h, "_microvision_daily", False) for h in logger.handlers):
            setattr(handler, "_microvision_daily", True)
            logger.addHandler(handler)
        logger.propagate = False
        _cleanup_old_logs(log_dir)

    _LOG_CONFIGURED = True


def _log_with_level(level: str, message: str, **kwargs: Any) -> None:
    _configure_logging()
    if _loguru_logger is not None:
        bound = logger.bind(**kwargs) if kwargs else logger
        getattr(bound, level)(message)
        return
    if kwargs:
        extras = ", ".join(f"{key}={value}" for key, value in kwargs.items())
        message = f"{message} | {extras}"
    getattr(logger, level)(message)


def _log_info(message: str, **kwargs: Any) -> None:
    _log_with_level("info", message, **kwargs)


def _log_debug(message: str, **kwargs: Any) -> None:
    _log_with_level("debug", message, **kwargs)


def _log_warning(message: str, **kwargs: Any) -> None:
    _log_with_level("warning", message, **kwargs)


def _log_error(message: str, **kwargs: Any) -> None:
    _log_with_level("error", message, **kwargs)


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


def _mask_sensitive(value: Any) -> Any:
    if value in (None, ""):
        return value
    return "***"


def _trace(action: str, **info: Any) -> None:
    entry: Dict[str, Any] = {
        "action": action,
        "timestamp": datetime.now().isoformat(timespec="seconds"),
    }
    for key, value in info.items():
        key_lower = key.lower()
        if "pass" in key_lower or "pwd" in key_lower:
            entry[key] = _mask_sensitive(value)
        else:
            entry[key] = value
    _last_login_trace.append(entry)


def get_last_login_trace() -> List[Dict[str, Any]]:
    return list(_last_login_trace)


def _table_meta_from_login_meta(meta: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not meta:
        return None
    if isinstance(meta.get("mode"), str) and meta.get("mode") == "table":
        return meta
    fallback = meta.get("fallback_table") if isinstance(meta, dict) else None
    if isinstance(fallback, dict):
        return fallback
    return None


def _collect_table_login_candidates() -> List[Dict[str, Any]]:
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
        pass_field = upper_map.get("PASS")
        hash_field = None
        for candidate in ("PASS_HASH", "PASSWORD_HASH", "PWD_HASH", "PAROLA_HASH"):
            if candidate in upper_map:
                hash_field = upper_map[candidate]
                break

        has_pass = pass_field is not None or hash_field is not None
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
                "password": pass_field,
                "password_hash": hash_field,
                "salt": salt_col,
                "has_name": has_name,
                "has_pass": has_pass,
                "has_hash": hash_field is not None,
            },
            "columns": cols,
        }
        table_candidates.append(entry)
    return table_candidates


def _prepare_table_meta(table_candidates: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not table_candidates:
        return None
    primary = dict(table_candidates[0])
    primary["candidates"] = table_candidates
    return primary


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


def _collect_relation_columns(cur: Any) -> Dict[str, List[str]]:
    mapping: Dict[str, List[str]] = {}
    try:
        cur.execute(
            """
            SELECT
                TRIM(rf.rdb$relation_name) AS relation_name,
                TRIM(rf.rdb$field_name) AS column_name
            FROM rdb$relation_fields rf
            JOIN rdb$relations r ON r.rdb$relation_name = rf.rdb$relation_name
            WHERE (r.rdb$system_flag IS NULL OR r.rdb$system_flag = 0)
            ORDER BY relation_name, rf.rdb$field_position
            """
        )
    except _FB_ERROR as exc:
        _log_warning("Нямам достъп до RDB$ метаданни: %s", error=str(exc))
        return {}
    rows = cur.fetchall() or []
    for rel_name, col_name in rows:
        if not rel_name:
            continue
        table = str(rel_name).strip().upper()
        column = str(col_name or "").strip().upper()
        if not column:
            continue
        mapping.setdefault(table, []).append(column)
    return mapping


def _parse_schema_dump() -> Dict[str, List[str]]:
    schema_file = Path(__file__).with_name("schema_TESTBARBERSHOP.sql")
    if not schema_file.exists():
        return {}
    try:
        content = schema_file.read_text(encoding="cp1251", errors="ignore")
    except Exception as exc:  # pragma: no cover - защитно
        _log_warning("Неуспешно четене на schema dump: %s", error=str(exc))
        return {}

    tables: Dict[str, List[str]] = {}
    create_re = re.compile(r"CREATE\s+TABLE\s+\"?([A-Z0-9_]+)\"?\s*\((.*?)\);", re.IGNORECASE | re.DOTALL)
    for match in create_re.finditer(content):
        table = match.group(1).upper()
        body = match.group(2)
        columns: List[str] = []
        for raw_line in body.splitlines():
            line = raw_line.strip().rstrip(",")
            if not line:
                continue
            upper_line = line.upper()
            if upper_line.startswith(("CONSTRAINT", "PRIMARY", "FOREIGN", "UNIQUE", "CHECK")):
                continue
            col_match = re.match(r'"?([A-Z0-9_]+)"?', line, re.IGNORECASE)
            if not col_match:
                continue
            columns.append(col_match.group(1).upper())
        if columns:
            tables[table] = columns
    if not tables:
        _log_warning("Не успях да извлека таблици от schema dump.")
    else:
        _log_debug("Схема от dump е заредена.", tables=len(tables))
    return tables


def _select_column_by_patterns(columns: List[str], patterns: Sequence[str]) -> Optional[str]:
    if not columns:
        return None
    normalized = [col.upper() for col in columns]
    for pattern in patterns:
        pattern_up = pattern.upper()
        if pattern_up in normalized:
            return columns[normalized.index(pattern_up)]
    for pattern in patterns:
        pattern_up = pattern.upper()
        for idx, column in enumerate(columns):
            if pattern_up in column.upper():
                return columns[idx]
    return None


def _contains_pattern(columns: List[str], patterns: Sequence[str]) -> bool:
    return _select_column_by_patterns(columns, patterns) is not None


def _score_material_table(table: str, columns: List[str]) -> float:
    score = 0.0
    upper_table = table.upper()
    if "MATER" in upper_table:
        score += 3
    if any(token in upper_table for token in ("ITEM", "PRODUCT", "GOOD")):
        score += 2
    if _contains_pattern(columns, ["CODE", "ART", "ARTIC", "SKU", "INTERNALCODE", "NOMER", "NUMBER"]):
        score += 2.5
    if _contains_pattern(columns, ["NAME", "MATERIAL", "DESCR", "TITLE", "FULLNAME"]):
        score += 2.5
    if _contains_pattern(columns, ["PRICE", "CENA", "VALUE", "COST", "LASTPRICE", "SALEPRICE", "PURCHASEPRICE"]):
        score += 1.5
    if _contains_pattern(columns, ["VAT", "DDS", "TAX", "TAXRATE"]):
        score += 1.0
    if _contains_pattern(columns, ["UNIT", "MEASURE", "MEAS", "UOM", "EDIN", "EDIZM"]):
        score += 0.5
    return score


def _score_barcode_table(table: str, columns: List[str]) -> float:
    score = 0.0
    upper_table = table.upper()
    if "BARC" in upper_table:
        score += 3
    if _contains_pattern(columns, ["BARCODE", "EAN", "EAN13", "UPC", "CODE"]):
        score += 2.5
    if _contains_pattern(columns, ["MATERIAL", "ITEM", "GOOD", "PRODUCT", "MAT", "ID"]):
        score += 1.0
    return score


def _detect_catalog_schema_from_map(columns_map: Dict[str, List[str]]) -> Dict[str, str | None]:
    if not columns_map:
        return {}

    best_material: tuple[str, float] | None = None
    for table, columns in columns_map.items():
        score = _score_material_table(table, columns)
        if best_material is None or score > best_material[1]:
            best_material = (table, score)

    if not best_material or best_material[1] < 3:
        raise MistralDBError("Не успях да открия таблица с материали. Нужна е ръчна конфигурация.")

    materials_table = best_material[0]
    materials_columns = columns_map[materials_table]
    id_col = _select_column_by_patterns(
        materials_columns,
        ["ID", f"{materials_table}_ID", "MATERIAL", "MATERIALID", "MATID", "ITEMID"],
    )
    code_col = _select_column_by_patterns(
        materials_columns,
        [
            "CODE",
            "MATERIALCODE",
            "ARTIC",
            "ARTICLE",
            "ARTNOMER",
            "INTERNALCODE",
            "NOMER",
        ],
    )
    name_col = _select_column_by_patterns(
        materials_columns,
        ["NAME", "MATERIAL", "DESCR", "DESCRIPTION", "SEARCHNAME", "FULLNAME"],
    )
    uom_col = _select_column_by_patterns(
        materials_columns,
        ["UOM", "MEASURE", "MEASUREUNIT", "UNIT", "EDIN", "EDIZM"],
    )
    price_col = _select_column_by_patterns(
        materials_columns,
        [
            "PRICE",
            "LASTPRICE",
            "LASTDELIVERYPRICE",
            "SALEPRICE",
            "DELIVERYPRICE",
            "PURCHASEPRICE",
        ],
    )
    vat_col = _select_column_by_patterns(
        materials_columns,
        ["VAT", "DDS", "TAX", "TAXRATE", "TAXPERCENTAGE", "DDSPROC"],
    )

    barcode_table: Optional[str] = None
    barcode_columns: List[str] = []
    best_barcode: tuple[str, float] | None = None
    for table, columns in columns_map.items():
        if table == materials_table:
            continue
        score = _score_barcode_table(table, columns)
        if best_barcode is None or score > best_barcode[1]:
            best_barcode = (table, score)
    if best_barcode and best_barcode[1] >= 3:
        barcode_table = best_barcode[0]
        barcode_columns = columns_map.get(barcode_table, [])

    barcode_col = None
    barcode_fk = None
    if barcode_table:
        barcode_col = _select_column_by_patterns(
            barcode_columns,
            ["BARCODE", "EAN", "EAN13", "CODE", "UPC"],
        )
        barcode_fk = _select_column_by_patterns(
            barcode_columns,
            [
                "MATERIAL",
                "MATERIALID",
                "MAT",
                "ITEM",
                "ITEMID",
                "GOOD",
                "PRODUCT",
                "IDMATERIAL",
            ],
        )

    schema = {
        "materials_table": materials_table,
        "materials_id": id_col,
        "materials_code": code_col,
        "materials_name": name_col,
        "materials_uom": uom_col,
        "materials_price": price_col,
        "materials_vat": vat_col,
        "barcode_table": barcode_table,
        "barcode_col": barcode_col,
        "barcode_mat_fk": barcode_fk,
    }

    _log_info(
        "Детектирана е каталожната схема",
        materials_table=materials_table,
        barcode_table=barcode_table or "<няма>",
        code_col=code_col or "<няма>",
        name_col=name_col or "<няма>",
    )
    return schema


def detect_catalog_schema(cur: Any | None = None, force_refresh: bool = False) -> Dict[str, str | None]:
    """Открива ключовите таблици и колони за артикули и баркодове."""

    global _CATALOG_SCHEMA
    if _CATALOG_SCHEMA is not None and not force_refresh:
        return dict(_CATALOG_SCHEMA)

    active_cur = _require_cursor(cur=cur)
    columns_map = _collect_relation_columns(active_cur)
    if not columns_map:
        columns_map = _parse_schema_dump()
    schema = _detect_catalog_schema_from_map(columns_map)
    _CATALOG_SCHEMA = dict(schema)
    return schema


def _clean_string(value: Any) -> Optional[str]:
    if value in (None, ""):
        return None
    try:
        text = str(value).strip()
    except Exception:  # pragma: no cover - защитно
        return None
    return text or None


def _decimal_or_none(value: Any) -> Optional[Decimal]:
    if value in (None, ""):
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def _row_to_catalog_item(row: Sequence[Any], columns: Sequence[str]) -> Dict[str, Any]:
    mapping = {name.upper(): row[idx] for idx, name in enumerate(columns)}
    result: Dict[str, Any] = {
        "id": None,
        "code": None,
        "barcode": None,
        "name": None,
        "uom": None,
        "price": None,
        "vat": None,
    }
    if "ITEM_ID" in mapping:
        try:
            result["id"] = int(mapping["ITEM_ID"])
        except Exception:
            result["id"] = None
    if "ITEM_CODE" in mapping:
        result["code"] = _clean_string(mapping["ITEM_CODE"])
    if "ITEM_BARCODE" in mapping:
        result["barcode"] = _clean_string(mapping["ITEM_BARCODE"])
    if "ITEM_NAME" in mapping:
        result["name"] = _clean_string(mapping["ITEM_NAME"])
    if "ITEM_UOM" in mapping:
        result["uom"] = _clean_string(mapping["ITEM_UOM"])
    if "ITEM_PRICE" in mapping:
        result["price"] = _decimal_or_none(mapping["ITEM_PRICE"])
    if "ITEM_VAT" in mapping:
        vat_value = _decimal_or_none(mapping["ITEM_VAT"])
        result["vat"] = vat_value
    return result


def get_field_max_len(cur: Any, table: str, field: str) -> int:
    """Връща максималната дължина за дадено поле, използвайки кеш."""

    normalized_table = (table or "").strip()
    normalized_field = (field or "").strip()
    if not normalized_table or not normalized_field:
        return 255

    cache_key = (normalized_table.upper(), normalized_field.upper())
    cached = _FIELD_LENGTH_CACHE.get(cache_key)
    if cached is not None:
        return cached

    active_cur = _require_cursor(cur=cur)
    sql = (
        "SELECT COALESCE(f.rdb$character_length, f.rdb$field_length) "
        "FROM rdb$relation_fields rf "
        "JOIN rdb$fields f ON f.rdb$field_name = rf.rdb$field_source "
        "WHERE UPPER(rf.rdb$relation_name) = ? "
        "AND UPPER(rf.rdb$field_name) = ?"
    )
    try:
        active_cur.execute(sql, cache_key)
        row = active_cur.fetchone()
        if row and row[0]:
            length = int(row[0])
        else:
            length = 255
    except Exception:
        length = 255

    _FIELD_LENGTH_CACHE[cache_key] = length
    return length


def _catalog_select_clause(schema: Dict[str, str | None], include_barcode: bool = True) -> Tuple[str, List[str]]:
    parts: List[str] = []
    aliases: List[str] = []

    def _add(expr: str, alias: str) -> None:
        parts.append(f"{expr} AS {alias}")
        aliases.append(alias)

    def _expr(column: Optional[str], alias: str, table_alias: str = "M") -> None:
        if column:
            _add(f"{table_alias}.{column}", alias)
        else:
            _add("NULL", alias)

    _expr(schema.get("materials_id"), "ITEM_ID")
    _expr(schema.get("materials_code"), "ITEM_CODE")
    if include_barcode:
        if schema.get("barcode_col"):
            _expr(schema.get("barcode_col"), "ITEM_BARCODE", table_alias="B")
        else:
            _add("NULL", "ITEM_BARCODE")
    _expr(schema.get("materials_name"), "ITEM_NAME")
    _expr(schema.get("materials_uom"), "ITEM_UOM")
    _expr(schema.get("materials_price"), "ITEM_PRICE")
    _expr(schema.get("materials_vat"), "ITEM_VAT")
    return ", ".join(parts), aliases


def get_item_by_barcode(cur: Any, barcode: str) -> Optional[Dict[str, Any]]:
    """Търси артикул по баркод."""

    value = (barcode or "").strip()
    if not value:
        return None

    active_cur = _require_cursor(cur=cur)
    schema = detect_catalog_schema(active_cur)
    barcode_table = schema.get("barcode_table")
    barcode_col = schema.get("barcode_col")
    barcode_fk = schema.get("barcode_mat_fk")
    materials_table = schema.get("materials_table")
    materials_code = schema.get("materials_code")
    if not (barcode_table and barcode_col and barcode_fk and materials_table and materials_code):
        return None

    select_clause, aliases = _catalog_select_clause(schema, include_barcode=True)
    sql = (
        f"SELECT FIRST 1 {select_clause} "
        f"FROM {barcode_table} B "
        f"JOIN {materials_table} M ON B.{barcode_fk} = M.{materials_code} "
        f"WHERE TRIM(B.{barcode_col}) = TRIM(?)"
    )
    active_cur.execute(sql, (value,))
    row = active_cur.fetchone()
    if not row:
        return None
    description = [desc[0].strip().upper() for desc in active_cur.description]
    return _row_to_catalog_item(row, description or aliases)


def get_item_by_code(cur: Any, code: str) -> Optional[Dict[str, Any]]:
    """Търси артикул по вътрешен код."""

    value = (code or "").strip()
    if not value:
        return None

    active_cur = _require_cursor(cur=cur)
    schema = detect_catalog_schema(active_cur)
    materials_table = schema.get("materials_table")
    materials_code = schema.get("materials_code")
    materials_id = schema.get("materials_id")
    if not (materials_table and materials_code):
        return None

    select_clause, aliases = _catalog_select_clause(schema, include_barcode=bool(schema.get("barcode_table")))
    join_clause = ""
    if (
        schema.get("barcode_table")
        and schema.get("barcode_col")
        and schema.get("barcode_mat_fk")
        and materials_code
    ):
        join_clause = (
            f" LEFT JOIN {schema['barcode_table']} B ON B.{schema['barcode_mat_fk']} = M.{materials_code}"
        )

    sql = (
        f"SELECT FIRST 1 {select_clause} "
        f"FROM {materials_table} M"
        f"{join_clause} "
        f"WHERE UPPER(TRIM(M.{materials_code})) = UPPER(TRIM(?))"
    )
    active_cur.execute(sql, (value,))
    row = active_cur.fetchone()
    if not row:
        return None
    description = [desc[0].strip().upper() for desc in active_cur.description]
    return _row_to_catalog_item(row, description or aliases)


def get_items_by_name(cur: Any, name_query: str, limit: int = 5) -> List[Dict[str, Any]]:
    """Търси артикули по име чрез CONTAINING."""

    normalized = " ".join((name_query or "").split())
    if not normalized:
        return []

    active_cur = _require_cursor(cur=cur)
    schema = detect_catalog_schema(active_cur)
    materials_table = schema.get("materials_table")
    name_col = schema.get("materials_name")
    materials_code = schema.get("materials_code")
    if not (materials_table and name_col and materials_code):
        return []

    try:
        safe_limit = max(1, int(limit))
    except (TypeError, ValueError):
        safe_limit = 5

    max_len = max(1, int(get_field_max_len(active_cur, materials_table, name_col)))
    search_value = normalized[:max_len]

    select_clause, aliases = _catalog_select_clause(schema, include_barcode=False)
    barcode_expr = "NULL AS ITEM_BARCODE"
    if (
        schema.get("barcode_table")
        and schema.get("barcode_col")
        and schema.get("barcode_mat_fk")
    ):
        barcode_expr = (
            f"(SELECT FIRST 1 TRIM(B.{schema['barcode_col']}) "
            f"FROM {schema['barcode_table']} B "
            f"WHERE B.{schema['barcode_mat_fk']} = M.{materials_code}) AS ITEM_BARCODE"
        )

    final_select = barcode_expr
    final_aliases = ["ITEM_BARCODE"]
    if select_clause:
        final_select = f"{select_clause}, {barcode_expr}"
        final_aliases = list(aliases) + ["ITEM_BARCODE"]

    sql = (
        f"SELECT FIRST {safe_limit} {final_select} "
        f"FROM {materials_table} M "
        f"WHERE M.{name_col} CONTAINING ? "
        f"ORDER BY CHAR_LENGTH(TRIM(M.{name_col}))"
    )
    active_cur.execute(sql, (search_value,))
    rows = active_cur.fetchall() or []
    if not rows:
        return []
    description = [desc[0].strip().upper() for desc in active_cur.description]
    columns = description or final_aliases
    return [_row_to_catalog_item(row, columns) for row in rows]


def find_item_candidates_by_name(cur: Any, name: str, limit: int = 3) -> List[Dict[str, Any]]:
    """Обратна съвместимост – използва get_items_by_name."""

    return get_items_by_name(cur, name, limit=limit)


def _enrich_catalog_result(data: Dict[str, Any], match: str) -> Dict[str, Any]:
    payload = dict(data)
    payload["source"] = "db"
    payload["match"] = match
    return payload


def resolve_item(cur: Any, token: str, limit: Optional[int] = None) -> List[Dict[str, Any]]:
    """Резолвира артикул с приоритет: баркод → код → име."""

    normalized = " ".join((token or "").split())
    if not normalized:
        return []

    active_cur = _require_cursor(cur=cur)

    item = get_item_by_barcode(active_cur, normalized)
    if item:
        enriched = dict(item)
        enriched["match"] = "barcode"
        enriched.setdefault("source", "db")
        return [enriched]

    item = get_item_by_code(active_cur, normalized)
    if item:
        enriched = dict(item)
        enriched["match"] = "code"
        enriched.setdefault("source", "db")
        return [enriched]

    if limit is None:
        try:
            env_limit = int(os.getenv("MV_DB_NAME_LIKE_LIMIT", "5") or "5")
        except ValueError:
            env_limit = 5
        limit_value = env_limit
    else:
        limit_value = limit

    candidates = get_items_by_name(active_cur, normalized, limit=max(1, int(limit_value)))
    results: List[Dict[str, Any]] = []
    for candidate in candidates:
        enriched = dict(candidate)
        enriched["match"] = "name"
        enriched.setdefault("source", "db")
        results.append(enriched)
    return results


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
    charset = str(charset).upper()

    profile_label = str(
        profile.get("label")
        or profile.get("name")
        or profile.get("client")
        or profile.get("profile_name")
        or database
    )

    _log_info(
        "Свързване към база",
        profile=profile_label,
        host=host,
        port=port,
        database=database,
        driver=_FB_API,
        charset=charset,
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
    _log_info("Свързването е успешно", profile=profile_label, driver=_FB_API, charset=charset)
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
          AND (
            UPPER(p.rdb$procedure_name) LIKE '%LOGIN%'
            OR UPPER(p.rdb$procedure_name) LIKE '%USER%'
          )
        ORDER BY 1
        """
    )
    procs = cur.fetchall()
    table_candidates_cache: Optional[List[Dict[str, Any]]] = None
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
            if table_candidates_cache is None:
                table_candidates_cache = _collect_table_login_candidates()
            fallback_meta = _prepare_table_meta(table_candidates_cache or [])
            if fallback_meta:
                meta["fallback_table"] = fallback_meta
            _log_info(
                "Открита login процедура",
                procedure=name,
                sp_kind=sp_kind,
            )
            return meta

    if table_candidates_cache is None:
        table_candidates_cache = _collect_table_login_candidates()
    table_meta = _prepare_table_meta(table_candidates_cache or [])
    if table_meta:
        has_name = "да" if table_meta["fields"].get("has_name") else "не"
        has_pass = "да" if table_meta["fields"].get("has_pass") else "не"
        _log_info(
            "Открит login чрез таблица",
            table=table_meta["name"],
            has_login=has_name,
            has_pass=has_pass,
        )
        return table_meta

    fallback_meta = {
        "mode": "table",
        "name": "USERS",
        "sp_kind": None,
        "fields": {
            "id": "ID",
            "login": "NAME",
            "password": "PASS",
            "has_name": True,
            "has_pass": True,
        },
    }
    _log_warning(
        "Не открих специфична login таблица – използвам USERS по подразбиране.",
        profile=profile_label,
    )
    return fallback_meta


def login_user(username: str, password: str) -> Tuple[int, str]:
    """Връща (operator_id, operator_login) или вдига MistralDBError."""

    global _LOGIN_META
    cur = _require_cursor()
    username = username or ""
    password = password or ""
    _last_login_trace.clear()
    display_user = username or "<само парола>"
    _trace("start", profile=_profile_label(), username=display_user)
    _log_info("Старт на логин", profile=_profile_label(), username=display_user)

    force_table = os.getenv("MV_FORCE_TABLE_LOGIN", "").strip() == "1"
    if force_table:
        _trace("force_table_login", profile=_profile_label())
        _log_warning("Активиран е принудителен табличен логин.", profile=_profile_label())
        if _LOGIN_META is None:
            _LOGIN_META = detect_login_method(cur)
        meta = _LOGIN_META or {}
        table_meta = _table_meta_from_login_meta(meta)
        _trace(
            "detected_mode",
            mode=meta.get("mode"),
            name=meta.get("name"),
            sp_kind=meta.get("sp_kind"),
        )
        _log_info(
            "Открит механизъм за логин",
            profile=_profile_label(),
            mode=meta.get("mode"),
            name=meta.get("name"),
        )
        operator_id, operator_login = _login_via_users_table(cur, username, password, table_meta)
        _trace(
            "success",
            mode="table",
            operator_id=operator_id,
            operator_login=operator_login,
        )
        match_mode = "username" if username.strip() else "password"
        _log_info(
            "Успешен вход чрез таблица (принудително)",
            profile=_profile_label(),
            username=display_user,
            match=match_mode,
            operator_id=operator_id,
        )
        return operator_id, operator_login

    if _LOGIN_META is None:
        _LOGIN_META = detect_login_method(cur)
    meta = _LOGIN_META or {}
    table_meta = _table_meta_from_login_meta(meta)
    _trace(
        "detected_mode",
        mode=meta.get("mode"),
        name=meta.get("name"),
        sp_kind=meta.get("sp_kind"),
    )
    _log_info(
        "Открит механизъм за логин",
        profile=_profile_label(),
        mode=meta.get("mode"),
        name=meta.get("name"),
    )

    try:
        if meta.get("mode") == "sp":
            sp_result = _login_via_procedure(cur, meta, username, password)
            if sp_result is not None:
                operator_id, operator_login = sp_result
                _trace(
                    "sp_ok",
                    procedure=meta.get("name"),
                    operator_id=operator_id,
                    operator_login=operator_login,
                )
                _trace(
                    "success",
                    mode="sp",
                    operator_id=operator_id,
                    operator_login=operator_login,
                )
                _log_info(
                    "Успешен вход чрез процедура",
                    profile=_profile_label(),
                    procedure=meta.get("name"),
                    username=display_user,
                )
                return operator_id, operator_login
            _trace(
                "procedure_fallback_table",
                procedure=meta.get("name"),
                table="USERS",
            )
            _log_warning(
                "Процедурата не върна резултат – преминаваме към табличен логин",
                profile=_profile_label(),
                procedure=meta.get("name"),
            )
        operator_id, operator_login = _login_via_users_table(cur, username, password, table_meta)
        _trace(
            "success",
            mode="table",
            operator_id=operator_id,
            operator_login=operator_login,
        )
        match_mode = "username" if username.strip() else "password"
        _log_info(
            "Успешен вход чрез таблица",
            profile=_profile_label(),
            username=display_user,
            match=match_mode,
            operator_id=operator_id,
        )
        return operator_id, operator_login
    except MistralDBError as exc:
        _trace("error", message=str(exc))
        _log_warning(
            "Неуспешен вход",
            profile=_profile_label(),
            username=display_user,
            error=str(exc),
        )
        raise


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
    row: Sequence[Any],
    outputs: List[Dict[str, Any]],
    username: str,
    description: Optional[Sequence[Sequence[Any]]] = None,
) -> Optional[Tuple[int, str]]:
    operator_id: Optional[int] = None
    operator_login: Optional[str] = None

    def _coerce_int(value: Any) -> Optional[int]:
        if value is None:
            return None
        if isinstance(value, bool):
            return int(value)
        if isinstance(value, (int,)):
            return int(value)
        if isinstance(value, float):
            try:
                return int(value)
            except (TypeError, ValueError):
                return None
        try:
            text = str(value).strip()
        except Exception:
            return None
        if not text:
            return None
        if text.isdigit():
            try:
                return int(text)
            except ValueError:
                return None
        return None

    names_from_description: List[str] = []
    if description:
        for col in description:
            if not col:
                continue
            raw_name = col[0] if isinstance(col, (list, tuple)) else None
            if raw_name:
                names_from_description.append(str(raw_name).strip().upper())

    tokens_for_id = ("OP", "USER", "OPER", "ID")
    tokens_for_login = ("LOGIN", "USER", "NAME", "CODE")

    for idx, value in enumerate(row):
        name_candidate = ""
        if idx < len(outputs):
            name_candidate = str(outputs[idx].get("name") or "").strip().upper()
        if not name_candidate and idx < len(names_from_description):
            name_candidate = names_from_description[idx]

        coerced = _coerce_int(value)
        if operator_id is None and name_candidate:
            if any(token in name_candidate for token in tokens_for_id) and coerced is not None:
                operator_id = coerced
        if operator_login is None and name_candidate:
            if any(token in name_candidate for token in tokens_for_login):
                try:
                    operator_login = str(value).strip()
                except Exception:
                    operator_login = None

    if operator_id is None and row:
        operator_id = _coerce_int(row[0])

    if operator_login is None:
        if len(row) > 1 and row[1] not in (None, ""):
            operator_login = str(row[1]).strip()
        elif operator_id is not None:
            operator_login = str(operator_id)
        else:
            operator_login = username or ""

    if operator_id is None:
        return None

    return operator_id, operator_login or (username or str(operator_id))


def _login_via_procedure(
    cur: Any, meta: Dict[str, Any], username: str, password: str
) -> Optional[Tuple[int, str]]:
    name = meta.get("name")
    if not name:
        return None

    inputs = meta.get("fields", {}).get("inputs", [])
    outputs = meta.get("fields", {}).get("outputs", [])
    args = _build_procedure_args(inputs, username, password)
    placeholders = ", ".join(["?"] * len(inputs))
    sp_kind = (meta.get("sp_kind") or "executable").lower()

    params_payload = {"username": username or "<празно>", "password": "***" if password else ""}

    if sp_kind == "selectable":
        sql = f"SELECT * FROM {name}({placeholders})" if placeholders else f"SELECT * FROM {name}"
        _trace("sp_select", procedure=name, sql=sql, params=params_payload)
        _log_info("Login чрез процедура (SELECT)", procedure=name)
        try:
            cur.execute(sql, args)
            description = getattr(cur, "description", None)
            rows = cur.fetchall()
        except _FB_ERROR as exc:
            if _is_no_result_set_error(exc):
                _trace("sp_no_result", procedure=name, mode="select")
                sp_kind = "executable"
                _log_warning("Процедурата не връща резултат при SELECT.", procedure=name)
            else:
                _trace("sp_error", procedure=name, mode="select", error=str(exc))
                raise MistralDBError(f"Грешка при изпълнение на {name}: {exc}") from exc
        else:
            if rows:
                row = rows[0]
                result = _extract_operator_from_row(row, outputs, username, description)
                if result is not None:
                    operator_id, operator_login = result
                    return operator_id, operator_login
                _trace("sp_missing_identifier", procedure=name, mode="select")
                _log_warning(
                    "Процедурата не върна идентификатор – преминаваме към таблица",
                    procedure=name,
                )
            _trace("sp_no_result", procedure=name, mode="select")

    exec_sql = (
        f"EXECUTE PROCEDURE {name} {placeholders}" if placeholders else f"EXECUTE PROCEDURE {name}"
    )
    _trace("sp_execute", procedure=name, sql=exec_sql, params=params_payload)
    _log_info("Login чрез процедура (EXECUTE)", procedure=name)
    row: Optional[Sequence[Any]] = None
    try:
        cur.execute(exec_sql, args)
        description = getattr(cur, "description", None)
        try:
            row = cur.fetchone()
        except _FB_ERROR as exc:
            if _is_no_result_set_error(exc):
                row = None
            else:
                _trace("sp_error", procedure=name, mode="execute", error=str(exc))
                raise MistralDBError(f"Грешка при изпълнение на {name}: {exc}") from exc
    except _FB_ERROR as exc:
        _trace("sp_error", procedure=name, mode="execute", error=str(exc))
        raise MistralDBError(f"Грешка при изпълнение на {name}: {exc}") from exc

    if not row:
        _trace("sp_no_result", procedure=name, mode="execute")
        return None

    result = _extract_operator_from_row(row, outputs, username, description)
    if result is None:
        _trace("sp_missing_identifier", procedure=name, mode="execute")
        _log_warning(
            "Процедурата не върна идентификатор – преминаваме към таблица",
            procedure=name,
        )
        return None

    operator_id, operator_login = result
    return operator_id, operator_login


def _login_via_users_table(
    cur: Any,
    username: str,
    password: str,
    meta: Optional[Dict[str, Any]] = None,
) -> Tuple[int, str]:
    table = (meta or {}).get("name") or "USERS"
    fields = (meta or {}).get("fields") or {}
    id_field = fields.get("id") or "ID"
    login_field = fields.get("login") or "NAME"
    pass_field = fields.get("password")
    hash_field = fields.get("password_hash")

    username_clean = username.strip()
    password_value = password
    mode = "username" if username_clean else "password"

    if not pass_field and hash_field and not password_value:
        raise MistralDBError("Не е въведена парола.")

    if not pass_field and hash_field:
        _trace(
            "table_hash_detected",
            table=table,
            hash_column=hash_field,
        )
        _log_warning(
            "Налична е колона за хеширани пароли – TODO реализация",
            table=table,
            hash_column=hash_field,
        )
        raise MistralDBError(
            "Този профил използва хеширани пароли – свържете се с поддръжка."
        )

    effective_pass_field = pass_field or hash_field or "PASS"

    clauses = [f"TRIM({effective_pass_field}) = TRIM(?)"]
    params: List[Any] = [password_value.strip() if isinstance(password_value, str) else password_value]
    if username_clean:
        clauses.insert(0, f"UPPER(TRIM({login_field})) = UPPER(TRIM(?))")
        params.insert(0, username_clean)

    query_sql = (
        f"SELECT {id_field}, {login_field}, {effective_pass_field} FROM {table} WHERE "
        + " AND ".join(clauses)
    )
    debug_params = (
        {"username": username_clean, "password": "***"}
        if username_clean
        else {"password": "***"}
    )
    _trace(
        "table_lookup",
        table=table,
        mode=mode,
        username=username_clean or None,
        password=password_value,
        sql=query_sql,
    )
    _log_debug(
        f"Табличен логин SQL: {query_sql} | params={debug_params}",
        table=table,
        mode=mode,
    )

    try:
        cur.execute(query_sql, tuple(params))
        rows = cur.fetchall()
    except _FB_ERROR as exc:
        _trace("table_error", table=table, error=str(exc))
        raise MistralDBError(f"Грешка при четене от {table}: {exc}") from exc

    if not rows:
        _trace("table_no_match", table=table, mode=mode, username=username_clean or None)
        raise MistralDBError("Невалидни данни за вход.")

    if mode == "password" and len(rows) > 1:
        _trace(
            "table_ambiguous",
            table=table,
            matches=len(rows),
        )
        raise MistralDBError(
            "Паролата съответства на повече от един оператор. Моля, въведете и потребителско име."
        )

    row = rows[0]
    stored_pass = "" if row[2] is None else str(row[2]).strip()
    password_comp = password_value.strip() if isinstance(password_value, str) else password_value
    if username_clean and stored_pass != (password_comp if password_comp is not None else ""):
        _trace(
            "table_no_match",
            table=table,
            mode=mode,
            username=username_clean,
            reason="password-mismatch",
        )
        raise MistralDBError("Невалидни данни за вход.")

    operator_id = int(row[0])
    operator_login_raw = row[1] if len(row) > 1 else None
    operator_login = (str(operator_login_raw or "").strip()) or (username_clean or str(operator_id))
    _trace(
        "table_ok",
        table=table,
        mode=mode,
        operator_id=operator_id,
        operator_login=operator_login,
    )
    return operator_id, operator_login


def get_item_info(code_or_name: str) -> Optional[Dict[str, Any]]:
    """Търсене на материал по код/баркод/име."""
    cur = _require_cursor()
    value = code_or_name.strip()
    if not value:
        return None

    candidates = resolve_item(cur, value, limit=1)
    if candidates:
        return candidates[0]
    return None


def db_find_by_barcode(cur: Any, barcode: str) -> List[Dict[str, Any]]:
    """Връща списък кандидати по точен баркод."""

    normalized = (barcode or "").strip()
    if not normalized:
        return []

    candidates = resolve_item(cur, normalized, limit=1)
    matches = [candidate for candidate in candidates if candidate.get("match") == "barcode"]
    if not matches:
        return []
    logger.info("DB resolve: barcode match → %s кандидата за %s", len(matches), normalized)
    return matches


def db_find_by_code(cur: Any, code: str) -> List[Dict[str, Any]]:
    """Точно съвпадение по вътрешен код/артикулен номер."""

    normalized = (code or "").strip()
    if not normalized:
        return []

    candidates = resolve_item(cur, normalized, limit=1)
    matches = [candidate for candidate in candidates if candidate.get("match") == "code"]
    if not matches:
        return []
    logger.info("DB resolve: code match → %s кандидата за %s", len(matches), normalized)
    return matches


def _escape_like(value: str) -> str:
    return re.sub(r"([%_\\])", r"\\\\\1", value)


def db_find_by_name_like(cur: Any, name: str, limit: int = 5) -> List[Dict[str, Any]]:
    """LIKE търсене по име (case-insensitive)."""

    normalized = " ".join((name or "").split())
    if not normalized:
        return []

    candidates = get_items_by_name(cur, normalized, limit=limit)
    results = [
        dict(candidate, match="name", source=candidate.get("source", "db"))
        for candidate in candidates
    ]
    logger.info(
        "DB resolve: name LIKE → %s кандидата за %s",
        len(results),
        normalized,
    )
    return results


def db_resolve_item(cur: Any, token: str) -> List[Dict[str, Any]]:
    """Централен резолвер за артикули."""

    normalized = " ".join((token or "").split())
    if not normalized:
        logger.debug("DB resolve: празен токен")
        return []

    active_cur = _require_cursor(cur=cur)
    candidates = resolve_item(active_cur, normalized)
    if candidates:
        logger.info(
            "DB resolve: намерени са %s кандидата за %s",
            len(candidates),
            normalized,
        )
        return candidates

    logger.info("DB resolve: no match за '%s'", normalized)
    return []


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
    if os.getenv("MV_ENABLE_OPEN_DELIVERY", "").strip() == "1":
        try:
            with _transaction():
                conn.cursor().execute(sql, [values[col] for col in column_names])
        except _FB_ERROR as exc:
            raise MistralDBError(f"Неуспешно създаване на OPEN доставка: {exc}") from exc
    else:
        _log_info(
            "OPEN доставка не е записана (скелет режим)",
            table=header_table,
            delivery_id=delivery_id,
        )

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

    if os.getenv("MV_ENABLE_OPEN_DELIVERY", "").strip() != "1":
        _log_info(
            "Артикулите не са записани (скелет режим)",
            table=detail_table,
            items=len(items),
        )
        return

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
