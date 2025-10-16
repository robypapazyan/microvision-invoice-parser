# db_integration.py
from __future__ import annotations
from typing import Optional, Tuple, Dict, Any
import json

from mistral_db import MistralDB, DBConfig


def _make_db(profile: Dict[str, Any]) -> MistralDB:
    conf = DBConfig(
        database=profile["database"],
        host=profile.get("host", "localhost"),
        port=int(profile.get("port", 3050)),
        user=profile.get("user", "SYSDBA"),
        password=profile.get("password", "masterkey"),
        charset=profile.get("charset", "WIN1251"),
    )
    return MistralDB(conf, profile.get("auth", {}))


# --- AUTH bridge (GUI -> MistralDB) ---
from mistral_db import DBConfig, MistralDB

def operator_login_session(profile: dict, login: str | None, password: str) -> int | None:
    """
    Връща user_id при успех, иначе None.
    - ако login е None/празно -> логин само с парола (password-only)
    - иначе -> логин с login + парола
    """
    # Профилните ключове са по твоя JSON (mistral_clients.json)
    conf = DBConfig(
        database = profile.get("database"),
        host     = profile.get("host", "localhost"),
        port     = int(profile.get("port", 3050)),
        user     = profile.get("user", "SYSDBA"),
        password = profile.get("password", "masterkey"),
        charset  = profile.get("charset", "WIN1251"),
    )

    db = MistralDB(conf)

    # Нормализирай login: празен стринг -> None
    if login is not None and str(login).strip() == "":
        login = None

    if login is None:
        # password-only (изисква уникален мач)
        return db.authenticate_operator_password_only(password)
    else:
        # класически login + password
        return db.authenticate_operator(login, password)
