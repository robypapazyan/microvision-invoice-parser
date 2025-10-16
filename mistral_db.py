# mistral_db.py
from __future__ import annotations
from dataclasses import dataclass
from typing import Optional, Dict, Any, List, Tuple
import hashlib

# Firebird 2.5.x -> fdb
import fdb  # pip install fdb


@dataclass
class DBConfig:
    database: str
    host: str = "localhost"
    port: int = 3050
    user: str = "SYSDBA"
    password: str = "masterkey"
    charset: str = "WIN1251"


class MistralDB:
    """
    Тънък wrapper около fdb с помощни методи за:
      - авто-откриване на схеми за login/пароли
      - логин по login+password и по password-only
    """
    def __init__(self, conf: DBConfig, auth_profile: Optional[dict] = None) -> None:
        self.conf = conf
        self.auth = auth_profile or {}
        self._conn: Optional[fdb.Connection] = None

    # ---------- low-level ----------
    def connect(self) -> fdb.Connection:
        if self._conn is None:
            self._conn = fdb.connect(
                host=self.conf.host,
                port=self.conf.port,
                database=self.conf.database,
                user=self.conf.user,
                password=self.conf.password,
                charset=self.conf.charset,
            )
        return self._conn

    def cursor(self) -> fdb.Cursor:
        return self.connect().cursor()

    # ---------- помощни за auth ----------
    @staticmethod
    def _hash_try(plain: str, salt: Optional[str], algo: str) -> str:
        data = (plain if salt is None else (plain + salt)).encode("utf-8")
        if algo == "PLAIN":
            return plain
        if algo == "MD5":
            return hashlib.md5(data).hexdigest()
        if algo == "SHA1":
            return hashlib.sha1(data).hexdigest()
        if algo == "SHA256":
            return hashlib.sha256(data).hexdigest()
        raise ValueError(f"Unknown hash algo: {algo}")

    def _collect_auth_schema(self) -> dict:
        """Взема дефолти от auth-профила, с разумни резерви."""
        a = self.auth or {}
        return {
            "use_procedures": bool(a.get("use_procedures", False)),
            "table_candidates": a.get(
                "table_candidates",
                ["USERS", "OPERATORS", "POTREBITELI", "OPERATORI", "USERI", "PERSONS"],
            ),
            "login_cols": a.get(
                "login_cols",
                ["CODE", "KOD", "LOGIN", "USERNAME", "USER_NAME", "NAME", "OPERATOR"],
            ),
            "password_cols": a.get(
                "password_cols",
                ["PASS", "PASSWORD", "PAROLA", "PASS_HASH", "PAROLA_HASH", "PWD"],
            ),
            "salt_cols": a.get("salt_cols", ["SALT"]),
            "hash_order": a.get("hash_order", ["MD5", "SHA1", "SHA256", "PLAIN"]),
            "id_cols": a.get("id_cols", ["ID", "USER_ID", "OP_ID"]),
        }

    def _table_has_cols(self, table: str, cols: List[str]) -> List[str]:
        cur = self.cursor()
        cur.execute(
            """
            SELECT TRIM(rf.rdb$field_name)
            FROM rdb$relation_fields rf
            WHERE rf.rdb$relation_name = ?
              AND COALESCE(rf.rdb$system_flag, 0) = 0
            """,
            (table.upper(),),
        )
        present = {r[0].upper() for r in cur.fetchall()}
        return [c for c in cols if c.upper() in present]

    # ---------- публични методи ----------
    def authenticate_operator(self, login: str, password: str) -> Optional[int]:
        """
        Логин по login + password. Връща user_id или None.
        """
        schema = self._collect_auth_schema()
        cur = self.cursor()

        for table in schema["table_candidates"]:
            ids = self._table_has_cols(table, schema["id_cols"])
            logins = self._table_has_cols(table, schema["login_cols"])
            passes = self._table_has_cols(table, schema["password_cols"])
            salts = self._table_has_cols(table, schema["salt_cols"])
            if not ids or not logins or not passes:
                continue

            # Извличаме първия валиден login-колон; първия id-колон и всички pass/solt колони
            id_col = ids[0]
            login_col = logins[0]
            sel_cols = [id_col] + passes + salts

            cur.execute(
                f"SELECT {', '.join(sel_cols)} FROM {table} WHERE {login_col} = ?",
                (login,),
            )
            row = cur.fetchone()
            if not row:
                continue

            user_id = row[0]
            pass_vals = list(row[1 : 1 + len(passes)])
            salt_vals = list(row[1 + len(passes) :])

            # Пробваме всички хешове и всички соли
            for pv in pass_vals:
                if pv is None:
                    continue
                pv_str = str(pv).strip().lower()
                for algo in schema["hash_order"]:
                    for salt in ([None] + salt_vals):
                        cand = self._hash_try(password, None if salt in (None, "") else str(salt), algo)
                        if algo == "PLAIN":
                            if password.strip().lower() == pv_str:
                                return int(user_id)
                        else:
                            if cand == pv_str:
                                return int(user_id)

        return None

    def authenticate_operator_password_only(self, password: str) -> Optional[int]:
        """
        Само по парола. Обхождаме всички кандидат-таблици/колони и връщаме
        user_id само ако има ТОЧНО 1 съвпадение. Иначе -> None.
        """
        schema = self._collect_auth_schema()
        cur = self.cursor()
        matches: List[int] = []

        for table in schema["table_candidates"]:
            ids = self._table_has_cols(table, schema["id_cols"])
            passes = self._table_has_cols(table, schema["password_cols"])
            salts = self._table_has_cols(table, schema["salt_cols"])
            if not ids or not passes:
                continue
            id_col = ids[0]
            sel_cols = [id_col] + passes + salts

            cur.execute(f"SELECT {', '.join(sel_cols)} FROM {table}")
            for row in cur.fetchall():
                user_id = row[0]
                pass_vals = list(row[1 : 1 + len(passes)])
                salt_vals = list(row[1 + len(passes) :])

                for pv in pass_vals:
                    if pv is None:
                        continue
                    pv_str = str(pv).strip().lower()
                    for algo in schema["hash_order"]:
                        for salt in ([None] + salt_vals):
                            cand = self._hash_try(password, None if salt in (None, "") else str(salt), algo)
                            if algo == "PLAIN":
                                if password.strip().lower() == pv_str:
                                    matches.append(int(user_id))
                            else:
                                if cand == pv_str:
                                    matches.append(int(user_id))

        uniq = sorted(set(matches))
        if len(uniq) == 1:
            return uniq[0]
        return None
