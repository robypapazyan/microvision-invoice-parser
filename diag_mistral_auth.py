# diag_mistral_auth.py
import json, fdb, os, sys

JSON = "mistral_clients.json"

PASS_COLS = {"PASS_HASH","PASSWORD","PASS","PWD","PAROLA","PAROLA_HASH"}
LOGIN_COLS = {"LOGIN","USERNAME","USER_NAME","OPERATOR","NAME","KOD","CODE"}
ID_COLS = {"ID","USER_ID","OP_ID","KOD","CODE"}

PROC_HINTS = ("LOGIN","VHOD","AUTH")  # ще хванем SP_LOGIN, SP_VHOD, AUTH_LOGIN и т.н.

def main():
    with open(JSON,"r",encoding="utf-8") as f:
        profile = json.load(f)[0]
    dsn = f"{profile.get('host','localhost')}/{profile.get('port',3050)}:{profile['database']}"
    con = fdb.connect(
        dsn=dsn,
        user=profile.get("user","SYSDBA"),
        password=profile.get("password","masterkey"),
        charset=profile.get("charset","WIN1251")
    )
    cur = con.cursor()
    print("=== TABLES ===")
    cur.execute("""
        SELECT TRIM(rdb$relation_name)
        FROM rdb$relations
        WHERE rdb$view_blr IS NULL AND COALESCE(rdb$system_flag,0)=0
        ORDER BY 1
    """)
    tables = [r[0] for r in cur.fetchall()]
    print(f"Total user tables: {len(tables)}")
    print()

    def columns(t):
        c = con.cursor()
        try:
            c.execute("""
              SELECT TRIM(rf.rdb$field_name)
              FROM rdb$relation_fields rf
              WHERE rf.rdb$relation_name=?
              ORDER BY rf.rdb$field_position
            """,(t,))
            return [x[0] for x in c.fetchall()]
        finally:
            c.close()

    print("=== CANDIDATE USER TABLES ===")
    candidates = []
    for t in tables:
        cols = set(columns(t))
        if (cols & PASS_COLS) and (cols & ID_COLS):
            candidates.append((t, cols))
            print(f"- {t}:")
            print("   id:", list(cols & ID_COLS))
            print("   login:", list(cols & LOGIN_COLS))
            print("   pass:", list(cols & PASS_COLS))
    if not candidates:
        print("(!) Не намерих таблица с парола + id. Вероятно се ползва stored procedure.")

    print("\n=== PROCEDURES (login-like) ===")
    c2 = con.cursor()
    c2.execute("SELECT TRIM(rdb$procedure_name) FROM rdb$procedures ORDER BY 1")
    procs = [r[0] for r in c2.fetchall()]
    like = [p for p in procs if any(h in p for h in PROC_HINTS)]
    if like:
        for p in like:
            print("-", p)
    else:
        print("(none)")

    con.close()

if __name__ == "__main__":
    main()
