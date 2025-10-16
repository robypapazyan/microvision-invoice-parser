
WHAT'S NEW (Direct Mistral DB + Mandatory Operator Login)
=========================================================
- mistral_db.py: Firebird connector, schema discovery, operator authentication (SPs or table+hash), create OPEN delivery.
- db_integration.py: operator_login_session() and push_items_to_mistral().
- export_txt_to_db.py: helper to parse existing TXT export and pass items to push_items_to_mistral().
- mistral_clients.json: sample profile for local/remote DB (technical Firebird credentials only).

Typical flow in GUI:
1) On app start: show Login dialog (choose profile, enter operator login+password).
   -> operator_login_session(profile, login, password) returns user_id.
2) After parsing invoice:
   -> push_items_to_mistral(export_items, profile, operator_user_id, operator_login=login)

Remote DB: Set host/port/path in mistral_clients.json. Prefer VPN.
Security: Use restricted Firebird account; do NOT save operator passwords.
