# Micro Vision – DEV Guide

Това е вътрешното ръководство за разработчици. Описва стартиране, конфигурация, структура, база (Mistral/Firebird), тестови файлове и билд.

## 1) Структура (важните папки/файлове)
- `microvision_gui.py` – Tkinter GUI
- `db_integration.py` – слой между GUI и DB (сесия, делегиране към mistral_db)
- `mistral_db.py` – Firebird достъп (Mistral 2.5.6)
- `diag_mistral_auth.py` – диагностика на login механизма (SP vs таблица)
- `extract_and_prepare.py` – PDF/IMG парсинг, OCR, TXT експорт
- `mistral_clients.json` – профили на бази
- `license.json` – данни за лиценз
- `schema_TESTBARBERSHOP.sql` – **DDL схема** от Firebird (генерирана с `isql -x`)
- `samples/invoices/` – **примерни входни файлове** (PDF/JPG) + примерни TXT експорти

## 2) Бърз старт (DEV)
```powershell
python -m venv .venv
.\.venv\Scripts\activate
pip install -U pip wheel
pip install -r requirements.txt
python microvision_gui.py

Задължителни библиотеки (виж `requirements.txt`): `fdb`, `PyPDF2`, `pdf2image`, `Pillow`, `pytesseract`, `loguru` и др. Може да ползвате `firebird-driver` вместо `fdb`, но тогава осигурете `fbclient.dll` (Firebird 3 client) в PATH или до изпълнимия файл.

Инсталирайте Tesseract OCR (x64) и добавете пътя към `tesseract.exe` в PATH.

За кирилица инсталирайте езиковия пакет `bul`.

3) Конфигурация на бази (Mistral/Firebird)
mistral_clients.json:

json
Copy code
{
  "office": {
    "host": "127.0.0.1",
    "port": 3050,
    "database": "C:/MISTRAL/DATA/OFFICE.FDB",
    "user": "SYSDBA",
    "password": "masterkey",
    "charset": "WIN1251"
  }
}
4) Диагностика на логин (важно)

```
python diag_mistral_auth.py --profile "Local TEST" --user operator --password secret
```

Скриптът автоматично описва открития механизъм (процедура или таблица, включително параметри/типове/колони) и прави тестов вход. При неуспешна автентикация показва пълната хронология от стъпките на логина. Метаданните идват от `RDB$RELATIONS`, `RDB$PROCEDURES`, `RDB$PROCEDURE_PARAMETERS`, `RDB$FIELDS`.

5) Обработка на документи
Поддържани: PDF/JPG/JPEG/PNG/TIFF/BMP.

PDF с текст → PyPDF2 (без OCR).

Сканиран PDF / снимка → pdf2image @ ~300 DPI → Tesseract OCR (bul+eng, OEM=3, PSM=6).

Предобработка при изображения: EXIF auto-rotate, лека deskew, контраст/threshold.

Примерни файлове: в samples/invoices/.

6) DB режим (OPEN доставка)
В GUI има checkbox „DB режим (отворена доставка)“.

При включен режим:

mistral_db.create_open_delivery(operator_id) – създава OPEN доставка.

mistral_db.push_items_to_mistral(delivery_id, items) – транзакционно записва редовете.

Винаги остава и опцията за TXT експорт.

По подразбиране модулът работи в **скелетен режим** и само логва SQL заявките, без да изпълнява `INSERT`. За реално записване в
базата задайте променливата на средата `MV_ENABLE_OPEN_DELIVERY=1` преди стартиране на приложението.

### 6а) DB Resolver & каталожна схема
- `mistral_db.detect_catalog_schema()` чете метаданните от `RDB$RELATIONS`/`RDB$RELATION_FIELDS` и намира таблици/колони за материали и баркодове. Ако достъпът е ограничен, пада обратно към `schema_TESTBARBERSHOP.sql` (локален dump от isql).
- Resolver API:
  - `get_item_by_barcode(cur, barcode)`
  - `get_item_by_code(cur, code)`
  - `find_item_candidates_by_name(cur, name, limit=3)`
  - Всички връщат `{id, code, barcode, name, uom, price, vat}` (`Decimal` за числата, `None` при липса).
- `db_integration.push_parsed_rows()` използва Resolver-а (barcode → code → name). При 2–3 кандидата показва малък Tk диалог; нерешените редове остават за TXT експорт. След push се логва обобщение (общо/успешни/нерешени/ръчни избори) в `logs/app_*.log`.
- `collect_db_diagnostics()` също ползва schema detection и показва първи примерни заявки. За ръчно дебъгване: проверете `RDB$RELATIONS`, `RDB$RELATION_FIELDS`, `RDB$PROCEDURES` през isql/FlameRobin.
- Връзката към Firebird винаги е с `lc_ctype='WIN1251'`. В Python **не** се правят ръчни `encode/decode`; логовете са UTF-8. GUI диагностика е в моношрифт (`TkFixedFont`), за да няма „����“.

7) Лиценз
Статусът се чете директно от `license.json` (`valid_until`). Ако файлът липсва → „Лиценз: проверка недостъпна“, ако датата е минала → „Лиценз: изтекъл“.

Логовете се записват в `logs/app_YYYYMMDD.log` (ротация 5 MB, пази последните 7 файла). За детайлен лог включете `MICROVISION_LOG_LEVEL=DEBUG` преди стартиране.

8) Билд (Windows EXE – onefile)
powershell
Copy code
pip install pyinstaller
pyinstaller --onefile --name MicroVisionInvoiceParser microvision_gui.spec
При първо пускане, ако липсва mistral_clients.json, приложението създава примерен и предлага избор/редакция на профил.

9) Troubleshooting
fbclient.dll липсва → инсталирай Firebird client; добави в PATH или до exe.

Tesseract not found → добави пътя към tesseract.exe в PATH.

Невалиден потребител → пусни diag_mistral_auth.py и провери дали е SP или таблица.

Грешна кирилица в SQL → увери се, че schema_TESTBARBERSHOP.sql е генериран с -ch UTF8 и записан UTF-8.

10) Генериране на DDL схема (как е направено)
powershell
Copy code
& "C:\Program Files\Firebird\Firebird_2_5\bin\isql.exe" -user SYSDBA -password masterkey -ch UTF8 -x "D:\base\TESTBARBERSHOP.FDB" |
    Out-File -FilePath "D:\base\schema_TESTBARBERSHOP.sql" -Encoding UTF8