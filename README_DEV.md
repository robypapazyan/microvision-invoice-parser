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
. .venv\Scripts\Activate
pip install -r requirements.txt
python microvision_gui.py
Зависимости (минимум): fdb или firebird-driver, PyPDF2, pdf2image, pytesseract, Pillow, opencv-python, loguru, tk.

Инсталирай Tesseract OCR (x64) и добави пътя към tesseract.exe в PATH.

За кирилица инсталирай езиковия пакет bul.

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
powershell
Copy code
python diag_mistral_auth.py
Скриптът извежда:

има ли SP за логин (напр. SP_LOGIN), вход/изход параметри;

ако не – търси табличен механизъм (USERS и колони LOGIN, PASS/PASS_HASH/PASSWORD);

източник на метаданни: RDB$RELATIONS, RDB$PROCEDURES, RDB$PROCEDURE_PARAMETERS, RDB$FIELDS.

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

7) Лиценз
Показва „Лиценз: оставащи X дни“ (от license_utils.validate_license()).

Logging: кратко към потребителя, детайлно в logs/app_YYYYMMDD.log.

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