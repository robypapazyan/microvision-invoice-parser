# -*- coding: utf-8 -*-
import os
import sys
import json
import re
import difflib

try:
    import pandas as pd

    PANDAS_AVAILABLE = True
except ImportError:
    pd = None  # type: ignore[assignment]
    PANDAS_AVAILABLE = False
    print("WARN: pandas липсва – продължаваме без таблици от materials.csv.")

def prompt_user(question, valid_answers=None, gui_mode=False):
    if gui_mode:
        import tkinter.simpledialog as simpledialog
        import tkinter.messagebox as messagebox
        import tkinter as tk
        root = tk.Tk()
        root.withdraw()

        while True:
            answer = simpledialog.askstring("Въпрос", question)
            if answer is None:
                return "cancel"
            answer = answer.strip().lower()
            if valid_answers is None or answer in valid_answers:
                return answer
            else:
                messagebox.showwarning("Невалиден отговор", f"Моля, отговори с: {', '.join(valid_answers)}")
    else:
        while True:
            answer = input(question + " ").strip().lower()
            if valid_answers is None or answer in valid_answers:
                return answer
            else:
                print(f"Невалиден отговор. Моля, въведи едно от: {', '.join(valid_answers)}")

# === ФУНКЦИИ ЗА ОБРАБОТКА НА MAPPING ===
def normalize_line(line):
    line = re.sub(r"^\d+\s*", "", line)  # Премахва водещи числа
    line = re.sub(r"\s*/.*", "", line)     # Премахва текст след наклонена черта
    return line.strip().lower()

def words_set(s):
    return set(re.findall(r'\b\w+\b', s.lower()))

def find_in_mapping(line, mapping):
    norm_line = normalize_line(line)
    line_words = words_set(norm_line)

    for key, value in mapping.items():
        key_words = words_set(key)
        if key_words.issubset(line_words) or line_words.issubset(key_words):
            return value['code'], key
    return None, None

def save_new_mapping(original_line, confirmed_code, mapping_file='mapping.json'):
    try:
        with open(mapping_file, 'r', encoding='utf-8') as f:
            mapping = json.load(f)
    except FileNotFoundError:
        mapping = {}

    cleaned_line = normalize_line(original_line)
    cleaned_line = re.sub(r'броя.*', '', cleaned_line).strip()

    if cleaned_line not in mapping:
        mapping[cleaned_line] = {
            "code": confirmed_code,
                    }
        with open(mapping_file, 'w', encoding='utf-8') as f:
            json.dump(mapping, f, ensure_ascii=False, indent=2)
        print(f"✅ Добавен нов ключ в mapping: '{cleaned_line}' -> {confirmed_code}")
    else:
        print(f"ℹ️ Ключът '{cleaned_line}' вече съществува в mapping.")

# --- Конфигурация ---
MAPPING_FILE = 'mapping.json'
MATERIALS_FILE = 'materials.csv'
EXPORT_DIR = 'export'
EXPORT_FILE = None  # Ще се определи динамично по-късно
FUZZY_MATCH_CUTOFF = 0.3 # Праг за близост на съвпадение (0.0 до 1.0)

# --- Проверка и импорт на опционални библиотеки ---
# OCR support
try:
    from pdf2image import convert_from_path
    from PIL import Image, ImageEnhance, ImageFilter, ImageOps
    import pytesseract
    # Ако потребителят не е задал пътя до Tesseract, може да се наложи да го укажете тук:
    # pytesseract.pytesseract.tesseract_cmd = r'C:\Program Files\Tesseract-OCR\tesseract.exe' # Пример за Windows
    PYTESSERACT_AVAILABLE = True
    print("INFO: OCR функционалност (Tesseract, pdf2image, PIL) е налична.")
except ImportError:
    PYTESSERACT_AVAILABLE = False
    print("WARN: OCR библиотеките (pytesseract, pdf2image, Pillow) не са инсталирани. Обработката на сканирани PDF/JPEG няма да работи.")

OSD_ROTATE_RE = re.compile(r"Rotate:\s*(\d+)")

# PDF support
try:
    from PyPDF2 import PdfReader
    PYPDF2_AVAILABLE = True
except ImportError:
    print("ERROR: Задължителната библиотека PyPDF2 не е инсталирана. Моля, инсталирайте я с 'pip install pypdf2'")
    sys.exit(1)

# --- Функции ---

def _apply_exif_orientation(image):
    try:
        return ImageOps.exif_transpose(image)
    except Exception:
        return image


def _deskew_image(image):
    if not PYTESSERACT_AVAILABLE:
        return image
    try:
        osd_output = pytesseract.image_to_osd(image, config="--psm 0")
    except Exception:
        return image
    match = OSD_ROTATE_RE.search(osd_output or "")
    if not match:
        return image
    try:
        angle = int(match.group(1))
    except (ValueError, TypeError):
        return image
    angle = angle % 360
    if angle in (0, 360):
        return image
    try:
        return image.rotate(-angle, expand=True)
    except Exception:
        return image


def preprocess_image(image):
    """Подготвя изображение за OCR."""
    try:
        working = _apply_exif_orientation(image)
        working = working.convert("L")
        working = ImageOps.autocontrast(working, cutoff=4)
        working = working.filter(ImageFilter.MedianFilter(size=3))
        working = _deskew_image(working)
        enhancer = ImageEnhance.Contrast(working)
        working = enhancer.enhance(1.4)
        threshold = working.point(lambda x: 0 if x < 135 else 255)
        return threshold
    except Exception as e:
        print(f"WARN: Грешка при препроцесинг на изображение: {e}")
        return image  # Върни оригиналното при грешка

def extract_text_from_pdf(pdf_path):
    """Комбинирано извличане: PyPDF2 + OCR fallback"""
    def has_meaningful_text(text: str) -> bool:
        cleaned = text.strip()
        if len(cleaned) < 40:
            return False
        letters = sum(1 for ch in cleaned if ch.isalpha())
        ratio = letters / len(cleaned)
        return ratio > 0.2

    text_pypdf2 = ""
    if PYPDF2_AVAILABLE:
        try:
            reader = PdfReader(pdf_path)
            text_pypdf2 = "\n".join((page.extract_text() or "") for page in reader.pages)
            print(f"INFO: PyPDF2 извлече {len(text_pypdf2)} символа.")
        except Exception as exc:
            print(f"ERROR: Грешка при четене на PDF с PyPDF2: {exc}")
            text_pypdf2 = ""

    if text_pypdf2 and has_meaningful_text(text_pypdf2):
        return text_pypdf2

    print("⚠️ PyPDF2 не върна достатъчно текст. Активирам OCR fallback…")
    if not PYTESSERACT_AVAILABLE:
        print("ERROR: OCR функционалност не е налична.")
        return text_pypdf2

    try:
        images = convert_from_path(pdf_path, dpi=300)
    except Exception as exc:
        print(f"ERROR: Неуспешно конвертиране на PDF в изображения: {exc}")
        return text_pypdf2

    ocr_text_parts = []
    for index, image in enumerate(images, start=1):
        print(f"INFO: OCR обработка на страница {index}/{len(images)}…")
        processed = preprocess_image(image)
        try:
            part = pytesseract.image_to_string(
                processed,
                config=r"-l bul+eng --oem 3 --psm 6",
            )
        except Exception as exc:
            print(f"WARN: OCR грешка на страница {index}: {exc}")
            part = ""
        ocr_text_parts.append(part)

    ocr_text = "\n".join(ocr_text_parts)
    print(f"INFO: OCR извлече {len(ocr_text)} символа.")
    return ocr_text or text_pypdf2

def ocr_image(image_obj):
    """Извлича текст от PIL Image обект с OCR."""
    if not PYTESSERACT_AVAILABLE:
        print("WARN: OCR не е наличен.")
        return ""
    try:
        processed_img = preprocess_image(image_obj.copy())
        # Задайте езици - български и английски
        custom_config = r'-l bul+eng --oem 3 --psm 6'
        text = pytesseract.image_to_string(processed_img, config=custom_config)
        # print(f"DEBUG: OCR Raw Output:\n---\n{text}\n---") # За отстраняване на грешки
        return text
    except Exception as e:
        print(f"ERROR: Грешка при OCR обработка на изображение: {e}")
        # Може да пробвате без препроцесинг при грешка
        try:
            print("INFO: Опит за OCR без препроцесинг...")
            text = pytesseract.image_to_string(image_obj, lang='bul+eng')
            return text
        except Exception as e2:
             print(f"ERROR: Повторна OCR грешка: {e2}")
             return ""

def extract_text_with_ocr(file_path):
    """Извлича текст от PDF (чрез OCR) или JPEG файл."""
    if not PYTESSERACT_AVAILABLE:
        print("ERROR: OCR функционалност не е налична.")
        return ""

    text = ""
    try:
        if file_path.lower().endswith('.pdf'):
            return extract_text_from_pdf(file_path)
        if file_path.lower().endswith(('.jpg', '.jpeg', '.png', '.tiff', '.bmp')):
            print(f"INFO: OCR на изображение ({os.path.basename(file_path)})…")
            with Image.open(file_path) as img:
                text = ocr_image(img)
        else:
            print(f"WARN: Неподдържан файлов тип за OCR: {file_path}")
            return ""

        print(f"INFO: Извлечен текст чрез OCR ({len(text)} символа).")
        return text

    except Exception as e:
        print(f"ERROR: Грешка при OCR екстракция от {file_path}: {e}")
        return ""
def merge_broken_lines(lines):
    """Обединява редове, които очевидно са част от един продукт (пример: описание + стойности)."""
    merged = []
    skip_next = False

    for i in range(len(lines)):
        if skip_next:
            skip_next = False
            continue

        current = lines[i]
        next_line = lines[i+1] if i + 1 < len(lines) else ""

        # Ако текущият ред няма числа, а следващият има поне 2 числа с десетични разделители → обединяваме
        if not re.search(r'\d', current) and len(re.findall(r'\d+[.,]\d{2}', next_line)) >= 2:
            combined = current.strip() + " " + next_line.strip()
            merged.append(combined)
            skip_next = True
        else:
            merged.append(current.strip())

    return merged

def is_product_line(line):
    """Проверява дали редът вероятно съдържа информация за продукт."""
    line = line.strip()
    if not line:
        return False
    # 1. Трябва да има цифри
    if not re.search(r'\d', line):
        return False
    # 2. Трябва да има поне 2 числа във формат цена/стойност (с ',' или '.' и 2 цифри)
    if len(re.findall(r'\b\d{1,10}[.,]\d{2}\b', line)) < 2:
         # Алтернативна проверка за число + мерна единица (напр. "3 бр", "1.5 кг")
         if not re.search(r'\b\d+[\.,]?\d*\s*(?:бр|pcs|кг|kg|л|l|м|m)\b', line, re.IGNORECASE):
             return False # Ако няма нито 2 цени, нито количество с единица - вероятно не е продукт
    # 3. Трябва да има поне 3 последователни букви (Кирилица или Латиница)
    if not re.search(r'[\u0400-\u04FFa-zA-Z]{3,}', line):
        return False
    # 4. Избягване на редове само с ДДС номер, дата, адрес и т.н. (евристика)
    if re.match(r'^(BG\s?)?\d{9,10}$', line): return False # ДДС номер
    if re.match(r'^\d{2}[./-]\d{2}[./-]\d{2,4}$', line): return False # Дата
    if 'адрес' in line.lower() or 'тел.' in line.lower() or 'e-mail' in line.lower(): return False
    if 'МОЛ' in line or 'IBAN' in line: return False
    if 'ДДС' in line and len(re.findall(r'\d+[.,]\d{2}', line)) <= 2 : return False # Редове само с ДДС суми

    # Ако всички проверки минат:
    return True

def extract_quantity(line):
    """Опитва да извлече количеството от реда (версия 3)."""
    print(f"  DEBUG_QTY: Анализирам ред: '{line}'")
    # Предварителна подготовка: добави интервал между думи и числа, ако са слепени
    line = re.sub(r'([а-яА-Яa-zA-Z]+)(\d+)', r'\1 \2', line)
    line = re.sub(r'(\d+)([а-яА-Яa-zA-Z]+)', r'\1 \2', line)
    numeric_values = []
    for n in re.findall(r'\d+[.,]?\d*', line):
        try:
            numeric_values.append(float(n.replace(',', '.')))
        except ValueError:
            continue



    # Метод 1: Търси число + мерна единица
    units_pattern = r'(бр|pcs|бр\.|брой|кг|kg|л|l|м|m|к-т|ком|оп|к-та)\b' # Улавяща група за единицата
    match_unit = re.search(r'(\d+[\.,]?\d*)\s*(' + units_pattern + r')', line, re.IGNORECASE)

    # --- Начало на логиката САМО ако Метод 1 намери съвпадение ---
    if match_unit:
        num_str_from_unit = match_unit.group(1) # Дефинира се САМО тук
        unit_found = match_unit.group(2)
        print(f"  DEBUG_QTY: Метод 1 намери: Число='{num_str_from_unit}', Единица='{unit_found}'")

        # ПРОВЕРКА: Дали намереното число прилича на цена? (с .,XX) - ТАЗИ ПРОВЕРКА Е ВЪТРЕ В if match_unit:
        if re.match(r'.*[.,]\d{2}$', num_str_from_unit):
            print(f"  DEBUG_QTY: Числото '{num_str_from_unit}' до единицата прилича на цена. Игнорирам за Метод 1.")
            # Не правим нищо, ще се продължи към Метод 2 СЛЕД else блока по-долу
        else:
            # Числото НЕ прилича на цена - вероятно е количество
            try:
                qty_str = num_str_from_unit.replace(',', '.')
                qty_float = float(qty_str)
                print(f"  DEBUG_QTY: Връщам количество от Метод 1: {qty_float}")
                return qty_float # Връщаме само ако НЕ прилича на цена
            except ValueError:
                print(f"  DEBUG_QTY: Грешка при конвертиране в Метод 1: {num_str_from_unit}")
                # Продължаваме към Метод 2
    # --- Край на логиката САМО ако Метод 1 намери съвпадение ---
    else: # Този else е за 'if match_unit:'
        print(f"  DEBUG_QTY: Метод 1 (число + единица) не намери съвпадение.")
        # Продължаваме към Метод 2

    # --- Метод 2 ЗАПОЧВА ТУК (изпълнява се само ако Метод 1 не е върнал стойност) ---
    print(f"  DEBUG_QTY: Изпълнявам Метод 2 (евристики)...")
    price_values_matches = re.findall(r'\b(\d{1,10}[.,]\d{2})\b', line)
    likely_prices_set = {p.replace(',', '.') for p in price_values_matches}
    print(f"  DEBUG_QTY: Вероятни цени/стойности в реда: {likely_prices_set}")

    all_numbers_matches = list(re.finditer(r'\b(\d+[\.,]?\d*)\b', line))
    print(f"  DEBUG_QTY: Всички намерени числа: {[m.group(1) for m in all_numbers_matches]}")

        # 🚫 ПРОВЕРКА: Първото число в началото на реда е под 100 и преди първата дума
    # Вероятно е пореден номер (позиция във фактурата), НЕ количество
    if all_numbers_matches:
        first_num_match = all_numbers_matches[0]
        first_num_str = first_num_match.group(1).replace(',', '.')
        try:
            first_num = float(first_num_str)
            if first_num.is_integer() and first_num < 100 and first_num_match.start() < 5:
                print(f"  DEBUG_QTY: '{first_num}' изглежда като пореден номер – ще го игнорираме.")
                all_numbers_matches = all_numbers_matches[1:]  # Премахваме го от списъка
        except ValueError:
            pass


    potential_qty = []
    words = re.findall(r'[\u0400-\u04FFa-zA-Z]{3,}', line)
    first_word_index = line.find(words[0]) if words else len(line)
    print(f"  DEBUG_QTY: Първа дума '{words[0] if words else 'N/A'}' на индекс: {first_word_index}")

    for match in all_numbers_matches:
        num_str = match.group(1)
        num_val_str = num_str.replace(',', '.')
        num_index = match.start()
        is_likely_price = num_val_str in likely_prices_set
        is_before_first_word = (num_index < first_word_index)
        print(f"  DEBUG_QTY: Проверявам число '{num_str}' на индекс {num_index}. Преди дума: {is_before_first_word}. Вероятна цена: {is_likely_price}")

        try:
            num_float = float(num_val_str)
        except ValueError:
            print(f"  DEBUG_QTY: Не мога да конвертирам '{num_str}' в число.")
            continue

        if is_likely_price:
            print(f"  DEBUG_QTY: Пропускам '{num_str}' – изглежда като цена.")
            continue

        try:
            num_float = float(num_val_str)
        except ValueError:
            print(f"  DEBUG_QTY: Не мога да конвертирам '{num_str}' в число.")
            continue

        # ⚠️ Филтър за 20.0 с подозрителен контекст (например ДДС)
        if num_float == 20.0:
            context_window = line[max(0, num_index - 10):num_index + 15].lower()
            if any(keyword in context_window for keyword in ['ддс', 'данък', 'сум', 'цена']):
                print(f"  DEBUG_QTY: ❌ Игнорирам 20.0 – съседен текст изглежда като ДДС: '{context_window}'")
                continue  # Пропускаме това число изцяло

        # ➕ Ако числото изглежда валидно, изчисли приоритета
            priority = 5
        if num_float == 1.0:
            priority = 0
            print(f"  DEBUG_QTY: -> Приоритет 0 (точно 1.0)")
        elif is_before_first_word and '.' not in num_val_str and ',' not in num_val_str and num_float <= 100 and num_index < 5:
            priority = 1
            print(f"  DEBUG_QTY: -> Приоритет 1 (малък int в началото)")
        elif is_before_first_word and '.' not in num_val_str and ',' not in num_val_str:
            priority = 2
            print(f"  DEBUG_QTY: -> Приоритет 2 (друг int преди дума)")
        elif is_before_first_word:
            priority = 3
            print(f"  DEBUG_QTY: -> Приоритет 3 (float преди дума)")
        elif '.' not in num_val_str and ',' not in num_val_str:
            priority = 4
            print(f"  DEBUG_QTY: -> Приоритет 4 (int след дума)")
        else:
            priority = 5
            print(f"  DEBUG_QTY: -> Приоритет 5 (float след дума)")

        potential_qty.append({'value': num_float, 'priority': priority, 'index': num_index})


    if potential_qty:
         potential_qty.sort(key=lambda x: (x['priority'], x['index']))
         best_qty = potential_qty[0]['value']
         print(f"  DEBUG_QTY: Потенциални количества (сортирани): {potential_qty}")
         print(f"  DEBUG_QTY: Избрано количество: {best_qty}")
         # Постави преди return best_qty:
         for i in range(len(numeric_values) - 2):
             q, unit_price, total = numeric_values[i:i+3]
             if q > 0 and unit_price > 0:
                 calculated_total = round(q * unit_price, 2)
                 if abs(calculated_total - total) <= 0.01 * total:
                     print(f"✅ DEBUG_QTY: Метод 2.6: Шаблон съвпада: {q} * {unit_price} ≈ {total}")
                     return q
         return best_qty
    
    
    # Метод 3: Ако нищо не е намерено, връщаме 1.0
    print(f"WARN: Неуспешно извличане на количество от ред чрез евристики: '{line}'. Приема се 1.0.")
    return 1.0
def load_materials_db():
    """Зарежда базата данни с материали от CSV файл."""
    if not PANDAS_AVAILABLE:
        print("WARN: Зареждането на materials.csv е пропуснато (pandas липсва).")
        return None
    try:
        df = pd.read_csv(MATERIALS_FILE, sep=';', encoding='cp1251', dtype=str)
        # Проверка за наличие на задължителните колони
        required_cols = ['Номер', 'Име на материал', 'Последна покупна цена', 'Продажна цена', 'Баркод']
        if not all(col in df.columns for col in required_cols):
            missing = [col for col in required_cols if col not in df.columns]
            print(f"ERROR: Липсват колони в '{MATERIALS_FILE}': {', '.join(missing)}")
            sys.exit(1)
        # Изчистване на празни стойности в ключови колони
        df = df.dropna(subset=['Номер', 'Име на материал'])
        # Попълване на празни цени/баркод с '0' или празен стринг
        df['Последна покупна цена'] = df['Последна покупна цена'].fillna('0.00')
        df['Продажна цена'] = df['Продажна цена'].fillna('0.00')
        df['Баркод'] = df['Баркод'].fillna('')
        print(f"INFO: Успешно заредени {len(df)} записа от '{MATERIALS_FILE}'.")
        return df[required_cols]
    except FileNotFoundError:
        print(f"\n❌ Файлът с материали '{MATERIALS_FILE}' не е намерен!")
        print("Моля, постави го в папката и натисни Enter, за да опиташ отново...")

        while True:
            input("➡️ Натисни Enter за повторен опит (или затвори прозореца за отказ): ")
            if os.path.exists(MATERIALS_FILE):
                return load_materials_db()  # Повтори зареждането
            else:
                print(f"⛔️ Файлът '{MATERIALS_FILE}' все още липсва.")

def load_mapping():
    """Зарежда JSON файла с асоциации."""
    if os.path.exists(MAPPING_FILE):
        try:
            with open(MAPPING_FILE, 'r', encoding='utf-8') as f:
                mapping_data = json.load(f)
                print(f"INFO: Успешно заредени {len(mapping_data)} асоциации от '{MAPPING_FILE}'.")
                return mapping_data
        except json.JSONDecodeError:
            print(f"ERROR: Файлът '{MAPPING_FILE}' е повреден (невалиден JSON). Ще бъде създаден нов.")
            return {}
        except Exception as e:
            print(f"ERROR: Грешка при зареждане на '{MAPPING_FILE}': {e}")
            return {} # Върни празен речник при друга грешка
    else:
        print(f"INFO: Файлът '{MAPPING_FILE}' не съществува. Ще бъде създаден при първото запазване.")
        return {}

def save_mapping(mapping):
    """Запазва JSON файла с асоциации."""
    try:
        with open(MAPPING_FILE, 'w', encoding='utf-8') as f:
            json.dump(mapping, f, ensure_ascii=False, indent=2, sort_keys=True)
        print(f"INFO: Асоциациите са запазени в '{MAPPING_FILE}'.")
    except Exception as e:
        print(f"ERROR: Грешка при запазване на '{MAPPING_FILE}': {e}")

def export_to_mistral_format(items, export_path):
    """Експортира обработените артикули в TXT формат за Мистрал."""
    try:
        with open(export_path, 'w', encoding='cp1251') as f:
            # Header - Важно е да е точно така!
            f.write("Склад\tСклад\tНомер\tИме на материал\tК-во\tЕд. цена\tПродажна цена\tБаркод\n")
            for item in items:
                # Форматиране на числата с точка като десетичен разделител
                qty_str = str(item['qty']) # Вече трябва да е float
                unit_price_str = str(item['purchase_price']).replace(',', '.') # Гарантираме точка
                selling_price_str = str(item['selling_price']).replace(',', '.') # Гарантираме точка

                # Запис на реда с TAB разделители
                f.write(f"1.00\tСклад\t{item['code']}\t{item['name']}\t"
                        f"{qty_str}\t{unit_price_str}\t"
                        f"{selling_price_str}\t{item['barcode']}\n")
        print(f"\n✅ Експортът ({len(items)} артикула) е успешно записан в: {EXPORT_FILE}\n")
    except Exception as e:
        print(f"ERROR: Грешка при експортиране в '{EXPORT_FILE}': {e}")

# --- Основна логика ---
print("DEBUG: Стигнах до дефиницията на main()")
def main(input_path=None, gui_mode=False):
    print("DEBUG: Влязох в main()")
    if input_path is None:
        input_path = input_path = prompt_user("Въведи пълния път до PDF или JPEG файла:", gui_mode=gui_mode)
    else:
        print(f"INFO: Получен файл от GUI: {input_path}")

    if not os.path.exists(input_path):
        print(f"❌ Грешка: Файлът '{input_path}' не е намерен!")
        return

    # ... продължава логиката ...
    # 🆕 ИЗВЛИЧАНЕ НА ИМЕТО НА ФАЙЛА И СЪЗДАВАНЕ НА ПАПКА export/
    invoice_filename = os.path.splitext(os.path.basename(input_path))[0]

    if not os.path.exists(EXPORT_DIR):
        os.makedirs(EXPORT_DIR)
        print(f"INFO: Създадена е папка за експорти: {EXPORT_DIR}")

    export_path = os.path.join(EXPORT_DIR, f"export_{invoice_filename}.txt")

    if not os.path.exists(input_path):
        print(f"❌ Грешка: Файлът '{input_path}' не е намерен!")
        return

    # 2. Зареждане на данни
    materials_df = load_materials_db() if PANDAS_AVAILABLE else None
    mapping = load_mapping()
    export_items = []
    processed_lines_count = 0
    skipped_lines_count = 0
    matched_via_mapping = 0
    matched_via_fuzzy = 0
    matched_via_manual = 0
    failed_match_count = 0

    # 3. Извличане на текст
    text = ""
    file_ext = os.path.splitext(input_path)[1].lower()

    if file_ext == '.pdf':
        text = extract_text_from_pdf(input_path)

    elif file_ext in ('.jpg', '.jpeg', '.png', '.tiff', '.bmp'):
        if PYTESSERACT_AVAILABLE:
            text = extract_text_with_ocr(input_path)
        else:
            print("ERROR: OCR не е наличен за обработка на изображения.")
            return

    else:
        print(f"❌ Грешка: Неподдържан файлов формат '{file_ext}'. Поддържат се PDF, JPG, JPEG, PNG, TIFF, BMP.")
        return

    # тази проверка вече е след всичко
    if not text or not text.strip():
        print("❌ Грешка: Неуспешно извличане на текст от файла.")
        return


    lines = [line.strip() for line in text.split('\n') if line.strip()]
    print(f"\n--- Извлечени {len(lines)} реда от документа ---")

    # 4. Обработка на редовете
    needs_saving = False # Флаг дали има промяна в mapping.json
    if materials_df is not None:
        material_names_list = materials_df['Име на материал'].tolist() # За по-бързо търсене
    else:
        material_names_list = []
        print("WARN: Няма заредена таблица с материали – автоматичните съвпадения са ограничени.")

    for i, line in enumerate(lines):
        print(f"\n[Ред {i+1}/{len(lines)}] Обработвам: '{line}'")
        if not is_product_line(line):
            print("  -> Пропускам (не изглежда като продуктов ред).")
            skipped_lines_count += 1
            continue

        processed_lines_count += 1
        found = False
        item_data = None

        only_name = normalize_line(line)
        # --- НАЧАЛО НА КОРЕКЦИЯТА ---
        # Тези два реда трябва да са ПРЕДИ цикъла for key in mapping:
        line_lower = line.lower()
        # Взимаме всички думи (букви/цифри/тирета) от реда веднъж
        line_words = set(re.findall(r'\b[\w\u0400-\u04FF-]+\b', only_name))
        cleaned_line = normalize_line(line)  # Изчистен ред за сравнение с mapping

        # 4.1 Проверка в mapping.json
        
        print(f"  DEBUG_MAP: Проверявам ред: '{line}'")
        # Този print вече ще работи, защото line_words е дефиниран по-горе:
        print(f"  DEBUG_MAP: Думи в ред: {line_words}")
        for key in mapping:
            key_lower = key.lower()
            # Взимаме всички думи от ключа (включително тези с тирета)
            key_words = set(re.findall(r'\b[\w\u0400-\u04FF-]+\b', key_lower))

            # Проверка: Дали ключът има думи И дали всички думи от ключа се съдържат в думите от реда?
            match_found = bool(key_words and key_words.issubset(line_words))
            print(f"  DEBUG_MAP: Сравнявам ДУМИ от ключ '{key}' (-> {key_words}) с ДУМИ от ред -> Резултат: {match_found}")

            if match_found:
                data = mapping[key]
                if materials_df is None:
                    print("  ⚠️ materials.csv не е зареден – пропускам mapping за този ред.")
                    failed_match_count += 1
                    found = True
                    break
                # Намиране на реда в базата данни
                row = materials_df[materials_df['Номер'] == data['code']]
                if not row.empty:
                    row_data = row.iloc[0]
                    # Създаване на item_data, извикване на extract_quantity и т.н.
                    item_data = {
                        'code': row_data['Номер'],
                        'name': row_data['Име на материал'], # Винаги името от базата
                        'qty': extract_quantity(line),      # Извличаме количеството ВИНАГИ
                        'purchase_price': row_data['Последна покупна цена'], # От базата
                        'selling_price': row_data['Продажна цена'], # От базата
                        'barcode': row_data['Баркод'], # От базата
                        'token': line,
                    }
                    print(f"  ✅ Намерено в mapping чрез СЪВПАДЕНИЕ НА ДУМИ: '{key}' -> {item_data['code']} / {item_data['name']}")
                    export_items.append(item_data)
                    matched_via_mapping += 1
                    found = True # Маркираме, че е намерен
                    break # Излизаме от цикъла for key in mapping
                else:
                    print(f"  ⚠️ Намерен ключ '{key}' в mapping (по думи), но код '{data['code']}' не е открит в materials.csv! Пропускам.")
                    failed_match_count +=1
                    found = True # Маркираме като обработен (макар и грешно), за да не търси fuzzy
                    break # Излизаме от цикъла for key in mapping
        # --- Край на mapping проверката ---

        # Този if блок си остава СЛЕД цикъла for key in mapping:
        if found:
            # Ако е намерен чрез mapping ИЛИ е възникнал проблем с кода в mapping (found=True),
            # пропусни fuzzy matching и мини към следващия ред от фактурата
            continue
        # --- КРАЙ НА КОРЕКЦИЯТА ---

        # Ако НЕ е намерен в mapping (found остава False):
        # Логиката за fuzzy matching започва от тук (ред ~422 и надолу)
        print("  -> Не е намерен в mapping. Търся близко съвпадение...")
        # Увери се, че този ред е тук и не е коментар:
        closest_matches = difflib.get_close_matches(line, material_names_list, n=1, cutoff=FUZZY_MATCH_CUTOFF)
        if closest_matches:
             # ... и т.н. ...
             if found:
            # Ако е намерен чрез mapping ИЛИ е възникнал проблем с кода в mapping (found=True),
            # пропусни fuzzy matching и мини към следващия ред от фактурата
                   continue



        # 4.2 Търсене чрез Fuzzy Matching (ако не е намерен в mapping)
        print("  -> Не е намерен в mapping. Търся близко съвпадение...")
        closest_matches = difflib.get_close_matches(line, material_names_list, n=1, cutoff=FUZZY_MATCH_CUTOFF)

        if closest_matches:
            if materials_df is None:
                print("  ⚠️ Няма заредена таблица с материали – пропускам fuzzy съвпадението.")
                failed_match_count += 1
                continue
            matched_name = closest_matches[0]
            matched_row = materials_df[materials_df['Име на материал'] == matched_name]
            if matched_row.empty:
                print("  ⚠️ Предложеното съвпадение не е намерено в materials.csv – пропускам.")
                failed_match_count += 1
                continue
            matched_row = matched_row.iloc[0]

            print(f"\n❓ Потенциално съвпадение за ред: '{line}'")
            print(f"   -> Предложение от базата: {matched_row['Номер']} – {matched_name} (Последна покупна: {matched_row['Последна покупна цена']})")

            while True:
                answer = prompt_user(
                    f"Ред:\n'{line}'\n\nПредложение от базата:\n{matched_row['Номер']} – {matched_name}\n\nПотвърждаваш ли?\n(въведи: y = да / n = не / s = пропусни)",
                    ['y', 'n', 's'],
                    gui_mode=gui_mode
                )
                if answer == 'cancel':
                    print("❌ Прекъснато от потребителя. Пропускам реда.")
                    failed_match_count += 1
                    break
                elif answer in ['y', 'yes', 'д', 'да']:
                    # Потвърдено съвпадение
                    ...
                    # Потвърдено съвпадение
                    item_data = {
                        'code': matched_row['Номер'],
                        'name': matched_name, # Името от базата
                        'qty': extract_quantity(line),
                        'purchase_price': matched_row['Последна покупна цена'], # От базата
                        'selling_price': matched_row['Продажна цена'], # От базата
                        'barcode': matched_row['Баркод'], # От базата
                        'token': line,
                    } # <--- УВЕРИ СЕ, ЧЕ ТАЗИ СКОБА Е ТУК!
                    export_items.append(item_data)
                    save_new_mapping(line, matched_row['Номер'])  # <-- този ред добави ТУК
                    matched_via_fuzzy += 1
                    break # Премини към следващия ред
                    # Горният код при теб свършва с break на ред 393
                elif answer in ['n', 'no', 'н', 'не']:
                    # Отхвърлено съвпадение - питай за ръчен код
                    manual_code = prompt_user("Въведи правилния код на материала (остави празно за пропускане):", None, gui_mode=gui_mode)
                    if not manual_code:
                        print("  -> Пропускам този ред по желание на потребителя.")
                        failed_match_count +=1
                        break # Премини към следващия ред

                    result = materials_df[materials_df['Номер'] == manual_code]
                    if not result.empty:
                        row_data = result.iloc[0]
                        manual_matched_name = row_data['Име на материал']
                        item_data = {
                            'code': manual_code,
                            'name': manual_matched_name, # Името от базата
                            'qty': extract_quantity(line),
                            'purchase_price': row_data['Последна покупна цена'], # От базата
                            'selling_price': row_data['Продажна цена'], # От базата
                            'barcode': row_data['Баркод'], # От базата
                            'token': line,
                        } # <--- УВЕРИ СЕ, ЧЕ И ТУК ИМА СКОБА!
                        export_items.append(item_data)
                         # Добавяне към mapping с ИМЕТО ОТ БАЗАТА като ключ
                        cleaned_line = normalize_line(line)
                        mapping[cleaned_line] = {
                            'code': manual_code,
                            'name': manual_matched_name  # това остава името от базата
                        }
                        needs_saving = True # Маркираме за запис
                        print(f"  ✅ Ръчно въведен код '{manual_code}' ({manual_matched_name}). Добавям към експорт и записвам в mapping.")
                        matched_via_manual += 1
                        break # Премини към следващия ред
                    else:
                        print(f"  ❌ Грешка: Код '{manual_code}' не е намерен в '{MATERIALS_FILE}'. Опитай пак.")
                        # Цикълът ще попита отново за Y/n/s
                elif answer in ['s', 'skip']:
                     print("  -> Пропускам този ред по желание на потребителя.")
                     failed_match_count +=1
                     break # Премини към следващия ред
                else:
                    print("  -> Невалиден отговор. Моля, въведи Y, N или S.")
            # Край на while True: за Y/N/S цикъла (отместването е навътре)
        else:
                # Няма намерено близко съвпадение (този else е за if closest_matches:)
                print(f"  ❌ Не е намерено съвпадение на '{line}' (нито в mapping, нито близко в базата). Въведи ръчно код.")
                manual_code = prompt_user("Въведи правилния код на материала (остави празно за пропускане):", None, gui_mode=gui_mode)
                if not manual_code:
                    print("  -> Пропускам този ред по желание на потребителя.")
                    failed_match_count +=1
                    break # Премини към следващия ред

                if materials_df is None:
                    print("  ⚠️ Няма заредена таблица с материали – ръчният код не може да бъде проверен.")
                    failed_match_count += 1
                    continue

                result = materials_df[materials_df['Номер'] == manual_code]
                if not result.empty:
                    row_data = result.iloc[0]
                    manual_matched_name = row_data['Име на материал']
                    item_data = {
                        'code': manual_code,
                        'name': manual_matched_name, # Името от базата
                        'qty': extract_quantity(line),
                        'purchase_price': row_data['Последна покупна цена'], # От базата
                        'selling_price': row_data['Продажна цена'], # От базата
                        'barcode': row_data['Баркод'], # От базата
                        'token': line,
                    }     # <--- УВЕРИ СЕ, ЧЕ И ТУК ИМА СКОБА!
                    export_items.append(item_data)
                    # Добавяне към mapping с ИМЕТО ОТ БАЗАТА като ключ
                    cleaned_line = normalize_line(line)
                    mapping[cleaned_line] = {
                        'code': manual_code,
                        'name': manual_matched_name  # това остава името от базата
                    }
                    needs_saving = True # Маркираме за запис
                    print(f"  ✅ Ръчно въведен код '{manual_code}' ({manual_matched_name}). Добавям към експорт и записвам в mapping.")
                    matched_via_manual += 1
                    continue # Премини към следващия ред
                else:
                    print(f"  ❌ Грешка: Код '{manual_code}' не е намерен в '{MATERIALS_FILE}'. Опитай пак.")
                        # Цикълът ще попита отново за Y/n/s
    # Край на for line in lines: цикъла (отместването е навън, подравнено с for)

    # 5. Запазване на mapping (ако има промени) - Този блок е СЛЕД for цикъла (подравнен с for)
    if needs_saving:
        save_mapping(mapping)
    else:
        print("\nINFO: Няма промени в mapping файла.")

    # 6. Експорт - Този блок е СЛЕД for цикъла (подравнен с for)
    if export_items:
        export_to_mistral_format(export_items, export_path)

    else:
        print("\n⚠️ Няма намерени/потвърдени артикули за експортиране.")

    # 7. Статистика - Този блок е СЛЕД for цикъла (подравнен с for)
    print("\n--- Статистика на обработката ---")
    print(f"Общо редове в документа: {len(lines)}")
    print(f"Пропуснати редове (не продуктови): {skipped_lines_count}")
    print(f"Обработени продуктови редове: {processed_lines_count}")
    print("-" * 20)
    print(f"Намерени чрез mapping.json: {matched_via_mapping}")
    print(f"Намерени чрез близко съвпадение (потвърдени): {matched_via_fuzzy}")
    print(f"Намерени чрез ръчно въведен код: {matched_via_manual}")
    print(f"Неуспешни/пропуснати продуктови редове: {failed_match_count}")
    print("-" * 20)
    print(f"Артикули добавени в експортния файл: {len(export_items)}")
    print("--- Край ---")
# Край на функцията main() - тук свършва отместването навътре за main

    return export_items


# --- Стартиране --- (Този блок е ИЗВЪН main(), без отместване)
if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\nКритична грешка в програмата: {e}")
        import traceback
        traceback.print_exc() # Отпечатва пълния traceback за дебъг
    finally:
        prompt_user("\Натисни Enter за изход...", gui_mode=False)





