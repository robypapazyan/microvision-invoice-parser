"""Основен графичен интерфейс за MicroVision Invoice Parser."""

from __future__ import annotations

import json
import logging
import os
import sys
import hashlib
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import tkinter as tk
from tkinter import ttk, filedialog, messagebox

import db_integration
import extract_and_prepare

try:  # Падаме gracefully ако модулът все още не е наличен
    from license_utils import validate_license  # type: ignore
except Exception:  # pragma: no cover - защитаваме се при липсващ модул
    validate_license = None  # type: ignore

try:  # legacy fallback
    from db_integration import operator_login_session  # type: ignore
except Exception:  # pragma: no cover
    operator_login_session = None  # type: ignore


logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")


APP_TITLE = "MicroVision Invoice Parser"
APP_SUBTITLE = "Продукт на Микро Вижън ЕООД | тел. 0883766674 | www.microvision.bg"

CLIENTS_JSON = "mistral_clients.json"


@dataclass
class SessionState:
    """Държи текущото състояние на операторската сесия."""

    profile_name: Optional[str] = None
    profile_data: Optional[Dict[str, Any]] = None
    username: str = ""
    user_id: Optional[int] = None
    raw_login_payload: Any = None
    db_mode: bool = False



# -------------------------
# Помощни функции
# -------------------------
def machine_id() -> str:
    """
    Правим стабилен (но не секретен) машинен ID от hostname + sys info.
    Ползва се само за показване.
    """
    base = f"{os.name}|{sys.platform}|{os.getenv('COMPUTERNAME','')}|{os.getenv('USERNAME','')}"
    return hashlib.sha1(base.encode("utf-8")).hexdigest()[:16]


def load_profiles(path: str = CLIENTS_JSON) -> Dict[str, Dict[str, Any]]:
    """Зарежда профилите и ги връща като {име: профил}."""

    try:
        with open(path, "r", encoding="utf-8") as fh:
            data = json.load(fh)
    except FileNotFoundError:
        logging.exception("Файлът %s липсва.", path)
        return {}
    except Exception as exc:  # pragma: no cover
        logging.exception("Неуспешно зареждане на профилите: %s", exc)
        return {}

    profiles: Dict[str, Dict[str, Any]] = {}
    if isinstance(data, dict):
        for key, value in data.items():
            if isinstance(value, dict):
                profiles[str(key)] = value
    elif isinstance(data, list):
        for idx, item in enumerate(data):
            if not isinstance(item, dict):
                continue
            name = str(item.get("name") or item.get("client") or f"Профил {idx + 1}")
            profiles[name] = item
    else:
        logging.error("Неочакван формат на %s. Очаква се dict или list.", path)
        return {}

    return profiles


# -------------------------
# Основно приложение
# -------------------------
class MicroVisionApp:
    def __init__(self, root: tk.Tk) -> None:
        self.root = root
        self.root.title(APP_TITLE)
        self.root.minsize(880, 540)

        icon_path = os.path.join(os.path.abspath(os.path.dirname(__file__)), "MicroVision_logo_2025.ico")
        if os.path.exists(icon_path):
            try:
                self.root.iconbitmap(icon_path)
            except Exception:  # pragma: no cover - iconbitmap не работи на някои платформи
                logging.debug("Неуспешно зареждане на икона от %s", icon_path)

        self.session = SessionState()
        self.profiles = load_profiles()
        self.profile_names: List[str] = list(self.profiles.keys())
        self.active_profile: Optional[Dict[str, Any]] = None
        self.active_profile_name: Optional[str] = None
        self.rows_cache: List[Dict[str, Any]] = []

        self._build_ui()
        self.session.db_mode = bool(self.db_mode_var.get())

        self._log("Приложението е стартирано.")
        if self.profile_names:
            self.profile_cmb.current(0)
            self._apply_profile(self.profile_names[0])
        else:
            self._log("⚠️ Няма профили в mistral_clients.json.")

        self._refresh_license_text()
        self.root.after(150, self.password_entry.focus_set)

    # ----------------- UI helpers -----------------

    def _build_ui(self) -> None:
        banner = ttk.Frame(self.root, padding=(16, 16, 16, 4))
        banner.pack(side="top", fill="x")

        title = ttk.Label(banner, text="MICRO VISION", font=("Segoe UI", 20, "bold"))
        subtitle = ttk.Label(banner, text=APP_TITLE, font=("Segoe UI", 12))
        title.grid(row=0, column=0, sticky="w")
        subtitle.grid(row=1, column=0, sticky="w")

        self.license_var = tk.StringVar(value="Лиценз: проверка...")

        strip = ttk.Frame(self.root, padding=(16, 8))
        strip.pack(side="top", fill="x")

        ttk.Label(strip, text="Профил:").grid(row=0, column=0, sticky="w")
        self.profile_cmb = ttk.Combobox(strip, state="readonly", width=28, values=self.profile_names)
        self.profile_cmb.grid(row=0, column=1, sticky="w", padx=(4, 12))
        self.profile_cmb.bind("<<ComboboxSelected>>", self._on_profile_change)

        ttk.Label(strip, text="Потребител:").grid(row=0, column=2, sticky="w")
        self.username_var = tk.StringVar()
        self.username_entry = ttk.Entry(strip, textvariable=self.username_var, width=18)
        self.username_entry.grid(row=0, column=3, sticky="w", padx=(4, 12))

        ttk.Label(strip, text="Парола:").grid(row=0, column=4, sticky="w")
        self.password_var = tk.StringVar()
        self.password_entry = ttk.Entry(strip, textvariable=self.password_var, show="•", width=18)
        self.password_entry.grid(row=0, column=5, sticky="w", padx=(4, 12))
        self.password_entry.bind("<Return>", lambda _e: self._on_login_clicked())

        self.login_status_var = tk.StringVar(value="Вход: няма активна сесия.")
        ttk.Button(strip, text="Вход", command=self._on_login_clicked).grid(row=0, column=6, padx=(0, 12))
        ttk.Label(strip, textvariable=self.login_status_var, foreground="#006400").grid(
            row=0, column=7, sticky="w"
        )

        self.db_mode_var = tk.BooleanVar(value=False)
        ttk.Checkbutton(
            strip,
            text="DB режим (отворена доставка)",
            variable=self.db_mode_var,
            command=self._on_db_mode_toggle,
        ).grid(row=1, column=0, columnspan=3, sticky="w", pady=(8, 0))

        ttk.Button(strip, text="ID на компютъра", command=self._on_get_machine_id).grid(
            row=1, column=3, padx=(4, 0), pady=(8, 0)
        )

        strip2 = ttk.Frame(self.root, padding=(16, 0, 16, 8))
        strip2.pack(side="top", fill="x")
        ttk.Button(strip2, text="Обработи файл…", command=self._on_process_file).grid(row=0, column=0, padx=(0, 8))

        outfrm = ttk.Frame(self.root, padding=(16, 0, 16, 16))
        outfrm.pack(side="top", fill="both", expand=True)
        self.output_text = tk.Text(outfrm, height=18, wrap="word")
        self.output_text.pack(side="left", fill="both", expand=True)
        yscroll = ttk.Scrollbar(outfrm, command=self.output_text.yview)
        yscroll.pack(side="right", fill="y")
        self.output_text.configure(yscrollcommand=yscroll.set)

        status = ttk.Frame(self.root, padding=(16, 4, 16, 12))
        status.pack(side="bottom", fill="x")
        ttk.Label(status, textvariable=self.license_var, foreground="#555").pack(side="right")

    def _log(self, text: str) -> None:
        self.output_text.insert(tk.END, text + "\n")
        self.output_text.see(tk.END)

    def _report_error(self, message: str, exc: Optional[BaseException] = None) -> None:
        detail = ""
        if exc is not None:
            logging.exception(message)
            detail = str(exc).strip()
        if detail:
            self._log(f"❌ {message}: {detail}")
        else:
            self._log(f"❌ {message}")

    def _on_profile_change(self, _evt: Optional[tk.Event] = None) -> None:
        name = self.profile_cmb.get()
        if not name:
            return
        self._apply_profile(name)

    def _apply_profile(self, profile_name: str) -> None:
        profile = self.profiles.get(profile_name)
        self.active_profile = profile
        self.active_profile_name = profile_name if profile else None
        self.session.profile_name = self.active_profile_name
        self.session.profile_data = profile
        self._reset_login_state()
        if profile_name:
            self._log(f"Профил зареден: {profile_name}")

    def _reset_login_state(self) -> None:
        self.session.username = ""
        self.session.user_id = None
        self.session.raw_login_payload = None
        self.username_var.set("")
        self.password_var.set("")
        self.login_status_var.set("Вход: няма активна сесия.")

    def _on_get_machine_id(self) -> None:
        mid = machine_id()
        self._log(f"Machine ID: {mid}")
        try:
            messagebox.showinfo("ID на компютъра", mid)
        except Exception:
            pass

    def _on_db_mode_toggle(self) -> None:
        self.session.db_mode = bool(self.db_mode_var.get())
        status = "активиран" if self.session.db_mode else "изключен"
        self._log(f"DB режим (отворена доставка): {status}")

    def _ensure_ready_for_processing(self) -> bool:
        if not self.active_profile:
            self._report_error("Моля, изберете профил преди обработка.")
            return False
        if not self.session.user_id:
            self._log("ℹ️ Необходим е успешен вход. Моля, въведете потребител и парола.")
            return False
        return True

    def _on_login_clicked(self) -> None:
        if not self.active_profile:
            self._report_error("Моля, изберете профил преди вход.")
            return

        username = self.username_var.get().strip()
        password = self.password_var.get()
        if not password:
            self._report_error("Моля, въведете парола.")
            return

        self.session.profile_data = self.active_profile
        self.session.profile_name = self.active_profile_name

        try:
            login_fn = getattr(db_integration, "perform_login", None)
            if callable(login_fn):
                result = login_fn(self.session, username, password)
            else:
                if operator_login_session is None:
                    raise RuntimeError("perform_login не е налична и няма резервна функция.")
                result = self._legacy_login_bridge(username, password)
        except Exception as exc:
            self._report_error("Грешка при опит за вход. Опитайте отново.", exc)
            return

        if not result:
            self._log("❌ Невалидни данни за вход.")
            return

        user_id = self._extract_user_id(result)
        self.session.username = username
        self.session.user_id = user_id
        self.session.raw_login_payload = result

        display_user = username if username else "само парола"
        suffix = f" (ID: {user_id})" if user_id is not None else ""
        self.login_status_var.set(f"Вход: {display_user}{suffix}")
        self._log(f"✅ Успешен вход: {display_user}{suffix}")
        self.password_var.set("")
        self._refresh_license_text()

    def _legacy_login_bridge(self, username: str, password: str) -> Optional[Dict[str, Any]]:
        assert self.session.profile_data is not None
        login_arg = username or None
        user_id = operator_login_session(self.session.profile_data, login_arg, password)  # type: ignore[arg-type]
        if not user_id:
            return None
        return {"user_id": user_id, "login": username}

    @staticmethod
    def _extract_user_id(payload: Any) -> Optional[int]:
        if isinstance(payload, dict):
            for key in ("user_id", "id", "operator_id"):
                value = payload.get(key)
                if isinstance(value, int):
                    return value
            return None
        if isinstance(payload, (list, tuple)) and payload:
            first = payload[0]
            return first if isinstance(first, int) else None
        if isinstance(payload, int):
            return payload
        return None

    def _on_process_file(self) -> None:
        if not self._ensure_ready_for_processing():
            return

        file_path = filedialog.askopenfilename(
            title="Избор на документ",
            filetypes=[
                ("Документи", "*.pdf *.jpg *.jpeg *.png *.tiff *.bmp"),
                ("PDF", "*.pdf"),
                ("Изображения", "*.jpg *.jpeg *.png *.tiff *.bmp"),
                ("Всички файлове", "*.*"),
            ],
        )
        if not file_path:
            return

        self._process_file_path(file_path)

    def _process_file_path(self, file_path: str) -> None:
        self._log(f"🔄 Обработка на файл: {file_path}")

        try:
            rows = extract_and_prepare.main(file_path, gui_mode=True)  # type: ignore[arg-type]
        except TypeError:
            rows = extract_and_prepare.main(file_path)
        except Exception as exc:
            self._report_error("Възникна грешка при обработката на файла.", exc)
            return

        if rows is None:
            self._log("⚠️ Няма върнати редове от обработката.")
            return
        if not isinstance(rows, list):
            self._log("⚠️ Върнатият резултат не е списък с редове.")
            return

        self.rows_cache = rows
        count = len(rows)
        if count == 0:
            self._log("ℹ️ Няма разпознати редове в документа.")
        else:
            self._log(f"✅ Разпознати редове: {count}")
            self._preview_rows(rows)

        if self.session.db_mode and count:
            self._push_to_open_delivery(rows)

        if count:
            self._offer_export(rows, file_path)

    def _preview_rows(self, rows: List[Dict[str, Any]]) -> None:
        preview_count = min(5, len(rows))
        for idx in range(preview_count):
            row = rows[idx] or {}
            code = row.get("code") or row.get("Номер") or row.get("item_code")
            name = row.get("name") or row.get("Име") or row.get("description")
            qty = row.get("qty") or row.get("quantity") or row.get("Количество")
            self._log(f"  • {code or '—'} | {name or 'без име'} | количество: {qty if qty is not None else '?'}")
        if len(rows) > preview_count:
            self._log(f"  … още {len(rows) - preview_count} реда.")

    def _push_to_open_delivery(self, rows: List[Dict[str, Any]]) -> None:
        start_fn = getattr(db_integration, "start_open_delivery", None)
        push_fn = getattr(db_integration, "push_parsed_rows", None)
        if not (callable(start_fn) and callable(push_fn)):
            self._log("⚠️ DB режим е активен, но липсват функции за отворена доставка.")
            return

        try:
            start_fn(self.session)
            push_fn(self.session, rows)
            self._log("✅ Данните са изпратени към отворена доставка.")
        except Exception as exc:
            self._report_error("Грешка при изпращане към отворена доставка.", exc)

    def _offer_export(self, rows: List[Dict[str, Any]], file_path: str) -> None:
        self._log("💾 Изберете място за TXT експорт или затворете прозореца за отказ.")
        base = os.path.splitext(os.path.basename(file_path))[0]
        out_path = filedialog.asksaveasfilename(
            title="Експорт в TXT",
            defaultextension=".txt",
            initialfile=f"export_{base}.txt",
            filetypes=[("TXT файлове", "*.txt"), ("Всички файлове", "*.*")],
        )
        if not out_path:
            self._log("ℹ️ Експортът в TXT е пропуснат.")
            return

        export_fn = getattr(db_integration, "export_txt", None)
        if not callable(export_fn):
            self._log("⚠️ Липсва функция за експорт в TXT.")
            return

        try:
            export_fn(rows, out_path)
            self._log(f"💾 TXT файлът е записан: {out_path}")
        except Exception as exc:
            self._report_error("Неуспешен експорт в TXT.", exc)

    def _refresh_license_text(self) -> None:
        if validate_license is None:
            self.license_var.set("Лиценз: проверка недостъпна")
            return
        try:
            remaining = validate_license()
            if remaining is None:
                raise ValueError("Няма информация за оставащи дни")
            self.license_var.set(f"Лиценз: оставащи {remaining} дни")
        except Exception as exc:
            logging.exception("Грешка при проверка на лиценза: %s", exc)
            self.license_var.set("Лиценз: грешка при проверка")


# -------------------------
# main
# -------------------------
def main() -> None:
    root = tk.Tk()
    try:
        root.call("tk", "scaling", 1.15)
        style = ttk.Style()
        if "vista" in style.theme_names():
            style.theme_use("vista")
    except Exception:
        pass

    MicroVisionApp(root)
    root.mainloop()


if __name__ == "__main__":
    main()
