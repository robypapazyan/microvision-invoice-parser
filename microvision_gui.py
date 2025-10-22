"""Основен графичен интерфейс за MicroVision Invoice Parser."""

from __future__ import annotations

import json
import os
import sys
import hashlib
import subprocess
from dataclasses import dataclass, field
from datetime import datetime
from importlib import import_module
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional

import tkinter as tk
from tkinter import ttk, filedialog, messagebox, simpledialog

import db_integration
from mistral_db import logger

try:  # legacy fallback
    from db_integration import operator_login_session  # type: ignore
except Exception:  # pragma: no cover
    operator_login_session = None  # type: ignore


APP_TITLE = "MicroVision Invoice Parser"
APP_SUBTITLE = "Продукт на Микро Вижън ЕООД | тел. 0883766674 | www.microvision.bg"

CLIENTS_JSON = "mistral_clients.json"


def ensure_clients_file(path: str = CLIENTS_JSON) -> None:
    file_path = Path(path)
    if file_path.exists():
        return
    sample_profile = {
        "name": "Local SAMPLE",
        "host": "localhost",
        "port": 3050,
        "database": "C:/Mistral/data/EXAMPLE.FDB",
        "user": "SYSDBA",
        "password": "masterkey",
        "charset": "WIN1251",
        "comment": "Автоматично генериран примерен профил. Попълнете реалните стойности.",
    }
    payload = [sample_profile]
    try:
        file_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        logger.warning(
            "Създаден е примерен mistral_clients.json. Попълнете реални параметри преди работа."
        )
    except Exception as exc:
        logger.exception(
            "Неуспешно създаване на примерен mistral_clients.json: {}",
            exc,
        )


def _check_runtime_dependencies() -> None:
    modules = {
        "loguru": "loguru",
        "PyPDF2": "PyPDF2",
        "pdf2image": "pdf2image",
        "PIL": "Pillow",
        "pytesseract": "pytesseract",
        "fdb": "fdb",
        "firebird.driver": "firebird-driver",
    }
    missing: List[str] = []
    for module_name, pip_name in modules.items():
        try:
            import_module(module_name)
        except ImportError:
            missing.append(pip_name)
        except Exception:
            continue
    if missing:
        logger.warning(
            "Липсващи зависимости: {}",
            ", ".join(sorted(set(missing))),
        )


@dataclass
class SessionState:
    """Държи текущото състояние на операторската сесия."""

    profile_name: Optional[str] = None
    profile_data: Optional[Dict[str, Any]] = None
    username: str = ""
    user_id: Optional[int] = None
    raw_login_payload: Any = None
    db_mode: bool = False
    last_login_trace: Optional[List[Dict[str, Any]]] = None
    password: str = ""
    ui_root: Any = None
    output_logger: Optional[Callable[[str], None]] = None
    select_user_callback: Optional[Callable[[List[Dict[str, Any]]], Optional[Dict[str, Any]]]] = None
    unresolved_items: List[Dict[str, Any]] = field(default_factory=list)
    catalog_preview: Dict[str, Any] = field(default_factory=dict)
    catalog_loaded: bool = False
    materials_preview: List[Dict[str, Any]] = field(default_factory=list)
    barcodes_preview: List[Dict[str, Any]] = field(default_factory=list)



class CandidateDialog(tk.Toplevel):
    """Диалог за избор между няколко артикула."""

    def __init__(self, parent: tk.Tk, token: str, candidates: List[str]) -> None:
        super().__init__(parent)
        self.result: Optional[int | str] = None
        self.title("Избор на артикул")
        self.transient(parent)
        self.grab_set()
        self.resizable(False, False)
        self.protocol("WM_DELETE_WINDOW", self._on_cancel)

        frame = ttk.Frame(self, padding=12)
        frame.pack(fill="both", expand=True)

        ttk.Label(frame, text="Разпознат текст:", font=("Segoe UI", 9, "bold")).pack(anchor="w")
        token_box = tk.Text(frame, height=2, width=50, wrap="word", relief="groove", borderwidth=1)
        token_box.pack(fill="x", pady=(0, 8))
        token_box.insert("1.0", token)
        token_box.configure(state="disabled")

        ttk.Label(frame, text="Изберете правилния артикул:").pack(anchor="w")
        self.listbox = tk.Listbox(frame, height=min(6, len(candidates)), exportselection=False)
        self.listbox.pack(fill="both", expand=True, pady=(4, 8))
        for entry in candidates:
            self.listbox.insert(tk.END, entry)

        btns = ttk.Frame(frame)
        btns.pack(fill="x")
        self.select_btn = ttk.Button(btns, text="Избери", command=self._on_select, state="disabled")
        self.select_btn.pack(side="left")
        ttk.Button(btns, text="Пропусни", command=self._on_skip).pack(side="left", padx=(8, 0))
        ttk.Button(btns, text="Отказ", command=self._on_cancel).pack(side="right")

        self.listbox.bind("<<ListboxSelect>>", self._on_listbox_change)
        self.listbox.bind("<Double-Button-1>", lambda _e: self._on_select())
        self.bind("<Return>", lambda _e: self._on_select())
        self.bind("<Escape>", lambda _e: self._on_cancel())

    def _on_listbox_change(self, _evt: tk.Event) -> None:
        if self.listbox.curselection():
            self.select_btn.state(["!disabled"])
        else:
            self.select_btn.state(["disabled"])

    def _on_select(self) -> None:
        selection = self.listbox.curselection()
        if not selection:
            return
        self.result = int(selection[0])
        self.destroy()

    def _on_skip(self) -> None:
        self.result = "skip"
        self.destroy()

    def _on_cancel(self) -> None:
        self.result = "cancel"
        self.destroy()

    def show(self) -> Optional[int | str]:
        self.wait_window()
        return self.result


class UserSelectionDialog(tk.Toplevel):
    """Диалог за избор на потребител при вход само с парола."""

    def __init__(self, parent: tk.Tk, users: List[Dict[str, Any]]) -> None:
        super().__init__(parent)
        self.result: Optional[Dict[str, Any]] = None
        self._users = list(users)
        self.title("Избор на потребител")
        self.transient(parent)
        self.grab_set()
        self.resizable(False, False)
        self.protocol("WM_DELETE_WINDOW", self._on_cancel)

        frame = ttk.Frame(self, padding=12)
        frame.pack(fill="both", expand=True)

        ttk.Label(frame, text="Намерени са няколко потребителя.").pack(anchor="w")
        ttk.Label(frame, text="Моля, изберете един за вход:").pack(anchor="w", pady=(0, 6))

        self.listbox = tk.Listbox(frame, height=min(8, len(self._users)) or 4, width=40, exportselection=False)
        self.listbox.pack(fill="both", expand=True, pady=(0, 8))
        for user in self._users:
            display = f"{user.get('name', '')} (ID: {user.get('id', '')})"
            self.listbox.insert(tk.END, display.strip())

        buttons = ttk.Frame(frame)
        buttons.pack(fill="x")
        self.ok_btn = ttk.Button(buttons, text="OK", command=self._on_confirm)
        self.ok_btn.pack(side="left")
        ttk.Button(buttons, text="Отказ", command=self._on_cancel).pack(side="right")

        self.listbox.bind("<Double-Button-1>", lambda _e: self._on_confirm())
        self.listbox.bind("<<ListboxSelect>>", self._on_select_change)
        self.bind("<Return>", lambda _e: self._on_confirm())
        self.bind("<Escape>", lambda _e: self._on_cancel())

        if self._users:
            self.listbox.selection_set(0)
        self._update_button_state()
        self.listbox.focus_set()

    def _on_select_change(self, _event: tk.Event) -> None:
        self._update_button_state()

    def _update_button_state(self) -> None:
        if self.listbox.curselection():
            self.ok_btn.state(["!disabled"])
        else:
            self.ok_btn.state(["disabled"])

    def _on_confirm(self) -> None:
        selection = self.listbox.curselection()
        if not selection:
            return
        idx = int(selection[0])
        if 0 <= idx < len(self._users):
            self.result = dict(self._users[idx])
        self.destroy()

    def _on_cancel(self) -> None:
        self.result = None
        self.destroy()

    def show(self) -> Optional[Dict[str, Any]]:
        self.wait_window()
        return self.result


class ItemResolverDialog(tk.Toplevel):
    """Интерактивен диалог за избор/създаване на mapping."""

    def __init__(
        self,
        parent: tk.Tk,
        resolver: Optional[db_integration.DbItemResolver],
        description: str,
        barcode: Optional[str],
        initial_hits: List[db_integration.ItemHit],
        default_mapping_kind: Optional[str] = None,
    ) -> None:
        super().__init__(parent)
        self.resolver = resolver
        self._hits: List[db_integration.ItemHit] = list(initial_hits)
        self._manual_hit: Optional[db_integration.ItemHit] = None
        self._default_mapping_kind = default_mapping_kind
        self.result: Optional[Dict[str, Any]] = None

        self.title("Избор на материал")
        self.transient(parent)
        self.grab_set()
        self.resizable(False, False)
        self.protocol("WM_DELETE_WINDOW", self._on_cancel)

        frame = ttk.Frame(self, padding=12)
        frame.pack(fill="both", expand=True)

        info_text = description or "(без описание)"
        ttk.Label(frame, text="Описание от фактурата:", font=("Segoe UI", 9, "bold")).pack(anchor="w")
        descr_box = tk.Text(frame, height=3, width=60, wrap="word", relief="groove", borderwidth=1)
        descr_box.pack(fill="x", pady=(0, 8))
        descr_box.insert("1.0", info_text)
        descr_box.configure(state="disabled")

        if barcode:
            ttk.Label(frame, text=f"Баркод: {barcode}", foreground="#555").pack(anchor="w", pady=(0, 6))

        search_row = ttk.Frame(frame)
        search_row.pack(fill="x")
        ttk.Label(search_row, text="Търси по име:").pack(side="left")
        self.search_var = tk.StringVar(value=description or "")
        self.search_entry = ttk.Entry(search_row, textvariable=self.search_var, width=40)
        self.search_entry.pack(side="left", padx=(4, 4))
        self.search_entry.bind("<Return>", self._on_search)
        self.search_btn = ttk.Button(search_row, text="Търси", command=self._on_search)
        self.search_btn.pack(side="left")

        ttk.Label(frame, text="Резултати:").pack(anchor="w", pady=(8, 0))
        self.listbox = tk.Listbox(frame, height=10, width=60, exportselection=False)
        self.listbox.pack(fill="both", expand=True, pady=(4, 8))
        self.listbox.bind("<<ListboxSelect>>", self._on_selection_change)
        self.listbox.bind("<Double-Button-1>", self._on_confirm)

        manual_row = ttk.Frame(frame)
        manual_row.pack(fill="x", pady=(4, 0))
        ttk.Label(manual_row, text="Ръчно въведен код:").pack(side="left")
        self.manual_var = tk.StringVar()
        self.manual_entry = ttk.Entry(manual_row, textvariable=self.manual_var, width=18)
        self.manual_entry.pack(side="left", padx=(4, 4))
        self.manual_btn = ttk.Button(manual_row, text="Провери код", command=self._on_check_manual)
        self.manual_btn.pack(side="left")

        self.status_var = tk.StringVar()
        ttk.Label(frame, textvariable=self.status_var, foreground="#555").pack(anchor="w", pady=(4, 4))

        check_text = "Запази mapping за този баркод" if barcode else "Запази mapping по текст"
        self.save_var = tk.BooleanVar(value=bool(default_mapping_kind))
        self.save_checkbox = ttk.Checkbutton(frame, text=check_text, variable=self.save_var)
        self.save_checkbox.pack(anchor="w", pady=(0, 6))

        buttons = ttk.Frame(frame)
        buttons.pack(fill="x")
        self.ok_btn = ttk.Button(buttons, text="Избери", command=self._on_confirm)
        self.ok_btn.pack(side="left")
        ttk.Button(buttons, text="Отказ", command=self._on_cancel).pack(side="right")

        if resolver is None:
            self.search_btn.state(["disabled"])
            self.manual_btn.state(["disabled"])
            self.status_var.set("Няма връзка с БД – наличен е само ръчен избор.")

        self._populate_hits(self._hits)
        if self._hits:
            self.listbox.selection_set(0)
        self.search_entry.focus_set()

    # ----------------- helpers -----------------
    def _populate_hits(self, hits: List[db_integration.ItemHit]) -> None:
        self.listbox.delete(0, tk.END)
        for hit in hits:
            label = f"{hit['code']} | {hit['name']}"
            self.listbox.insert(tk.END, label)
        if not hits:
            self.status_var.set("Няма резултати.")

    def _on_selection_change(self, _event: Any = None) -> None:
        self._manual_hit = None
        if self.listbox.curselection():
            self.ok_btn.state(["!disabled"])
        else:
            self.ok_btn.state(["disabled"])

    def _on_search(self, _event: Any = None) -> None:
        if self.resolver is None:
            return
        query = self.search_var.get().strip()
        if not query:
            self.status_var.set("Въведете текст за търсене.")
            return
        hits = self.resolver.resolve_by_name(query, limit=20)
        seen: set[str] = set()
        combined: List[db_integration.ItemHit] = []
        for hit in hits:
            if hit["code"] in seen:
                continue
            seen.add(hit["code"])
            combined.append(hit)
        self._hits = combined
        self._populate_hits(combined)
        if combined:
            self.listbox.selection_set(0)
            self.status_var.set(f"Намерени резултати: {len(combined)}")
        else:
            self.status_var.set("Няма резултати.")

    def _on_check_manual(self) -> None:
        if self.resolver is None:
            return
        code = self.manual_var.get().strip()
        if not code:
            self.status_var.set("Въведете код за проверка.")
            return
        hit = self.resolver.ensure_item(code)
        if hit:
            self._manual_hit = hit
            self.status_var.set(f"Кодът е намерен: {hit['name']}")
            self.ok_btn.state(["!disabled"])
        else:
            self._manual_hit = None
            self.status_var.set("Кодът не е намерен.")

    def _on_confirm(self, _event: Any = None) -> None:
        chosen_hit: Optional[db_integration.ItemHit] = None
        mapping_kind = self._default_mapping_kind
        if self._manual_hit is not None:
            chosen_hit = self._manual_hit
            mapping_kind = "barcode" if mapping_kind == "barcode" else "manual"
        else:
            selection = self.listbox.curselection()
            if selection:
                chosen_hit = self._hits[int(selection[0])]
        if chosen_hit is None:
            self.status_var.set("Изберете резултат или проверете код.")
            return
        self.result = {
            "hit": chosen_hit,
            "save_mapping": bool(self.save_var.get()),
            "mapping_kind": mapping_kind or "text",
        }
        self.destroy()

    def _on_cancel(self) -> None:
        self.result = None
        self.destroy()

    def show(self) -> Optional[Dict[str, Any]]:
        self.wait_window()
        return self.result



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

    ensure_clients_file(path)
    try:
        with open(path, "r", encoding="utf-8") as fh:
            data = json.load(fh)
    except FileNotFoundError:
        logger.exception("Файлът {} липсва.", path)
        return {}
    except Exception as exc:  # pragma: no cover
        logger.exception("Неуспешно зареждане на профилите: {}", exc)
        return {}

    profiles: Dict[str, Dict[str, Any]] = {}
    if isinstance(data, dict):
        source = data.get("profiles") if isinstance(data.get("profiles"), dict) else data
        if not isinstance(source, dict):
            source = {}
        for key, value in source.items():
            if isinstance(value, dict):
                profiles[str(key)] = dict(value)
    elif isinstance(data, list):
        for idx, item in enumerate(data):
            if not isinstance(item, dict):
                continue
            name = str(item.get("name") or item.get("client") or f"Профил {idx + 1}")
            profiles[name] = item
    else:
        logger.error("Неочакван формат на {}. Очаква се dict или list.", path)
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
                logger.debug("Неуспешно зареждане на икона от {}", icon_path)

        self.session = SessionState()
        self.session.ui_root = self.root
        self.session.output_logger = self._log
        self.session.select_user_callback = self._choose_user_by_password
        _check_runtime_dependencies()
        self.profiles = load_profiles()
        self.profile_names: List[str] = list(self.profiles.keys())
        self.active_profile: Optional[Dict[str, Any]] = None
        self.active_profile_name: Optional[str] = None
        self.rows_cache: List[Dict[str, Any]] = []
        self.last_login_trace: List[Dict[str, Any]] = []
        self.status_summary_var = tk.StringVar(
            value="Намерени в БД: 0 | чрез mapping: 0 | нерешени: 0"
        )
        self.mapping_store = db_integration.Mapping()

        self._build_ui()
        self.session.db_mode = bool(self.db_mode_var.get())

        self._log("Приложението е стартирано.")
        initial_profile_label = "няма профил"
        if self.profile_names:
            self.profile_cmb.current(0)
            self._apply_profile(self.profile_names[0])
            initial_profile_label = self.active_profile_name or self.profile_names[0]
        else:
            self._log("⚠️ Няма профили в mistral_clients.json.")
        logger.info("Приложението е стартирано. Профил: {}", initial_profile_label)

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
        self.login_status_label = ttk.Label(
            strip,
            textvariable=self.login_status_var,
            foreground="#006400",
        )
        self.login_status_label.grid(row=0, column=7, sticky="w")
        self.login_diag_btn = ttk.Button(
            strip,
            text="Покажи диагностика",
            command=self._show_login_diagnostics,
        )
        self.login_diag_btn.grid(row=0, column=8, sticky="w", padx=(4, 0))
        self.login_diag_btn.state(["disabled"])

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
        self.output_text.configure(font="TkFixedFont")
        yscroll = ttk.Scrollbar(outfrm, command=self.output_text.yview)
        yscroll.pack(side="right", fill="y")
        self.output_text.configure(yscrollcommand=yscroll.set)

        status = ttk.Frame(self.root, padding=(16, 4, 16, 12))
        status.pack(side="bottom", fill="x")
        ttk.Button(status, text="Отвори логове", command=self._on_open_logs).pack(side="left")
        ttk.Label(status, textvariable=self.status_summary_var).pack(side="left", padx=(12, 0))
        ttk.Label(status, textvariable=self.license_var, foreground="#555").pack(side="right")

    def _log(self, *args: Any) -> None:
        message = " ".join(str(arg) for arg in args) if args else ""
        try:
            self.output_text.insert(tk.END, message + "\n")
            self.output_text.see(tk.END)
        except Exception:
            pass
        if message:
            try:
                logger.info(message)
            except Exception:
                pass

    def _on_open_logs(self) -> None:
        log_dir = Path(__file__).resolve().parent / "logs"
        try:
            log_dir.mkdir(parents=True, exist_ok=True)
        except Exception:  # pragma: no cover - защитно
            pass
        try:
            if sys.platform.startswith("win"):
                os.startfile(str(log_dir))  # type: ignore[attr-defined]
            elif sys.platform == "darwin":
                subprocess.Popen(["open", str(log_dir)])
            else:
                subprocess.Popen(["xdg-open", str(log_dir)])
        except Exception as exc:
            logger.exception("Неуспешно отваряне на директорията с логове: {}", exc)
            messagebox.showerror(
                "Логове",
                f"Неуспешно отваряне на {log_dir}.\n{exc}",
            )

    def _report_error(self, message: str, exc: Optional[BaseException] = None) -> None:
        detail = ""
        if exc is not None:
            logger.exception(message)
            detail = str(exc).strip()
        if detail:
            self._log(f"❌ {message}: {detail}")
        else:
            self._log(f"❌ {message}")

    def _toggle_login_diag_button(self, show: bool) -> None:
        if not hasattr(self, "login_diag_btn"):
            return
        try:
            if show:
                self.login_diag_btn.state(["!disabled"])
            else:
                self.login_diag_btn.state(["disabled"])
        except Exception:  # pragma: no cover - защитно
            pass

    def _show_login_diagnostics(self) -> None:
        profile_name = self.active_profile_name or self.session.profile_name
        if not profile_name:
            self._report_error("Моля, изберете профил преди диагностика.")
            return

        username = self.username_var.get().strip() or self.session.username
        stored_password = getattr(self.session, "password", "")
        password = stored_password or self.password_var.get() or ""
        if not password:
            self._log("ℹ️ Диагностиката ще използва празна парола.")

        script_path = Path(__file__).with_name("diag_mistral_auth.py")
        if not script_path.exists():
            self._report_error("Липсва скриптът за диагностика (diag_mistral_auth.py).")
            return

        cmd = [sys.executable, str(script_path), "--profile", profile_name]
        if username:
            cmd.extend(["--user", username])
        if password:
            cmd.extend(["--password", password])
        if os.getenv("MV_FORCE_TABLE_LOGIN", "").strip() == "1":
            cmd.append("--force-table")

        self._log("🔎 Стартирам диагностика на входа…")
        logger.info(
            "Стартирана е диагностика (профил: {}, потребител: {})",
            profile_name,
            username or "<само парола>",
        )

        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
            )
        except Exception as exc:
            self._report_error("Неуспешно стартиране на диагностиката.", exc)
            return

        summary_prefix = "SUMMARY:"
        stdout = result.stdout or ""
        stderr = result.stderr or ""
        summary_lines = [
            line.split(summary_prefix, 1)[1].strip()
            for line in stdout.splitlines()
            if line.startswith(summary_prefix)
        ]

        if result.returncode != 0:
            error_line = stderr.strip().splitlines()[-1] if stderr.strip() else "Неуспешно изпълнение."
            summary_lines.append(f"Диагностиката приключи с код {result.returncode}: {error_line}")

        if not summary_lines:
            summary_lines = [line for line in stdout.splitlines() if line.strip()][:5]
        if not summary_lines:
            summary_lines = ["Няма налично обобщение от диагностиката."]

        diag_fn = getattr(db_integration, "collect_db_diagnostics", None)
        if callable(diag_fn):
            try:
                diag_info = diag_fn(self.session)
                diag_lines: List[str] = []
                status_text = diag_info.get("status")
                if status_text:
                    diag_lines.append(f"Статус: {status_text}")

                login_info = diag_info.get("login") or {}
                if isinstance(login_info, dict):
                    mode = login_info.get("mode")
                    name = login_info.get("name") or login_info.get("table")
                    if mode == "sp":
                        diag_lines.append(
                            f"Логин: процедура {name or '—'} ({login_info.get('sp_kind') or 'неизвестна'})"
                        )
                    elif mode == "table":
                        diag_lines.append(f"Логин: таблица {name or '—'}")
                if diag_info.get("login_error"):
                    diag_lines.append(f"Логин: грешка ({diag_info['login_error']})")

                connection_info = diag_info.get("connection") if isinstance(diag_info, dict) else None
                if isinstance(connection_info, dict):
                    driver_name = diag_info.get("driver") or connection_info.get("driver")
                    if driver_name:
                        diag_lines.append(f"Драйвер: {driver_name}")
                    dsn_value = connection_info.get("dsn")
                    if dsn_value:
                        diag_lines.append(f"DSN: {dsn_value}")
                    else:
                        host_value = connection_info.get("host") or "—"
                        port_value = connection_info.get("port")
                        database_value = connection_info.get("database") or "—"
                        if port_value is not None:
                            diag_lines.append(f"Host: {host_value}:{port_value}")
                        else:
                            diag_lines.append(f"Host: {host_value}")
                        diag_lines.append(f"Database: {database_value}")
                    charset_value = connection_info.get("charset")
                    if charset_value:
                        diag_lines.append(f"Charset: {charset_value}")

                schema_info = diag_info.get("schema") or {}
                if isinstance(schema_info, dict) and schema_info.get("materials_table"):
                    diag_lines.append(
                        "Каталожна таблица: {0} (код={1}, име={2})".format(
                            schema_info.get("materials_table"),
                            schema_info.get("materials_code") or "—",
                            schema_info.get("materials_name") or "—",
                        )
                    )
                if isinstance(schema_info, dict) and schema_info.get("barcode_table"):
                    diag_lines.append(
                        "Таблица баркодове: {0} (колона={1}, FK={2})".format(
                            schema_info.get("barcode_table"),
                            schema_info.get("barcode_col") or "—",
                            schema_info.get("barcode_mat_fk") or "—",
                        )
                    )
                if diag_info.get("schema_error"):
                    diag_lines.append(f"Схема: грешка ({diag_info['schema_error']})")
                materials_count = diag_info.get("materials_count")
                if materials_count is not None:
                    diag_lines.append(f"Материали в БД: {materials_count}")
                elif diag_info.get("materials_error"):
                    diag_lines.append(f"Материали: грешка ({diag_info['materials_error']})")
                barcode_count = diag_info.get("barcode_count")
                if barcode_count is not None:
                    diag_lines.append(f"Баркодове: {barcode_count}")
                elif diag_info.get("barcode_error"):
                    diag_lines.append(f"Баркодове: грешка ({diag_info['barcode_error']})")

                samples_payload = diag_info.get("samples") or {}
                if isinstance(samples_payload, dict):
                    barcode_payload = samples_payload.get("barcode") or {}
                    if barcode_payload.get("value"):
                        material = barcode_payload.get("material") or {}
                        diag_lines.append(
                            "Пример баркод {0} → {1} | {2}".format(
                                barcode_payload.get("value"),
                                material.get("code") or "—",
                                material.get("name") or "без име",
                            )
                        )
                    name_payload = samples_payload.get("name") or {}
                    if name_payload.get("value"):
                        candidates = name_payload.get("candidates") or []
                        first_candidate = candidates[0] if candidates else {}
                        diag_lines.append(
                            "Пример име '{0}' → {1}".format(
                                name_payload.get("value"),
                                first_candidate.get("code") or "—",
                            )
                        )

                errors_list = diag_info.get("errors") or []
                for error_item in errors_list:
                    diag_lines.append(f"⚠️ {error_item}")

                summary_lines.append("--- DB диагностика ---")
                summary_lines.extend(diag_lines or ["Няма налични данни за диагностика на БД."])
            except Exception as exc:
                summary_lines.append(f"DB диагностика: неуспешно ({exc})")

        self._log("📋 Обобщение от диагностика:")
        for item in summary_lines:
            self._log(f"  • {item}")

        dialog = tk.Toplevel(self.root)
        dialog.title("Диагностика на входа")
        dialog.transient(self.root)
        dialog.grab_set()
        dialog.resizable(True, False)

        frame = ttk.Frame(dialog, padding=12)
        frame.pack(fill="both", expand=True)

        text = tk.Text(frame, width=80, height=len(summary_lines) + 2, wrap="word")
        text.pack(fill="both", expand=True)
        text.configure(font="TkFixedFont")
        text.insert("1.0", "\n".join(summary_lines))
        text.configure(state="disabled")

        ttk.Button(dialog, text="Затвори", command=dialog.destroy).pack(pady=(6, 0))
        dialog.bind("<Escape>", lambda _e: dialog.destroy())

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
        self.session.last_login_trace = None
        self.session.password = ""
        self.session.unresolved_items = []
        self.last_login_trace = []
        self.username_var.set("")
        self.password_var.set("")
        self.login_status_var.set("Вход: няма активна сесия.")
        self._toggle_login_diag_button(False)

    def _choose_user_by_password(
        self, candidates: List[Dict[str, Any]]
    ) -> Optional[Dict[str, Any]]:
        if not candidates:
            return None
        dialog = UserSelectionDialog(self.root, candidates)
        result = dialog.show()
        if result is None:
            try:
                messagebox.showinfo("Вход", "Входът е прекъснат.")
            except Exception:
                pass
            return None
        return result

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

        if isinstance(result, dict) and result.get("error"):
            message = str(result.get("error"))
            trace = result.get("trace") or db_integration.last_login_trace(self.session)
            self.last_login_trace = trace or []
            self.session.last_login_trace = self.last_login_trace
            self.session.password = ""
            self.login_status_var.set(f"Вход: неуспешен – {message}")
            try:
                self.login_status_label.configure(foreground="#8B0000")
            except Exception:
                pass
            self._log(f"❌ {message}")
            self._toggle_login_diag_button(True)
            try:
                messagebox.showerror("Вход", message)
            except Exception:
                pass
            return
        if not result:
            self.login_status_var.set("Вход: неуспешен – Невалидни данни за вход.")
            try:
                self.login_status_label.configure(foreground="#8B0000")
            except Exception:
                pass
            self._log("❌ Невалидни данни за вход.")
            return

        login_name = ""
        if isinstance(result, dict):
            login_name = str(result.get("login") or "").strip()

        user_id = self._extract_user_id(result)
        effective_username = login_name or username
        self.session.username = effective_username
        self.session.user_id = user_id
        self.session.raw_login_payload = result
        self.last_login_trace = db_integration.last_login_trace(self.session)
        self.session.last_login_trace = self.last_login_trace
        self.session.password = password

        display_user = effective_username or ("само парола" if not username else username)
        suffix = f" (ID: {user_id})" if user_id is not None else ""
        self.login_status_var.set("Вход: успешен.")
        try:
            self.login_status_label.configure(foreground="#006400")
        except Exception:
            pass
        self._log(f"✅ Успешен вход: {display_user}{suffix}")
        self.password_var.set("")
        self._toggle_login_diag_button(True)
        self._refresh_license_text()
        if self.session.catalog_loaded:
            materials_preview = getattr(self.session, "materials_preview", []) or []
            barcodes_preview = getattr(self.session, "barcodes_preview", []) or []
            self._log(
                f"📚 Заредени каталожни данни: материали={len(materials_preview)} | баркодове={len(barcodes_preview)}"
            )
        else:
            self._log(
                "⚠️ Няма заредена таблица с материали – автоматичните съвпадения са ограничени."
            )

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
            extractor = import_module("extract_and_prepare")
        except Exception as exc:
            self._report_error("Липсва модулът за обработка на документи.", exc)
            return

        main_fn = getattr(extractor, "main", None)
        if not callable(main_fn):
            self._log("⚠️ Модулът extract_and_prepare няма функция main.")
            return

        try:
            rows = main_fn(file_path, gui_mode=True)  # type: ignore[arg-type]
        except TypeError:
            rows = main_fn(file_path)  # type: ignore[arg-type]
        except Exception as exc:
            self._report_error("Възникна грешка при обработката на файла.", exc)
            return

        if rows is None:
            self._log("⚠️ Няма върнати редове от обработката.")
            return
        if not isinstance(rows, list):
            self._log("⚠️ Върнатият резултат не е списък с редове.")
            return

        if not self._resolve_rows(rows):
            self._update_status_summary(rows)
            return

        self.rows_cache = rows
        count = len(rows)
        if count == 0:
            self._log("ℹ️ Няма разпознати редове в документа.")
            self._update_status_summary(rows)
            return

        self._update_status_summary(rows)
        self._log(f"✅ Разпознати редове: {count}")
        self._preview_rows(rows)

        if self.session.db_mode:
            self._push_to_open_delivery(self.rows_cache)
            self._update_status_summary(self.rows_cache)

        final_items = [row.get("final_item") for row in self.rows_cache if row.get("final_item")]

        if final_items:
            self._offer_export(final_items, file_path)
        else:
            self._log("⚠️ Няма потвърдени артикули за експорт/доставка.")

    def _update_status_summary(self, rows: List[Dict[str, Any]]) -> None:
        db_count = 0
        mapping_count = 0
        manual_count = 0
        for row in rows:
            final = row.get("final_item") or {}
            source = final.get("source")
            if source in {"db-barcode", "db-text"}:
                db_count += 1
            elif source in {"mapping-barcode", "mapping-text"}:
                mapping_count += 1
            elif source == "manual":
                manual_count += 1
        unresolved = max(len(rows) - db_count - mapping_count - manual_count, 0)
        summary = (
            f"Намерени в БД: {db_count} | чрез mapping: {mapping_count} | ръчни: {manual_count} | нерешени: {unresolved}"
        )
        self.status_summary_var.set(summary)

    def _determine_supplier_key(self, row: Optional[Dict[str, Any]] = None) -> str:
        profile = self.session.profile_data or {}
        supplier = None
        if isinstance(row, dict):
            for key in ("supplier_id", "supplier", "issuer", "issuer_id"):
                value = row.get(key)
                if value not in (None, ""):
                    supplier = value
                    break
        if supplier in (None, ""):
            supplier = profile.get("default_supplier_id")
        if supplier in (None, ""):
            supplier = profile.get("name") or self.active_profile_name or "DEFAULT"
        return str(supplier)

    @staticmethod
    def _row_first(row: Dict[str, Any], keys: Iterable[str]) -> Optional[str]:
        for key in keys:
            if key not in row:
                continue
            value = row.get(key)
            if isinstance(value, str):
                cleaned = value.strip()
                if cleaned:
                    return cleaned
            elif value not in (None, ""):
                return str(value)
        return None

    def _row_token(self, row: Dict[str, Any]) -> str:
        token = row.get("token")
        if isinstance(token, str) and token.strip():
            return token.strip()
        candidate = self._row_first(
            row,
            ("description", "name", "raw", "product", "item_name", "Наименование", "Описание"),
        )
        return candidate or ""

    def _apply_hit(
        self,
        row: Dict[str, Any],
        hit: db_integration.ItemHit,
        source: str,
        match_kind: str,
        barcode: Optional[str],
    ) -> None:
        candidate = {
            "id": None,
            "code": hit["code"],
            "name": hit["name"],
            "barcode": barcode,
            "match": match_kind,
            "source": source,
        }
        db_integration.apply_candidate_choice(row, candidate, source)

    def _prompt_manual_code(
        self,
        index: int,
        row: Dict[str, Any],
        mapping: db_integration.Mapping,
        supplier_key: str,
        description: Optional[str],
        token: str,
        barcode: Optional[str],
        resolver: Optional[db_integration.DbItemResolver],
    ) -> Optional[str]:
        prompt_text = description or token or row.get("name") or row.get("description") or ""
        initial_code = (
            row.get("code")
            or row.get("Номер")
            or row.get("item_code")
            or row.get("Артикул")
            or ""
        )
        while True:
            try:
                manual_code = simpledialog.askstring(
                    "Ръчно въвеждане на материал",
                    (
                        "Въведете материален код за ред {idx}.\n" "Описание: {text}"
                    ).format(idx=index, text=prompt_text or "(без описание)"),
                    parent=self.root,
                    initialvalue=str(initial_code) if initial_code else None,
                )
            except Exception:
                manual_code = None
            if manual_code is None:
                return None
            code = manual_code.strip()
            if not code:
                try:
                    messagebox.showwarning(
                        "Ръчно въвеждане",
                        "Моля, въведете валиден MATERIALCODE.",
                    )
                except Exception:
                    pass
                continue
            hit: Optional[db_integration.ItemHit] = None
            if resolver is not None:
                try:
                    hit = resolver.ensure_item(code)
                except Exception as exc:
                    self._log(f"⚠️ Ред {index}: проверката на код {code} е неуспешна: {exc}")
            if resolver is not None and not hit:
                retry = False
                try:
                    retry = messagebox.askretrycancel(
                        "Ръчно въвеждане",
                        f"Код {code} не беше намерен в каталога. Опитайте отново?",
                    )
                except Exception:
                    retry = False
                if retry:
                    continue
                return None
            if hit is None:
                name = (
                    description
                    or row.get("name")
                    or row.get("description")
                    or token
                    or code
                )
                hit = {"code": code, "name": name}
            self._apply_hit(row, hit, "manual", "manual", barcode)
            if barcode:
                mapping.set_mapped_barcode(supplier_key, barcode, hit["code"])
            key_text = description or token
            if key_text:
                mapping.set_mapped_text(supplier_key, key_text, hit["code"])
            self._log(f"✅ Ред {index}: ръчно зададен материал {hit['code']}")
            return "manual"

    def _resolve_rows(self, rows: List[Dict[str, Any]]) -> bool:
        mapping = self.mapping_store
        supplier_key = self._determine_supplier_key()
        resolver: Optional[db_integration.DbItemResolver] = None
        cur = getattr(self.session, "cur", None)
        if cur is not None:
            try:
                resolver = db_integration.DbItemResolver(cur)
                self.session.catalog = resolver.catalog
                catalog = resolver.catalog
                code_id = catalog.get("code_id_col") or "MATERIALCODE"
                code_col = catalog.get("code_col") or "CODE"
                fk_col = catalog.get("fk_col") or "FK_STORAGEMATERIALCODE"
                self._log(
                    f"Каталожна схема: MATERIAL({code_id}) ↔ BARCODE(code={code_col}, fk={fk_col})"
                )
            except db_integration.MistralDBError as exc:
                self._log(f"⚠️ Схема неразпозната: {exc}")
                messagebox.showerror("Схема", f"Каталогът не може да бъде детектиран: {exc}")
                return False

        stats = {"mapping": 0, "db": 0, "manual": 0, "unresolved": 0}
        unresolved_entries: List[Dict[str, Any]] = []

        for index, row in enumerate(rows, start=1):
            outcome = self._resolve_single_row(index, row, resolver, mapping, supplier_key)
            if outcome == "cancel":
                self._log("⚠️ Обработката е прекъсната от потребителя.")
                self.session.unresolved_items = unresolved_entries
                return False
            if outcome is None:
                stats["unresolved"] += 1
                unresolved_entries.append(
                    {
                        "token": self._row_token(row),
                        "barcode": self._row_first(row, ("barcode", "Баркод", "EAN")),
                        "name": self._row_first(row, ("name", "description", "Наименование")),
                    }
                )
            elif outcome in stats:
                stats[outcome] += 1

        self.session.unresolved_items = unresolved_entries
        self.session.last_resolution_stats = stats
        if unresolved_entries:
            preview = ", ".join(
                filter(
                    None,
                    [
                        (entry.get("token") or entry.get("name") or entry.get("barcode") or "?")
                        for entry in unresolved_entries[:3]
                    ],
                )
            )
            suffix = f" ({preview})" if preview else ""
            self._log(
                f"📝 Нерешени редове за последваща обработка: {len(unresolved_entries)}{suffix}"
            )
        return True

    def _resolve_single_row(
        self,
        index: int,
        row: Dict[str, Any],
        resolver: Optional[db_integration.DbItemResolver],
        mapping: db_integration.Mapping,
        supplier_key: str,
    ) -> Optional[str]:
        row["resolved"] = None
        row["final_item"] = None
        token = self._row_token(row)
        row["token"] = token
        barcode = self._row_first(row, ("barcode", "Баркод", "EAN", "ean", "Barcode"))
        description = self._row_first(
            row,
            (
                "description",
                "name",
                "product",
                "item_name",
                "Наименование",
                "Описание",
                "raw",
            ),
        ) or token

        def _log_choice(hit: db_integration.ItemHit, source: str) -> None:
            logger.info(
                "Lookup: {} → код={} | име={}",
                source,
                hit["code"],
                hit["name"],
            )

        def _safe_call(func: Callable[..., Any], *args: Any) -> Any:
            try:
                return func(*args)
            except Exception as exc:
                logger.error("DB lookup error: {}", exc)
                self._log(f"⚠️ БД грешка: {exc}")
                return None

        # Mapping по баркод
        if barcode:
            mapped_code = mapping.get_mapped_barcode(supplier_key, barcode)
            if mapped_code:
                hit = _safe_call(resolver.ensure_item, mapped_code) if resolver else None
                if hit:
                    self._apply_hit(row, hit, "mapping-barcode", "barcode", barcode)
                    self._log(f"✅ Ред {index}: mapping по баркод → {hit['code']}")
                    _log_choice(hit, "mapping-barcode")
                    return "mapping"
                else:
                    self._log(
                        f"⚠️ Ред {index}: mapping по баркод е невалиден за код {mapped_code}."
                    )

        # Mapping по текст
        if description:
            mapped_code = mapping.get_mapped_text(supplier_key, description)
            if mapped_code:
                hit = _safe_call(resolver.ensure_item, mapped_code) if resolver else None
                if hit:
                    self._apply_hit(row, hit, "mapping-text", "text", barcode)
                    self._log(f"✅ Ред {index}: mapping по текст → {hit['code']}")
                    _log_choice(hit, "mapping-text")
                    return "mapping"
                else:
                    self._log(
                        f"⚠️ Ред {index}: mapping по текст е невалиден за код {mapped_code}."
                    )

        if resolver is None:
            return self._prompt_manual_code(
                index,
                row,
                mapping,
                supplier_key,
                description,
                token,
                barcode,
                None,
            )

        hits: List[db_integration.ItemHit] = []
        mapping_kind: Optional[str] = None
        barcode_hit = _safe_call(resolver.resolve_by_barcode, barcode or "") if barcode else None
        if barcode_hit:
            hits.append(barcode_hit)
            mapping_kind = "barcode"
        name_hits = (
            _safe_call(resolver.resolve_by_name, description or token or "")
            if (description or token)
            else []
        ) or []
        seen_codes = {hit["code"] for hit in hits}
        for hit in name_hits:
            if hit["code"] in seen_codes:
                continue
            seen_codes.add(hit["code"])
            hits.append(hit)
        if not hits:
            return self._prompt_manual_code(
                index,
                row,
                mapping,
                supplier_key,
                description,
                token,
                barcode,
                resolver,
            )

        dialog = ItemResolverDialog(
            self.root,
            resolver,
            description or token or "",
            barcode,
            hits,
            mapping_kind or ("text" if description else None),
        )
        choice = dialog.show()
        if choice is None:
            return "cancel"
        hit = choice.get("hit") if isinstance(choice, dict) else None
        if not hit:
            return None
        mapping_type = choice.get("mapping_kind") or "text"
        save_mapping = bool(choice.get("save_mapping"))
        if mapping_type == "barcode" and barcode:
            self._apply_hit(row, hit, "db-barcode", "barcode", barcode)
            source_key = "db"
            if save_mapping:
                mapping.set_mapped_barcode(supplier_key, barcode, hit["code"])
        elif mapping_type == "manual":
            self._apply_hit(row, hit, "manual", "manual", barcode)
            source_key = "manual"
            if barcode:
                mapping.set_mapped_barcode(supplier_key, barcode, hit["code"])
            key_text = description or token
            if key_text:
                mapping.set_mapped_text(supplier_key, key_text, hit["code"])
        else:
            self._apply_hit(row, hit, "db-text", "text", barcode)
            source_key = "db"
            if save_mapping and description:
                mapping.set_mapped_text(supplier_key, description, hit["code"])
        self._log(f"✅ Ред {index}: избран материал {hit['code']}")
        _log_choice(hit, row.get("final_item", {}).get("source") or source_key)
        return source_key

    def _preview_rows(self, rows: List[Dict[str, Any]]) -> None:
        preview_count = min(5, len(rows))
        for idx in range(preview_count):
            row = rows[idx] or {}
            final = row.get("final_item") or {}
            code = final.get("code") or row.get("code") or row.get("Номер") or row.get("item_code")
            name = final.get("name") or row.get("name") or row.get("Име") or row.get("description")
            qty = final.get("qty") or row.get("qty") or row.get("quantity") or row.get("Количество")
            self._log(f"  • {code or '—'} | {name or 'без име'} | количество: {qty if qty is not None else '?'}")
        if len(rows) > preview_count:
            self._log(f"  … още {len(rows) - preview_count} реда.")

    def _push_to_open_delivery(self, rows: List[Dict[str, Any]]) -> None:
        start_fn = getattr(db_integration, "start_open_delivery", None)
        push_fn = getattr(db_integration, "push_parsed_rows", None)
        if not (callable(start_fn) and callable(push_fn)):
            self._log("⚠️ DB режим е активен, но липсват функции за отворена доставка.")
            return

        if os.getenv("MV_ENABLE_OPEN_DELIVERY", "").strip() != "1":
            self._log("ℹ️ DB режим е в демонстрационен режим – няма да бъдат записани INSERT заявки.")

        try:
            start_fn(self.session)
            push_fn(self.session, rows)
            if os.getenv("MV_ENABLE_OPEN_DELIVERY", "").strip() == "1":
                self._log("✅ Данните са изпратени към отворена доставка.")
            else:
                self._log("ℹ️ Данните са обработени, но не са записани в Мистрал (скелет режим).")
            stats = getattr(self.session, "last_push_stats", None)
            if isinstance(stats, dict):
                total = stats.get("total", 0)
                resolved = stats.get("resolved", 0)
                unresolved = stats.get("unresolved", 0)
                manual = stats.get("manual", 0)
                self._log(
                    f"📦 Статистика: общо {total} | записани {resolved} | нерешени {unresolved} | ръчни избори {manual}"
                )
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
        license_file = Path(__file__).with_name("license.json")
        validator = None
        try:
            from license_utils import validate_license as _validate_license  # type: ignore

            validator = _validate_license
        except ImportError:
            validator = None
        except Exception as exc:  # pragma: no cover - защитно
            logger.warning("Неуспешно зареждане на license_utils: {}", exc)
            validator = None

        if validator is not None:
            try:
                try:
                    validation_result = validator(str(license_file))
                except TypeError:
                    validation_result = validator()
            except Exception as exc:
                logger.exception("Грешка при validate_license: {}", exc)
                self.license_var.set("Лиценз: проверка недостъпна")
                return

            days_remaining: Optional[int] = None
            valid_flag: Optional[bool] = None

            if isinstance(validation_result, dict):
                for key in ("days_remaining", "remaining_days", "days_left"):
                    if key in validation_result:
                        try:
                            days_remaining = int(validation_result[key])
                        except (TypeError, ValueError):
                            days_remaining = None
                        break
                valid_flag = validation_result.get("valid")
            elif isinstance(validation_result, (tuple, list)):
                for item in validation_result:
                    if isinstance(item, (int, float)):
                        days_remaining = int(item)
                    elif isinstance(item, bool) and valid_flag is None:
                        valid_flag = item
            elif isinstance(validation_result, (int, float)):
                days_remaining = int(validation_result)
            elif isinstance(validation_result, bool):
                valid_flag = validation_result

            if days_remaining is not None:
                if days_remaining < 0:
                    self.license_var.set("Лиценз: изтекъл")
                else:
                    self.license_var.set(f"Лиценз: оставащи {days_remaining} дни")
                return
            if valid_flag is True:
                self.license_var.set("Лиценз: оставащи ? дни")
                return
            if valid_flag is False:
                self.license_var.set("Лиценз: изтекъл")
                return
            logger.warning(
                "validate_license върна неочаквани данни: {!r}",
                validation_result,
            )

        if not license_file.exists():
            self.license_var.set("Лиценз: проверка недостъпна")
            logger.warning("Лиценз файлът липсва: {}", license_file)
            return

        try:
            with license_file.open("r", encoding="utf-8") as fh:
                data = json.load(fh)
        except Exception as exc:
            logger.exception("Грешка при прочитане на лиценз файла: {}", exc)
            self.license_var.set("Лиценз: проверка недостъпна")
            return

        valid_until = data.get("valid_until")
        if not valid_until:
            self.license_var.set("Лиценз: проверка недостъпна")
            return

        try:
            expiry = datetime.fromisoformat(str(valid_until)).date()
        except ValueError:
            self.license_var.set("Лиценз: проверка недостъпна")
            return

        today = datetime.now().date()
        remaining = (expiry - today).days
        if remaining < 0:
            self.license_var.set("Лиценз: изтекъл")
        else:
            self.license_var.set(f"Лиценз: оставащи {remaining} дни")


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
