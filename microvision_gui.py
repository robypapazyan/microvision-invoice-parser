# microvision_gui.py
# -*- coding: utf-8 -*-

import json
import os
import sys
import hashlib
import tkinter as tk
from tkinter import ttk, filedialog, messagebox

# ВАЖНО: тук държим само login функцията; push ще се импортне локално когато потрябва
from db_integration import operator_login_session


APP_TITLE = "MicroVision Invoice Parser"
APP_SUBTITLE = "Продукт на Микро Вижън ЕООД | тел. 0883766674 | www.microvision.bg"

CLIENTS_JSON = "mistral_clients.json"


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


def load_profiles(path=CLIENTS_JSON):
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, list):
            raise ValueError("mistral_clients.json трябва да е списък от профили.")
        return data
    except Exception as e:
        messagebox.showerror("Грешка", f"Не мога да заредя {CLIENTS_JSON}:\n{e}")
        return []


# -------------------------
# Login диалог
# -------------------------
class LoginDialog(tk.Toplevel):
    """
    Диалог за вход:
      - поле за Потребител (не е задължително)
      - поле за Парола (задължително)
    Връща: (login, password) – login може да е "" за режим "само парола".
    """

    def __init__(self, master, title="Вход (оператор)"):
        super().__init__(master)
        self.title(title)
        self.resizable(False, False)
        self.transient(master)
        self.grab_set()

        self.login_var = tk.StringVar(value="")
        self.password_var = tk.StringVar(value="")

        frm = ttk.Frame(self, padding=12)
        frm.grid(row=0, column=0, sticky="nsew")

        ttk.Label(frm, text="Потребител (логин):").grid(row=0, column=0, sticky="w", pady=(0, 6))
        e_user = ttk.Entry(frm, textvariable=self.login_var, width=32)
        e_user.grid(row=0, column=1, sticky="we", pady=(0, 6))

        ttk.Label(frm, text="Парола:").grid(row=1, column=0, sticky="w")
        e_pwd = ttk.Entry(frm, textvariable=self.password_var, show="•", width=32)
        e_pwd.grid(row=1, column=1, sticky="we")

        btns = ttk.Frame(frm)
        btns.grid(row=2, column=0, columnspan=2, pady=(12, 0))
        ttk.Button(btns, text="Вход", command=self.ok).grid(row=0, column=0, padx=(0, 8))
        ttk.Button(btns, text="Отказ", command=self.cancel).grid(row=0, column=1)

        self.bind("<Return>", lambda e: self.ok())
        self.bind("<Escape>", lambda e: self.cancel())

        self.result = None
        e_pwd.focus_set()

    def ok(self):
        login = self.login_var.get().strip()
        pwd = self.password_var.get()
        if not pwd:
            messagebox.showerror("Вход", "Моля въведете парола.")
            return
        self.result = (login, pwd)  # ВАЖНО: връщаме кортеж
        self.destroy()

    def cancel(self):
        self.result = None
        self.destroy()


# -------------------------
# Основно приложение
# -------------------------
class MicroVisionApp:
    def __init__(self, root):
        self.root = root
        self.root.title(APP_TITLE)
        self.root.minsize(860, 520)

        # internal state
        self.profiles = load_profiles()
        self.active_profile = None
        self.selected_file = None
        self.operator_user_id = None
        self.operator_login_session = None  # текстов indication

        # top banner
        banner = ttk.Frame(root, padding=(16, 16, 16, 4))
        banner.pack(side="top", fill="x")

        title = ttk.Label(banner, text="MICRO VISION", font=("Segoe UI", 20, "bold"))
        subtitle = ttk.Label(banner, text=APP_TITLE, font=("Segoe UI", 12))
        title.grid(row=0, column=0, sticky="w")
        subtitle.grid(row=1, column=0, sticky="w")
        ttk.Label(banner, text="Оставащи дни по лиценз: —", foreground="#555").grid(row=0, column=1, rowspan=2, sticky="e", padx=(24, 0))

        # controls strip
        strip = ttk.Frame(root, padding=(16, 8))
        strip.pack(side="top", fill="x")

        # Профил
        ttk.Label(strip, text="Профил:").grid(row=0, column=0, sticky="w")
        self.profile_cmb = ttk.Combobox(strip, state="readonly", width=32, values=[p.get("name", f"Profile#{i+1}") for i, p in enumerate(self.profiles)])
        self.profile_cmb.grid(row=0, column=1, sticky="w", padx=(4, 16))
        self.profile_cmb.bind("<<ComboboxSelected>>", self._on_profile_change)

        # език
        ttk.Label(strip, text="Език / Language:").grid(row=0, column=2, sticky="w")
        self.lang_var = tk.StringVar(value="BG")
        ttk.Radiobutton(strip, text="BG", variable=self.lang_var, value="BG").grid(row=0, column=3, padx=(4, 0))
        ttk.Radiobutton(strip, text="EN", variable=self.lang_var, value="EN").grid(row=0, column=4)

        # чекбокс за отворена доставка
        self.open_delivery_var = tk.BooleanVar(value=True)
        ttk.Checkbutton(strip, variable=self.open_delivery_var, text="Вкарвай директно в Мистрал (OPEN доставка)").grid(row=0, column=5, padx=(12, 12))

        # бутон за Machine ID
        ttk.Button(strip, text="Вземи ID на компютъра", command=self._on_get_machine_id).grid(row=0, column=6)

        # second strip (file + start)
        strip2 = ttk.Frame(root, padding=(16, 0, 16, 8))
        strip2.pack(side="top", fill="x")
        ttk.Button(strip2, text="Избери файл / Select file", command=self._on_pick_file).grid(row=0, column=0, padx=(0, 6))
        ttk.Button(strip2, text="Стартирай / Start", command=self._on_start_clicked).grid(row=0, column=1)

        # output
        outfrm = ttk.Frame(root, padding=(16, 0, 16, 16))
        outfrm.pack(side="top", fill="both", expand=True)
        self.output_text = tk.Text(outfrm, height=16, wrap="word")
        self.output_text.pack(side="left", fill="both", expand=True)
        yscroll = ttk.Scrollbar(outfrm, command=self.output_text.yview)
        yscroll.pack(side="right", fill="y")
        self.output_text.configure(yscrollcommand=yscroll.set)

        # init profile
        if self.profiles:
            self.profile_cmb.current(0)
            self._apply_profile(0)
        else:
            self._log("⚠️ Няма профили в mistral_clients.json")

        self._log("Профил зареден: " + (self.active_profile.get("name", "—") if self.active_profile else "—"))

        # при стартиране – вход
        self.root.after(50, self._operator_login_flow)

    # ----------------- UI helpers -----------------

    def _log(self, text):
        self.output_text.insert(tk.END, text + "\n")
        self.output_text.see(tk.END)

    def _on_profile_change(self, _evt=None):
        idx = self.profile_cmb.current()
        self._apply_profile(idx)
        self._log("Профил зареден: " + self.active_profile.get("name", "—"))
        # при смяна на профил нулираме login-а
        self.operator_user_id = None
        self.operator_login_session = None
        self._operator_login_flow()

    def _apply_profile(self, idx):
        try:
            self.active_profile = self.profiles[idx]
        except Exception:
            self.active_profile = None

    def _on_get_machine_id(self):
        mid = machine_id()
        self._log(f"Machine ID: {mid}")
        messagebox.showinfo("ID на компютъра", mid)

    def _on_pick_file(self):
        fn = filedialog.askopenfilename(
            title="Избор на документ",
            filetypes=[("PDF", "*.pdf"), ("Всички", "*.*")]
        )
        if fn:
            self.selected_file = fn
            self._log(f"Избран файл: {fn}")

    # ----------------- Login flow -----------------

    def _operator_login_flow(self):
        """
        Логика на входа:
          - ако login е празен -> password-only режим (уникална парола в таблицата)
          - иначе -> login + password
        """
        if not self.active_profile:
            messagebox.showerror("Грешка", "Няма активен профил.")
            return

        dlg = LoginDialog(self.root)
        self.root.wait_window(dlg)

        if not getattr(dlg, "result", None):
            self._log("Входът е отказан.")
            # не затваряме приложението насила, но без вход не пускаме старт
            return

        login, pwd = dlg.result
        login_arg = login if login else None

        try:
            uid = operator_login_session(self.active_profile, login_arg, pwd)
        except Exception as e:
            messagebox.showerror("Вход неуспешен", f"Грешка при проверка на потребителя:\n{e}")
            return

        if not uid:
            msg = "Невалиден потребител или парола."
            if not login_arg:
                msg = "Паролата не е уникална или не съществува."
            messagebox.showerror("Вход неуспешен", msg)
            return

        self.operator_user_id = uid
        self.operator_login_session = login_arg or "*pwd_only*"
        self._log(f"✅ Успешен вход. Потребител ID = {uid}")

    # ----------------- Start / Export -----------------

    def _on_start_clicked(self):
        if not self.active_profile:
            messagebox.showerror("Грешка", "Няма активен профил.")
            return

        # трябва да сме логнати
        if not self.operator_user_id:
            self._log("Няма активна сесия. Изисква се вход...")
            self._operator_login_flow()
            if not self.operator_user_id:
                return

        if not self.selected_file:
            self._log("⚠️ Не е избран файл. Продължавам – демо режим (само показва статистика).")

        # >>> Тук ти си правиш реалното парсване на PDF-а <<<
        parsed_items = self._demo_parse_items()

        self._log("")
        self._log("== Статистика на обработката ==")
        self._log(f"Намерени артикула: {len(parsed_items)}")

        # Ако чекбоксът за „OPEN доставка“ е включен – пращаме към Мистрал
        if self.open_delivery_var.get():
            self._log("Опция: Вкарвай директно в Мистрал (OPEN доставка) -> ВКЛ.")
            try:
                # Импортираме локално и опитваме няколко възможни имена
                push_fn = None
                try:
                    from db_integration import push_items_to_mistral as _push
                    push_fn = _push
                except Exception:
                    try:
                        from db_integration import push_open_delivery as _push
                        push_fn = _push
                    except Exception:
                        try:
                            from db_integration import push_to_open_delivery as _push
                            push_fn = _push
                        except Exception:
                            pass

                if not push_fn:
                    messagebox.showerror(
                        "Грешка",
                        "Липсва функция за вкарване на артикули в Мистрал.\n"
                        "Очаквах една от: push_items_to_mistral, push_open_delivery, push_to_open_delivery."
                    )
                    return

                inserted = push_fn(self.active_profile, parsed_items)
                self._log(f"✅ Експортирани артикули: {inserted}")
            except Exception as e:
                messagebox.showerror("Грешка при вкарване в базата", str(e))
        else:
            self._log("Опция: Вкарвай директно в Мистрал (OPEN доставка) -> ИЗКЛ.")

        self._log("— Край —")

    # демо парсер – върни 1-2 артикула за тест; замени с твоя реален
    def _demo_parse_items(self):
        items = []
        if self.selected_file:
            # Сложи тук реалното четене/парсване
            pass
        # демо
        items.append({"code": "10", "name": "Пиле филе касе", "qty": 1, "price": 5.83})
        return items


# -------------------------
# main
# -------------------------
def main():
    root = tk.Tk()
    # плавен, светъл вид на ttk
    try:
        from tkinter import ttk as _ttk  # noqa
        root.call("tk", "scaling", 1.25)
        style = ttk.Style()
        if "vista" in style.theme_names():
            style.theme_use("vista")
    except Exception:
        pass

    app = MicroVisionApp(root)
    root.mainloop()


if __name__ == "__main__":
    main()
