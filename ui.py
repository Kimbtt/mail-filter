"""Tkinter GUI for the IMAP email filter application."""

from __future__ import annotations

import logging
import os
import threading
import tkinter as tk
from datetime import datetime
from tkinter import messagebox, ttk
from tkinter.filedialog import asksaveasfilename
from typing import List, Optional

import pandas as pd
from dotenv import load_dotenv

from email_filter import EmailFilter, EmailFilterCriteria, EmailRecord, collect_unique_recipients
from imap_connector import IMAPConnectionConfig, IMAPConnector
from main import (  # reuse shared utilities from CLI implementation
    configure_logging,
    load_templates,
    parse_date_input,
    parse_keywords,
    records_to_dataframe,
    save_template,
)


class EmailFilterApp:
    """Encapsulates the Tkinter application."""

    ATTACHMENT_OPTIONS = ["Bất kỳ", "Có tệp đính kèm", "Không có tệp đính kèm"]

    def __init__(self, root: tk.Tk):
        load_dotenv()
        configure_logging()

        self.root = root
        self.root.title("IMAP Email Filter")
        self.root.geometry("1100x950")

        self.templates = load_templates()
        self.records: List[EmailRecord] = []
        self.recipients: List[str] = []
        self.filter_state: dict | None = None
        self.connector: Optional[IMAPConnector] = None
        self.search_thread: Optional[threading.Thread] = None

        self._init_variables()
        self._build_widgets()

        self.root.protocol("WM_DELETE_WINDOW", self._on_close)
        self._set_status("Sẵn sàng.")

    # ------------------------------------------------------------------ #
    # UI initialisation                                                  #
    # ------------------------------------------------------------------ #
    def _init_variables(self) -> None:
        self.template_var = tk.StringVar(value="Tự nhập")
        self.server_var = tk.StringVar(value=self._env_default("IMAP_SERVER", "imap.gmail.com"))
        self.port_var = tk.StringVar(value=self._env_default("IMAP_PORT", "993"))
        self.use_ssl_var = tk.BooleanVar(value=self._env_bool("IMAP_USE_SSL", True))
        self.folder_var = tk.StringVar(value=self._env_default("IMAP_FOLDER", "INBOX"))
        self.auth_var = tk.StringVar(value=self._env_default("IMAP_AUTH", "LOGIN").upper())
        self.email_var = tk.StringVar(value=self._env_default("IMAP_EMAIL", ""))
        self.password_var = tk.StringVar()

        self.subject_var = tk.StringVar()
        self.body_var = tk.StringVar()
        self.from_keywords_var = tk.StringVar()
        self.from_domains_var = tk.StringVar()
        self.operator_var = tk.StringVar(value="AND")
        self.attachment_var = tk.StringVar(value=self.ATTACHMENT_OPTIONS[0])
        self.from_date_var = tk.StringVar()
        self.to_date_var = tk.StringVar()

        self.mark_read_var = tk.BooleanVar(value=False)
        self.move_var = tk.BooleanVar(value=False)
        self.move_folder_var = tk.StringVar()
        self.label_var = tk.BooleanVar(value=False)
        self.label_name_var = tk.StringVar()
        self.delete_var = tk.BooleanVar(value=False)

        self.status_var = tk.StringVar()

    def _build_widgets(self) -> None:
        container = ttk.Frame(self.root)
        container.pack(fill=tk.BOTH, expand=True, padx=12, pady=10)

        self._build_template_frame(container)
        self._build_connection_frame(container)
        self._build_filter_frame(container)
        self._build_actions_frame(container)
        self._build_results_frame(container)
        self._build_export_frame(container)

        status_bar = ttk.Label(self.root, textvariable=self.status_var, anchor="w")
        status_bar.pack(fill=tk.X, padx=12, pady=(0, 8))

    def _build_template_frame(self, parent: ttk.Frame) -> None:
        frame = ttk.LabelFrame(parent, text="Mẫu bộ lọc")
        frame.pack(fill=tk.X, pady=(0, 10))

        ttk.Label(frame, text="Chọn mẫu:").grid(row=0, column=0, padx=8, pady=6, sticky="w")
        template_options = ["Tự nhập"] + sorted(self.templates.keys())
        self.template_combo = ttk.Combobox(frame, textvariable=self.template_var, state="readonly")
        self.template_combo["values"] = template_options
        self.template_combo.grid(row=0, column=1, padx=8, pady=6, sticky="ew")

        apply_btn = ttk.Button(frame, text="Áp dụng", command=self._apply_template)
        apply_btn.grid(row=0, column=2, padx=8, pady=6)

        save_btn = ttk.Button(frame, text="Lưu mẫu hiện tại", command=self._save_template_dialog)
        save_btn.grid(row=0, column=3, padx=8, pady=6)

        frame.columnconfigure(1, weight=1)

    def _build_connection_frame(self, parent: ttk.Frame) -> None:
        frame = ttk.LabelFrame(parent, text="Thông tin kết nối IMAP")
        frame.pack(fill=tk.X, pady=(0, 10))

        entries = [
            ("Máy chủ", self.server_var),
            ("Port", self.port_var),
            ("Thư mục", self.folder_var),
            ("Email", self.email_var),
        ]
        for index, (label, var) in enumerate(entries):
            ttk.Label(frame, text=label).grid(row=index // 2, column=(index % 2) * 2, padx=8, pady=4, sticky="w")
            ttk.Entry(frame, textvariable=var, width=30).grid(
                row=index // 2, column=(index % 2) * 2 + 1, padx=8, pady=4, sticky="ew"
            )

        ttk.Label(frame, text="Cơ chế xác thực").grid(row=2, column=0, padx=8, pady=4, sticky="w")
        auth_combo = ttk.Combobox(frame, textvariable=self.auth_var, values=["LOGIN", "XOAUTH2"], state="readonly")
        auth_combo.grid(row=2, column=1, padx=8, pady=4, sticky="ew")

        ttk.Label(frame, text="Mật khẩu / Token").grid(row=2, column=2, padx=8, pady=4, sticky="w")
        ttk.Entry(frame, textvariable=self.password_var, show="*", width=30).grid(
            row=2, column=3, padx=8, pady=4, sticky="ew"
        )

        ttk.Checkbutton(frame, text="Sử dụng SSL", variable=self.use_ssl_var).grid(
            row=0, column=4, padx=8, pady=4, sticky="w"
        )

        search_btn = ttk.Button(frame, text="Lọc email", command=self._on_search_clicked)
        search_btn.grid(row=3, column=3, padx=8, pady=8, sticky="e")
        self.search_button = search_btn

        frame.columnconfigure(1, weight=1)
        frame.columnconfigure(3, weight=1)

    def _build_filter_frame(self, parent: ttk.Frame) -> None:
        frame = ttk.LabelFrame(parent, text="Điều kiện lọc")
        frame.pack(fill=tk.X, pady=(0, 10))

        ttk.Label(frame, text="Từ khóa tiêu đề").grid(row=0, column=0, padx=8, pady=4, sticky="w")
        ttk.Entry(frame, textvariable=self.subject_var).grid(row=0, column=1, padx=8, pady=4, sticky="ew")

        ttk.Label(frame, text="Từ khóa nội dung").grid(row=0, column=2, padx=8, pady=4, sticky="w")
        ttk.Entry(frame, textvariable=self.body_var).grid(row=0, column=3, padx=8, pady=4, sticky="ew")

        ttk.Label(frame, text="Người gửi chứa").grid(row=1, column=0, padx=8, pady=4, sticky="w")
        ttk.Entry(frame, textvariable=self.from_keywords_var).grid(row=1, column=1, padx=8, pady=4, sticky="ew")

        ttk.Label(frame, text="Domain người gửi").grid(row=1, column=2, padx=8, pady=4, sticky="w")
        ttk.Entry(frame, textvariable=self.from_domains_var).grid(row=1, column=3, padx=8, pady=4, sticky="ew")

        ttk.Label(frame, text="Kết hợp từ khóa").grid(row=2, column=0, padx=8, pady=4, sticky="w")
        operator_combo = ttk.Combobox(frame, textvariable=self.operator_var, values=["AND", "OR"], state="readonly")
        operator_combo.grid(row=2, column=1, padx=8, pady=4, sticky="ew")

        ttk.Label(frame, text="Tệp đính kèm").grid(row=2, column=2, padx=8, pady=4, sticky="w")
        attachment_combo = ttk.Combobox(
            frame, textvariable=self.attachment_var, values=self.ATTACHMENT_OPTIONS, state="readonly"
        )
        attachment_combo.grid(row=2, column=3, padx=8, pady=4, sticky="ew")

        ttk.Label(frame, text="Từ ngày (YYYY-MM-DD)").grid(row=3, column=0, padx=8, pady=4, sticky="w")
        ttk.Entry(frame, textvariable=self.from_date_var).grid(row=3, column=1, padx=8, pady=4, sticky="ew")

        ttk.Label(frame, text="Đến ngày (YYYY-MM-DD)").grid(row=3, column=2, padx=8, pady=4, sticky="w")
        ttk.Entry(frame, textvariable=self.to_date_var).grid(row=3, column=3, padx=8, pady=4, sticky="ew")

        frame.columnconfigure(1, weight=1)
        frame.columnconfigure(3, weight=1)

    def _build_actions_frame(self, parent: ttk.Frame) -> None:
        frame = ttk.LabelFrame(parent, text="Hành động tự động (tùy chọn)")
        frame.pack(fill=tk.X, pady=(0, 10))

        ttk.Checkbutton(frame, text="Đánh dấu đã đọc", variable=self.mark_read_var).grid(
            row=0, column=0, padx=8, pady=4, sticky="w"
        )
        ttk.Checkbutton(frame, text="Xóa / Archive", variable=self.delete_var).grid(
            row=0, column=1, padx=8, pady=4, sticky="w"
        )

        ttk.Checkbutton(frame, text="Chuyển thư mục", variable=self.move_var).grid(
            row=1, column=0, padx=8, pady=4, sticky="w"
        )
        ttk.Entry(frame, textvariable=self.move_folder_var, width=30).grid(
            row=1, column=1, padx=8, pady=4, sticky="ew"
        )

        ttk.Checkbutton(frame, text="Gắn nhãn (Gmail)", variable=self.label_var).grid(
            row=1, column=2, padx=8, pady=4, sticky="w"
        )
        ttk.Entry(frame, textvariable=self.label_name_var, width=30).grid(
            row=1, column=3, padx=8, pady=4, sticky="ew"
        )

        action_btn = ttk.Button(frame, text="Áp dụng hành động", command=self._apply_actions)
        action_btn.grid(row=0, column=3, rowspan=2, padx=8, pady=4, sticky="e")

        frame.columnconfigure(1, weight=1)
        frame.columnconfigure(3, weight=1)

    def _build_results_frame(self, parent: ttk.Frame) -> None:
        frame = ttk.LabelFrame(parent, text="Kết quả lọc")
        frame.pack(fill=tk.BOTH, expand=True, pady=(0, 10))

        columns = ("uid", "subject", "from", "date", "snippet")
        self.tree = ttk.Treeview(frame, columns=columns, show="headings", height=12)
        headings = {
            "uid": "UID",
            "subject": "Tiêu đề",
            "from": "Người gửi",
            "date": "Ngày",
            "snippet": "Trích đoạn",
        }
        widths = {"uid": 80, "subject": 250, "from": 160, "date": 140, "snippet": 400}
        for col in columns:
            self.tree.heading(col, text=headings[col])
            self.tree.column(col, width=widths[col], stretch=True)

        scrollbar = ttk.Scrollbar(frame, orient=tk.VERTICAL, command=self.tree.yview)
        self.tree.configure(yscroll=scrollbar.set)
        self.tree.grid(row=0, column=0, sticky="nsew")
        scrollbar.grid(row=0, column=1, sticky="ns")

        self.tree.bind("<Double-1>", self._show_message_details)

        frame.rowconfigure(0, weight=1)
        frame.columnconfigure(0, weight=1)

        recipients_frame = ttk.LabelFrame(parent, text="Người nhận (đã loại trùng)")
        recipients_frame.pack(fill=tk.X, pady=(0, 10))

        self.recipients_text = tk.Text(recipients_frame, height=3, wrap="word")
        self.recipients_text.pack(fill=tk.BOTH, expand=True, padx=8, pady=(6, 0))
        self.recipients_text.configure(state="disabled")

        btn_frame = ttk.Frame(recipients_frame)
        btn_frame.pack(fill=tk.X, padx=8, pady=6)
        ttk.Button(
            btn_frame, text="Sao chép danh sách", command=self._copy_recipients_to_clipboard
        ).pack(side=tk.LEFT)
        ttk.Button(
            btn_frame, text="Lưu danh sách...", command=self._export_recipients
        ).pack(side=tk.LEFT, padx=8)

    def _build_export_frame(self, parent: ttk.Frame) -> None:
        frame = ttk.Frame(parent)
        frame.pack(fill=tk.X, pady=(0, 5))

        ttk.Label(frame, text="Xuất dữ liệu:").pack(side=tk.LEFT, padx=(0, 8))

        ttk.Button(frame, text="CSV", command=lambda: self._export_results("csv")).pack(side=tk.LEFT, padx=4)
        ttk.Button(frame, text="JSON", command=lambda: self._export_results("json")).pack(side=tk.LEFT, padx=4)
        ttk.Button(frame, text="Excel", command=lambda: self._export_results("excel")).pack(side=tk.LEFT, padx=4)

    # ------------------------------------------------------------------ #
    # Event handlers                                                     #
    # ------------------------------------------------------------------ #
    def _apply_template(self) -> None:
        name = self.template_var.get()
        if name == "Tự nhập":
            return
        template = self.templates.get(name)
        if not template:
            messagebox.showerror("Lỗi", f"Không tìm thấy mẫu '{name}'.")
            return
        self._set_status(f"Áp dụng mẫu {name}.")
        self.server_var.set(template.get("imap_server", self.server_var.get()))
        self.port_var.set(str(template.get("imap_port", self.port_var.get())))
        self.use_ssl_var.set(
            self._normalize_bool(template.get("use_ssl"), self.use_ssl_var.get())
        )
        self.folder_var.set(template.get("folder", self.folder_var.get()))
        self.auth_var.set(template.get("auth_mechanism", self.auth_var.get()).upper())
        self.email_var.set(template.get("email_address", self.email_var.get()))

        self.subject_var.set(template.get("subject_keywords", ""))
        self.body_var.set(template.get("body_keywords", ""))
        self.from_keywords_var.set(template.get("from_keywords", ""))
        self.from_domains_var.set(template.get("from_domains", ""))
        self.operator_var.set(template.get("keyword_operator", "AND"))
        self.attachment_var.set(template.get("attachment_choice", "Bất kỳ"))
        self.from_date_var.set(template.get("from_date") or "")
        self.to_date_var.set(template.get("to_date") or "")

    def _save_template_dialog(self) -> None:
        if not self.filter_state:
            messagebox.showinfo("Thông báo", "Hãy chạy lọc ít nhất một lần trước khi lưu mẫu.")
            return
        name = self._prompt_text("Nhập tên mẫu")
        if not name:
            return
        payload = {
            "imap_server": self.server_var.get().strip(),
            "imap_port": int(self.port_var.get() or 993),
            "use_ssl": self.use_ssl_var.get(),
            "folder": self.folder_var.get().strip() or "INBOX",
            "auth_mechanism": self.auth_var.get().upper(),
            "email_address": self.email_var.get().strip(),
            **self.filter_state,
        }
        save_template(name, payload)
        self.templates = load_templates()
        self.template_combo["values"] = ["Tự nhập"] + sorted(self.templates.keys())
        messagebox.showinfo("Thành công", f"Đã lưu mẫu '{name}'.")

    def _on_search_clicked(self) -> None:
        if self.search_thread and self.search_thread.is_alive():
            return
        try:
            port = int(self.port_var.get())
            if port <= 0 or port > 65535:
                raise ValueError
        except ValueError:
            messagebox.showerror("Lỗi", "Port không hợp lệ.")
            return

        self.search_button.configure(state="disabled")
        self._set_status("Đang tìm email...")
        self.records = []
        self._clear_results_table()

        thread = threading.Thread(target=self._perform_search, daemon=True)
        self.search_thread = thread
        thread.start()

    def _perform_search(self) -> None:
        connection_details = self._build_connection_details()
        criteria, filter_state = self._build_filter_criteria()

        connector = IMAPConnector(
            IMAPConnectionConfig(
                server=connection_details["server"],
                port=connection_details["port"],
                use_ssl=connection_details["use_ssl"],
                folder=connection_details["folder"],
                auth_mechanism=connection_details["auth_mechanism"],
            )
        )

        try:
            connector.login(connection_details["email_address"], connection_details["password"])
            filter_instance = EmailFilter(connector)
            records = filter_instance.search(criteria)
        except Exception as exc:  # noqa: BLE001
            logging.exception("Lỗi khi lọc email: %s", exc)
            connector.logout()
            error = exc
            self.root.after(0, lambda err=error: self._on_search_failed(err))
            return

        self.root.after(
            0,
            lambda: self._on_search_success(connector, records, filter_state),
        )

    def _on_search_success(self, connector: IMAPConnector, records: List[EmailRecord], filter_state: dict) -> None:
        if self.connector:
            try:
                self.connector.logout()
            except Exception:  # noqa: BLE001
                pass

        self.connector = connector
        self.records = records
        self.filter_state = filter_state

        self._populate_results_table(records)
        self._populate_recipients(records)

        count = len(records)
        if count:
            self._set_status(f"Tìm thấy {count} email phù hợp.")
        else:
            self._set_status("Không tìm thấy email phù hợp.")
        self.search_button.configure(state="normal")
        self.search_thread = None

    def _on_search_failed(self, error: Exception) -> None:
        self._set_status("Lỗi khi lọc email.")
        messagebox.showerror("Lỗi", f"Không thể lọc email:\n{error}")
        self.search_button.configure(state="normal")
        self.search_thread = None

    def _apply_actions(self) -> None:
        if not self.records:
            messagebox.showinfo("Thông báo", "Chưa có kết quả để áp dụng hành động.")
            return
        if not self.connector:
            messagebox.showerror("Lỗi", "Chưa có kết nối IMAP đang mở.")
            return

        uids = [record.uid for record in self.records]
        actions = []
        try:
            if self.mark_read_var.get():
                self.connector.add_flags(uids, ["\\Seen"])
                actions.append("đánh dấu đã đọc")
            if self.move_var.get():
                destination = self.move_folder_var.get().strip()
                if not destination:
                    raise ValueError("Vui lòng nhập thư mục đích.")
                self.connector.move(uids, destination)
                actions.append(f"chuyển tới {destination}")
            if self.label_var.get():
                label = self.label_name_var.get().strip()
                if not label:
                    raise ValueError("Vui lòng nhập tên nhãn.")
                self.connector.add_gmail_labels(uids, [label])
                actions.append(f"gắn nhãn {label}")
            if self.delete_var.get():
                self.connector.delete(uids)
                actions.append("xóa/archive")
        except Exception as exc:  # noqa: BLE001
            logging.exception("Lỗi khi áp dụng hành động: %s", exc)
            messagebox.showerror("Lỗi", f"Không thể áp dụng hành động:\n{exc}")
            return

        if actions:
            messagebox.showinfo("Thành công", f"Đã hoàn tất: {', '.join(actions)}.")
            self._set_status("Đã áp dụng hành động.")
        else:
            messagebox.showinfo("Thông báo", "Không có hành động nào được chọn.")

    def _export_results(self, kind: str) -> None:
        if not self.records:
            messagebox.showinfo("Thông báo", "Không có dữ liệu để xuất.")
            return

        filetypes = {
            "csv": ("CSV files", "*.csv"),
            "json": ("JSON files", "*.json"),
            "excel": ("Excel files", "*.xlsx"),
        }
        default_extensions = {"csv": ".csv", "json": ".json", "excel": ".xlsx"}
        path = asksaveasfilename(
            title="Chọn vị trí lưu",
            defaultextension=default_extensions[kind],
            filetypes=[filetypes[kind], ("All files", "*.*")],
        )
        if not path:
            return

        df = records_to_dataframe(self.records)
        try:
            if kind == "csv":
                df.to_csv(path, index=False, encoding="utf-8-sig")
            elif kind == "json":
                df.to_json(path, orient="records", indent=2, force_ascii=False)
            else:
                with pd.ExcelWriter(path, engine="xlsxwriter") as writer:
                    df.to_excel(writer, index=False)
        except Exception as exc:  # noqa: BLE001
            logging.exception("Lỗi khi xuất dữ liệu: %s", exc)
            messagebox.showerror("Lỗi", f"Không thể lưu tệp:\n{exc}")
            return

        messagebox.showinfo("Thành công", f"Đã lưu tệp: {path}")

    def _show_message_details(self, event) -> None:  # noqa: D401 - event handler
        item = self.tree.identify_row(event.y)
        if not item:
            return
        index = self.tree.index(item)
        if index >= len(self.records):
            return
        record = self.records[index]
        sent_at = record.sent_at.strftime("%Y-%m-%d %H:%M")
        messagebox.showinfo(
            "Chi tiết email",
            f"Tiêu đề: {record.subject}\n"
            f"Người gửi: {record.from_address}\n"
            f"Ngày gửi: {sent_at}\n\n"
            f"Trích đoạn:\n{record.snippet}",
        )

    # ------------------------------------------------------------------ #
    # Helpers                                                            #
    # ------------------------------------------------------------------ #
    def _build_connection_details(self) -> dict:
        return {
            "server": self.server_var.get().strip(),
            "port": int(self.port_var.get()),
            "use_ssl": self.use_ssl_var.get(),
            "folder": self.folder_var.get().strip() or "INBOX",
            "auth_mechanism": self.auth_var.get().upper(),
            "email_address": self.email_var.get().strip(),
            "password": self.password_var.get(),
        }

    def _build_filter_criteria(self) -> tuple[EmailFilterCriteria, dict]:
        attachment_map = {
            "Bất kỳ": None,
            "Có tệp đính kèm": True,
            "Không có tệp đính kèm": False,
        }
        from_date = parse_date_input(self.from_date_var.get())
        to_date = parse_date_input(self.to_date_var.get())

        criteria = EmailFilterCriteria(
            subject_keywords=parse_keywords(self.subject_var.get()),
            body_keywords=parse_keywords(self.body_var.get()),
            from_contains=parse_keywords(self.from_keywords_var.get()),
            from_domains=[domain.lstrip("@") for domain in parse_keywords(self.from_domains_var.get())],
            from_date=from_date,
            to_date=to_date,
            has_attachments=attachment_map[self.attachment_var.get()],
            match_operator=self.operator_var.get(),
        )
        filter_state = {
            "subject_keywords": self.subject_var.get(),
            "body_keywords": self.body_var.get(),
            "from_keywords": self.from_keywords_var.get(),
            "from_domains": self.from_domains_var.get(),
            "keyword_operator": self.operator_var.get(),
            "attachment_choice": self.attachment_var.get(),
            "from_date": self.from_date_var.get(),
            "to_date": self.to_date_var.get(),
        }
        return criteria, filter_state

    def _populate_results_table(self, records: List[EmailRecord]) -> None:
        self._clear_results_table()
        for record in records:
            sent_at = record.sent_at.strftime("%Y-%m-%d %H:%M")
            snippet = record.snippet
            if len(snippet) > 100:
                snippet = snippet[:97] + "..."
            self.tree.insert(
                "",
                "end",
                values=(record.uid, record.subject, record.from_address, sent_at, snippet),
            )

    def _populate_recipients(self, records: List[EmailRecord]) -> None:
        recipients = collect_unique_recipients(records)
        self.recipients = recipients
        self.recipients_text.configure(state="normal")
        self.recipients_text.delete("1.0", tk.END)
        if recipients:
            self.recipients_text.insert(tk.END, ", ".join(recipients))
        else:
            self.recipients_text.insert(tk.END, "Không có dữ liệu.")
        self.recipients_text.configure(state="disabled")

    def _clear_results_table(self) -> None:
        for child in self.tree.get_children():
            self.tree.delete(child)
        self.recipients_text.configure(state="normal")
        self.recipients_text.delete("1.0", tk.END)
        self.recipients_text.configure(state="disabled")
        self.recipients = []

    def _set_status(self, message: str) -> None:
        timestamp = datetime.now().strftime("%H:%M:%S")
        self.status_var.set(f"[{timestamp}] {message}")

    def _prompt_text(self, message: str) -> Optional[str]:
        dialog = tk.Toplevel(self.root)
        dialog.title(message)
        dialog.grab_set()

        ttk.Label(dialog, text=message).pack(padx=12, pady=(12, 6))
        value_var = tk.StringVar()
        entry = ttk.Entry(dialog, textvariable=value_var)
        entry.pack(padx=12, pady=6, fill=tk.X)
        entry.focus_set()

        result: dict[str, Optional[str]] = {"value": None}

        def on_ok():
            result["value"] = value_var.get().strip() or None
            dialog.destroy()

        def on_cancel():
            dialog.destroy()

        button_frame = ttk.Frame(dialog)
        button_frame.pack(padx=12, pady=(0, 12))
        ttk.Button(button_frame, text="Lưu", command=on_ok).pack(side=tk.LEFT, padx=4)
        ttk.Button(button_frame, text="Hủy", command=on_cancel).pack(side=tk.LEFT, padx=4)

        self.root.wait_window(dialog)
        return result["value"]

    def _on_close(self) -> None:
        if self.connector:
            try:
                self.connector.logout()
            except Exception:  # noqa: BLE001
                pass
        self.root.destroy()

    # ------------------------------------------------------------------ #
    # Environment helpers                                                #
    # ------------------------------------------------------------------ #
    @staticmethod
    def _env_default(key: str, default: str) -> str:
        value = os.getenv(key)
        return value if value is not None and value.strip() else default

    @staticmethod
    def _env_bool(key: str, default: bool) -> bool:
        value = os.getenv(key)
        if value is None:
            return default
        lowered = value.strip().lower()
        if lowered in {"true", "1", "yes", "y", "on"}:
            return True
        if lowered in {"false", "0", "no", "n", "off"}:
            return False
        return default

    @staticmethod
    def _normalize_bool(value, default: bool) -> bool:
        if isinstance(value, bool):
            return value
        if value is None:
            return default
        if isinstance(value, str):
            lowered = value.strip().lower()
            if lowered in {"true", "1", "yes", "y", "on"}:
                return True
            if lowered in {"false", "0", "no", "n", "off"}:
                return False
            if not lowered:
                return default
        return bool(value)

    # ------------------------------------------------------------------ #
    # Recipient utilities                                                #
    # ------------------------------------------------------------------ #
    def _copy_recipients_to_clipboard(self) -> None:
        if not self.recipients:
            messagebox.showinfo("Thông báo", "Không có danh sách người nhận.")
            return
        payload = "\n".join(self.recipients)
        self.root.clipboard_clear()
        self.root.clipboard_append(payload)
        self._set_status("Đã sao chép danh sách người nhận.")

    def _export_recipients(self) -> None:
        if not self.recipients:
            messagebox.showinfo("Thông báo", "Không có danh sách người nhận.")
            return
        path = asksaveasfilename(
            title="Lưu danh sách người nhận",
            defaultextension=".txt",
            filetypes=[("Text files", "*.txt"), ("All files", "*.*")],
        )
        if not path:
            return
        try:
            with open(path, "w", encoding="utf-8") as handle:
                handle.write("\n".join(self.recipients))
        except Exception as exc:  # noqa: BLE001
            logging.exception("Không thể lưu danh sách người nhận: %s", exc)
            messagebox.showerror("Lỗi", f"Không thể lưu tệp:\n{exc}")
            return
        messagebox.showinfo("Thành công", f"Đã lưu danh sách người nhận vào {path}")
        self._set_status("Đã lưu danh sách người nhận.")


def run_app() -> None:
    root = tk.Tk()
    app = EmailFilterApp(root)
    root.mainloop()


if __name__ == "__main__":
    run_app()

