"""CLI entry point for the IMAP email filter application."""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime
from getpass import getpass
from pathlib import Path
from typing import Dict, Iterable, Optional

import pandas as pd
from dotenv import load_dotenv

from email_filter import EmailFilter, EmailFilterCriteria, collect_unique_recipients
from imap_connector import IMAPConnectionConfig, IMAPConnector

APP_ROOT = Path(__file__).parent
LOG_PATH = APP_ROOT / "logs" / "app.log"
TEMPLATE_PATH = APP_ROOT / "filters" / "templates.json"


def configure_logging() -> None:
    """Initialise logging to both console and the application log file."""
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
        handlers=[
            logging.FileHandler(LOG_PATH, encoding="utf-8"),
            logging.StreamHandler(),
        ],
    )


def load_templates() -> Dict[str, dict]:
    """Load filter templates stored on disk."""
    if TEMPLATE_PATH.exists():
        try:
            with TEMPLATE_PATH.open("r", encoding="utf-8") as handle:
                return json.load(handle)
        except json.JSONDecodeError:
            logging.warning("Không thể đọc filters/templates.json, tạo mới.")
    return {}


def save_template(name: str, payload: dict) -> None:
    """Persist a template payload to disk."""
    templates = load_templates()
    templates[name] = payload
    TEMPLATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    with TEMPLATE_PATH.open("w", encoding="utf-8") as handle:
        json.dump(templates, handle, indent=2, ensure_ascii=False)


def prompt_choice(prompt: str, choices: Iterable[str], default: Optional[str] = None) -> str:
    """Prompt user to choose from explicit choices."""
    choice_list = list(choices)
    mapping = {str(index + 1): value for index, value in enumerate(choice_list)}
    while True:
        print(prompt)
        for index, value in mapping.items():
            indicator = " (mặc định)" if default and value == default else ""
            print(f"  {index}. {value}{indicator}")
        selection = input("Chọn số: ").strip() or ""
        if not selection and default:
            return default
        if selection in mapping:
            return mapping[selection]
        print("Lựa chọn không hợp lệ, vui lòng thử lại.")


def input_with_default(prompt: str, default: Optional[str] = None, required: bool = False) -> str:
    """Prompt for a value, offering an optional default."""
    suffix = f" [{default}]" if default else ""
    while True:
        value = input(f"{prompt}{suffix}: ").strip()
        if not value and default is not None:
            return str(default)
        if value:
            return value
        if not required:
            return ""
        print("Thông tin bắt buộc, vui lòng nhập giá trị.")


def input_password(prompt: str) -> str:
    """Prompt for a secret without echo."""
    return getpass(f"{prompt}: ")


def input_bool(prompt: str, default: bool = False) -> bool:
    """Ask a yes/no question."""
    suffix = " [Y/n]" if default else " [y/N]"
    while True:
        response = input(f"{prompt}{suffix}: ").strip().lower()
        if not response:
            return default
        if response in {"y", "yes"}:
            return True
        if response in {"n", "no"}:
            return False
        print("Vui lòng trả lời y(es) hoặc n(o).")


def coerce_bool(value, default: bool = True) -> bool:
    """Best-effort conversion of environment/template values to booleans."""
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


def parse_date_input(value: str) -> Optional[datetime]:
    """Parse a YYYY-MM-DD date string to a datetime."""
    value = value.strip()
    if not value:
        return None
    for fmt in ("%Y-%m-%d", "%d/%m/%Y"):
        try:
            dt = datetime.strptime(value, fmt)
            return datetime.combine(dt.date(), datetime.min.time())
        except ValueError:
            continue
    print("Không thể đọc ngày, bỏ qua.")
    return None


def parse_keywords(value: str) -> list[str]:
    """Split comma separated values into a list of trimmed strings."""
    return [token.strip() for token in value.split(",") if token.strip()]


def records_to_dataframe(records) -> pd.DataFrame:
    """Convert filter results into a pandas DataFrame."""
    return pd.DataFrame(
        [
            {
                "UID": record.uid,
                "Subject": record.subject,
                "From": record.from_address,
                "Date": record.sent_at.isoformat(),
                "Snippet": record.snippet,
                "To": ", ".join(record.to_recipients),
                "Cc": ", ".join(record.cc_recipients),
                "Bcc": ", ".join(record.bcc_recipients),
                "Has Attachments": record.has_attachments,
                "Mailbox": record.mailbox,
            }
            for record in records
        ]
    )


def export_results(df: pd.DataFrame) -> None:
    """Offer exporting results to CSV/JSON/Excel files."""
    if df.empty:
        print("Không có dữ liệu để xuất.")
        return

    while True:
        export_choice = prompt_choice(
            "Xuất kết quả?",
            ["Không", "CSV", "JSON", "Excel", "CSV + JSON + Excel"],
            default="Không",
        )
        if export_choice == "Không":
            return

        export_dir = Path(
            input_with_default("Thư mục lưu file", default=str(APP_ROOT / "exports"))
        )
        export_dir.mkdir(parents=True, exist_ok=True)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        if export_choice in {"CSV", "CSV + JSON + Excel"}:
            csv_path = export_dir / f"emails_{timestamp}.csv"
            df.to_csv(csv_path, index=False, encoding="utf-8-sig")
            print(f"Đã lưu {csv_path}")
        if export_choice in {"JSON", "CSV + JSON + Excel"}:
            json_path = export_dir / f"emails_{timestamp}.json"
            df.to_json(json_path, orient="records", indent=2, force_ascii=False)
            print(f"Đã lưu {json_path}")
        if export_choice in {"Excel", "CSV + JSON + Excel"}:
            excel_path = export_dir / f"emails_{timestamp}.xlsx"
            with pd.ExcelWriter(excel_path, engine="xlsxwriter") as writer:
                df.to_excel(writer, index=False)
            print(f"Đã lưu {excel_path}")
        return


def perform_actions(connector: IMAPConnector, records) -> None:
    """Apply optional actions to the selected messages."""
    uids = [record.uid for record in records]
    if not uids:
        return
    print("\n--- Hành động tự động ---")
    if input_bool("Đánh dấu tất cả email là đã đọc?", default=False):
        connector.add_flags(uids, ["\\Seen"])
        print("Đã đánh dấu đã đọc.")

    if input_bool("Chuyển email sang thư mục khác?", default=False):
        destination = input_with_default("Tên thư mục đích", required=True)
        connector.move(uids, destination)
        print(f"Đã chuyển tới {destination}.")

    if input_bool("Gắn nhãn Gmail?", default=False):
        label = input_with_default("Tên nhãn", required=True)
        connector.add_gmail_labels(uids, [label])
        print(f"Đã gắn nhãn {label}.")

    if input_bool("Xóa/Archive các email này?", default=False):
        connector.delete(uids)
        print("Đã xóa/Archive các email.")


def prompt_template_selection(templates: Dict[str, dict]) -> Optional[dict]:
    """Allow user to pick a template."""
    if not templates:
        return None
    names = ["Tự nhập"] + sorted(templates)
    choice = prompt_choice("Chọn mẫu bộ lọc", names, default="Tự nhập")
    if choice == "Tự nhập":
        return None
    print(f"Đã chọn mẫu: {choice}")
    return templates.get(choice, {})


def gather_connection_details(template: Optional[dict]) -> dict:
    """Prompt user for IMAP connection data."""
    print("\n--- Thông tin kết nối ---")
    defaults = {
        "server": template.get("imap_server") if template else os.getenv("IMAP_SERVER", "imap.gmail.com"),
        "port": str(template.get("imap_port")) if template else os.getenv("IMAP_PORT", "993"),
        "use_ssl": template.get("use_ssl") if template is not None else True,
        "folder": template.get("folder") if template else os.getenv("IMAP_FOLDER", "INBOX"),
        "auth_mechanism": template.get("auth_mechanism") if template else os.getenv("IMAP_AUTH", "LOGIN"),
        "email_address": template.get("email_address") if template else os.getenv("IMAP_EMAIL", ""),
    }

    server = input_with_default("IMAP server", defaults["server"])
    port = int(input_with_default("Port", defaults["port"]))
    use_ssl = input_bool("Sử dụng SSL?", default=coerce_bool(defaults["use_ssl"], True))
    folder = input_with_default("Thư mục (vd: INBOX)", defaults["folder"])
    auth_mechanism = prompt_choice(
        "Cơ chế xác thực",
        ["LOGIN", "XOAUTH2"],
        default=(defaults["auth_mechanism"] or "LOGIN").upper(),
    )
    email_address = input_with_default("Địa chỉ email", defaults["email_address"], required=True)
    password = input_password("Mật khẩu / OAuth token")

    return {
        "server": server,
        "port": port,
        "use_ssl": use_ssl,
        "folder": folder,
        "auth_mechanism": auth_mechanism,
        "email_address": email_address,
        "password": password,
    }


def gather_filter_criteria(template: Optional[dict]) -> tuple[EmailFilterCriteria, dict]:
    """Prompt user for filtering options."""
    print("\n--- Điều kiện lọc ---")
    subject_keywords_raw = input_with_default(
        "Từ khóa tiêu đề (phân cách bởi dấu phẩy)",
        template.get("subject_keywords", "") if template else "",
    )
    body_keywords_raw = input_with_default(
        "Từ khóa nội dung (phân cách bởi dấu phẩy)",
        template.get("body_keywords", "") if template else "",
    )
    from_keywords_raw = input_with_default(
        "Người gửi chứa (vd: support, contact)",
        template.get("from_keywords", "") if template else "",
    )
    from_domains_raw = input_with_default(
        "Domain người gửi (vd: company.com)",
        template.get("from_domains", "") if template else "",
    )
    match_operator = prompt_choice(
        "Kết hợp từ khóa",
        ["AND", "OR"],
        default=template.get("keyword_operator", "AND") if template else "AND",
    )

    has_attachment_map = {
        "Bất kỳ": None,
        "Có tệp đính kèm": True,
        "Không có tệp đính kèm": False,
    }
    legacy_attachment_map = {
        "Any": "Bất kỳ",
        "Has attachments": "Có tệp đính kèm",
        "No attachments": "Không có tệp đính kèm",
    }
    attachment_default = "Bất kỳ"
    if template:
        attachment_default = template.get("attachment_choice", "Bất kỳ")
        attachment_default = legacy_attachment_map.get(attachment_default, attachment_default)
    attachment_choice = prompt_choice(
        "Điều kiện tệp đính kèm",
        list(has_attachment_map),
        default=attachment_default,
    )

    from_date_input = input_with_default(
        "Ngày bắt đầu (YYYY-MM-DD hoặc dd/mm/YYYY, để trống nếu không dùng)",
        template.get("from_date") if template else "",
    )
    to_date_input = input_with_default(
        "Ngày kết thúc (YYYY-MM-DD hoặc dd/mm/YYYY, để trống nếu không dùng)",
        template.get("to_date") if template else "",
    )
    from_date = parse_date_input(from_date_input)
    to_date = parse_date_input(to_date_input)

    criteria = EmailFilterCriteria(
        subject_keywords=parse_keywords(subject_keywords_raw),
        body_keywords=parse_keywords(body_keywords_raw),
        from_contains=parse_keywords(from_keywords_raw),
        from_domains=[domain.lstrip("@") for domain in parse_keywords(from_domains_raw)],
        from_date=from_date,
        to_date=to_date,
        has_attachments=has_attachment_map[attachment_choice],
        match_operator=match_operator,
    )
    filter_state = {
        "subject_keywords": subject_keywords_raw,
        "body_keywords": body_keywords_raw,
        "from_keywords": from_keywords_raw,
        "from_domains": from_domains_raw,
        "keyword_operator": match_operator,
        "attachment_choice": attachment_choice,
        "from_date": from_date_input,
        "to_date": to_date_input,
    }
    return criteria, filter_state


def display_results(records) -> None:
    """Print a human readable summary of the filtered emails."""
    if not records:
        print("\nKhông tìm thấy email phù hợp.")
        return

    print(f"\nTìm thấy {len(records)} email phù hợp:")
    for index, record in enumerate(records, start=1):
        sent_at = record.sent_at.strftime("%Y-%m-%d %H:%M")
        snippet = (record.snippet[:120] + "...") if len(record.snippet) > 120 else record.snippet
        print(f"\n[{index}] UID: {record.uid}")
        print(f"  Tiêu đề : {record.subject}")
        print(f"  Người gửi : {record.from_address}")
        print(f"  Ngày gửi  : {sent_at}")
        print(f"  Đính kèm : {'Có' if record.has_attachments else 'Không'}")
        print(f"  Trích đoạn: {snippet}")

    recipients = collect_unique_recipients(records)
    if recipients:
        print("\nNgười nhận (đã loại trùng):")
        print(", ".join(recipients))


def maybe_save_template(template_data: dict) -> None:
    """Offer to save the current filter configuration."""
    if not input_bool("Lưu bộ lọc thành mẫu để sử dụng sau?", default=False):
        return
    name = input_with_default("Tên mẫu", required=True)
    save_template(name, template_data)
    print(f"Đã lưu mẫu '{name}'.")


def main() -> None:
    """Entry point for the CLI application."""
    load_dotenv()
    configure_logging()

    print("====================================")
    print(" IMAP Email Filter - CLI Interface ")
    print("====================================\n")

    templates = load_templates()
    chosen_template = prompt_template_selection(templates)

    connection_data = gather_connection_details(chosen_template or {})
    criteria, filter_state = gather_filter_criteria(chosen_template or {})

    config = IMAPConnectionConfig(
        server=connection_data["server"],
        port=connection_data["port"],
        use_ssl=connection_data["use_ssl"],
        folder=connection_data["folder"],
        auth_mechanism=connection_data["auth_mechanism"],
    )
    connector = IMAPConnector(config)

    try:
        print("\nĐang đăng nhập vào máy chủ IMAP...")
        connector.login(connection_data["email_address"], connection_data["password"])
        print("Đăng nhập thành công.\nĐang tìm email...")

        filter_instance = EmailFilter(connector)
        records = filter_instance.search(criteria)

        display_results(records)

        if records:
            df = records_to_dataframe(records)
            export_results(df)
            perform_actions(connector, records)

            template_snapshot = {
                "imap_server": connection_data["server"],
                "imap_port": connection_data["port"],
                "use_ssl": connection_data["use_ssl"],
                "folder": connection_data["folder"],
                "auth_mechanism": connection_data["auth_mechanism"],
                "email_address": connection_data["email_address"],
                "subject_keywords": filter_state["subject_keywords"],
                "body_keywords": filter_state["body_keywords"],
                "from_keywords": filter_state["from_keywords"],
                "from_domains": filter_state["from_domains"],
                "keyword_operator": filter_state["keyword_operator"],
                "attachment_choice": filter_state["attachment_choice"],
                "from_date": filter_state["from_date"],
                "to_date": filter_state["to_date"],
            }
            maybe_save_template(template_snapshot)
    except Exception as exc:  # noqa: BLE001
        logging.exception("Lỗi trong quá trình lọc email: %s", exc)
        print(f"Đã xảy ra lỗi: {exc}")
    finally:
        connector.logout()
        print("\nĐã ngắt kết nối IMAP. Hẹn gặp lại!")


if __name__ == "__main__":
    main()

