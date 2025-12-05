"""Filtering logic for IMAP messages."""

from __future__ import annotations

import email
import logging
import re
import signal
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from email.header import decode_header, make_header
from email.message import Message
from email.utils import getaddresses, parsedate_to_datetime
from typing import Iterable, List, Optional, Sequence, Tuple

from imap_connector import IMAPConnector

logger = logging.getLogger(__name__)


class FilterCancelled(Exception):
    """Exception raised when user cancels the filtering process."""
    pass


@dataclass
class EmailFilterCriteria:
    """Represents filtering options provided by the user."""

    subject_keywords: List[str] = field(default_factory=list)
    body_keywords: List[str] = field(default_factory=list)
    from_contains: List[str] = field(default_factory=list)
    from_domains: List[str] = field(default_factory=list)
    from_date: Optional[datetime] = None
    to_date: Optional[datetime] = None
    has_attachments: Optional[bool] = None
    match_operator: str = "AND"  # AND / OR

    def normalized_operator(self) -> str:
        op = self.match_operator.upper()
        return op if op in {"AND", "OR"} else "AND"


@dataclass
class EmailRecord:
    """Lightweight representation of a single email."""

    uid: int
    subject: str
    from_address: str
    sent_at: datetime
    snippet: str
    to_recipients: List[str]
    cc_recipients: List[str]
    bcc_recipients: List[str]
    has_attachments: bool
    mailbox: str


class EmailFilter:
    """High-level filtering using an :class:`IMAPConnector`."""

    def __init__(self, connector: IMAPConnector):
        self.connector = connector
        self._cancelled = False
    
    def cancel(self) -> None:
        """Cancel the ongoing filtering operation."""
        self._cancelled = True
        logger.info("Filtering operation cancelled by user")

    # ------------------------------------------------------------------ #
    # Public API                                                         #
    # ------------------------------------------------------------------ #
    def search(self, criteria: EmailFilterCriteria, limit: Optional[int] = None, batch_size: int = 100) -> List[EmailRecord]:
        """Fetch matching messages based on the supplied criteria."""
        self._cancelled = False
        
        imap_criteria = self._build_imap_criteria(criteria)
        uids = self.connector.search(imap_criteria)
        logger.info("IMAP search returned %d UIDs", len(uids))
        
        if limit:
            uids = uids[:limit]
        
        records: List[EmailRecord] = []
        total_uids = len(uids)
        
        # Process in batches to avoid memory issues and improve performance
        for i in range(0, total_uids, batch_size):
            if self._cancelled:
                logger.warning("Filtering cancelled at batch %d/%d", i // batch_size + 1, (total_uids + batch_size - 1) // batch_size)
                raise FilterCancelled(f"Đã hủy! Đã xử lý {i}/{total_uids} emails, tìm thấy {len(records)} kết quả.")
            
            batch_uids = uids[i:i + batch_size]
            batch_num = i // batch_size + 1
            total_batches = (total_uids + batch_size - 1) // batch_size
            logger.info("Processing batch %d/%d (emails %d-%d of %d)", 
                       batch_num, total_batches, i + 1, min(i + batch_size, total_uids), total_uids)
            print(f"  → Đang xử lý: {i + 1}-{min(i + batch_size, total_uids)}/{total_uids} emails | Batch {batch_num}/{total_batches} | Tìm thấy: {len(records)} (Nhấn Ctrl+C để hủy)")
            
            # Only fetch what we need: ENVELOPE for quick filtering, BODY.PEEK[TEXT] for snippet
            # Avoid fetching full BODY[] unless body_keywords are specified
            if criteria.body_keywords:
                # Need full body for keyword search
                fetch_parts = ["ENVELOPE", "BODY[]", "FLAGS", "BODYSTRUCTURE"]
            else:
                # Only need headers and structure (much faster)
                fetch_parts = ["ENVELOPE", "BODY.PEEK[TEXT]<0.500>", "FLAGS", "BODYSTRUCTURE"]
            
            try:
                fetch_map = self.connector.fetch(batch_uids, fetch_parts)
            except KeyboardInterrupt:
                self._cancelled = True
                raise FilterCancelled(f"Đã hủy! Đã xử lý {i}/{total_uids} emails, tìm thấy {len(records)} kết quả.")
            
            for uid, message_parts in fetch_map.items():
                if self._cancelled:
                    raise FilterCancelled(f"Đã hủy! Đã xử lý {i + len(fetch_map)}/{total_uids} emails, tìm thấy {len(records)} kết quả.")
                
                try:
                    if b"BODY[]" in message_parts:
                        msg = email.message_from_bytes(message_parts[b"BODY[]"])
                    else:
                        # Build minimal message from ENVELOPE
                        msg = self._build_message_from_envelope(message_parts)
                    
                    has_attachments = self._has_attachments_from_structure(message_parts.get(b"BODYSTRUCTURE"))
                    
                    if not self._matches_subject(criteria, msg):
                        continue
                    if not self._matches_body(criteria, msg):
                        continue
                    if not self._matches_attachment(criteria, has_attachments):
                        continue
                    record = self._build_record(uid, msg, has_attachments)
                    if not self._matches_from(criteria, record.from_address):
                        continue
                    records.append(record)
                except Exception as exc:  # noqa: BLE001
                    logger.exception("Failed to parse message UID %s: %s", uid, exc)
        
        logger.info("Filtered down to %d records after client-side checks", len(records))
        print(f"✓ Hoàn thành! Đã xử lý {total_uids} emails, tìm thấy {len(records)} kết quả phù hợp.")
        return records

    # ------------------------------------------------------------------ #
    # Query building                                                     #
    # ------------------------------------------------------------------ #
    def _build_imap_criteria(self, criteria: EmailFilterCriteria) -> List[str]:
        result: List[str] = ["ALL"]

        if criteria.from_date:
            since = self._to_utc(criteria.from_date)
            result.extend(["SINCE", since.strftime("%d-%b-%Y")])

        if criteria.to_date:
            before = self._to_utc(criteria.to_date) + timedelta(days=1)
            result.extend(["BEFORE", before.strftime("%d-%b-%Y")])

        from_tokens = self._build_from_tokens(criteria)
        result.extend(from_tokens)
        
        # Only add SUBJECT filter for ASCII keywords (server-side filtering)
        # Non-ASCII keywords (Japanese, Vietnamese, etc.) will be filtered client-side
        if criteria.subject_keywords and len(criteria.subject_keywords) == 1:
            keyword = criteria.subject_keywords[0]
            try:
                # Test if keyword is ASCII-safe
                keyword.encode('ascii')
                result.extend(["SUBJECT", keyword])
                logger.debug("Added server-side SUBJECT filter: %s", keyword)
            except UnicodeEncodeError:
                logger.debug("Skipping server-side SUBJECT filter for non-ASCII keyword: %s", keyword)
        
        logger.debug("IMAP search criteria computed: %s", result)
        return result

    def _build_from_tokens(self, criteria: EmailFilterCriteria) -> List[str]:
        tokens: List[str] = []
        values = criteria.from_contains + [f"@{domain}" for domain in criteria.from_domains]
        values = [v for v in values if v]
        if not values:
            return tokens

        if len(values) == 1:
            tokens.extend(["FROM", f'"{values[0]}"'])
        else:
            # Build nested OR statements (IMAP only supports binary OR)
            accumulator = None
            for value in values:
                clause = ['FROM', f'"{value}"']
                if accumulator is None:
                    accumulator = clause
                else:
                    accumulator = ["OR"] + accumulator + clause
            if accumulator:
                tokens.extend(accumulator)
        return tokens

    # ------------------------------------------------------------------ #
    # Criteria checks                                                    #
    # ------------------------------------------------------------------ #
    def _matches_subject(self, criteria: EmailFilterCriteria, msg: Message) -> bool:
        keywords = [kw.strip() for kw in criteria.subject_keywords if kw.strip()]
        if not keywords:
            return True
        subject = self._decode_header(msg.get("Subject", ""))
        result = self._match_tokens(subject, keywords, criteria.normalized_operator())
        if not result and keywords:
            logger.debug("Subject mismatch: '%s' does not contain keywords %s", subject, keywords)
        return result

    def _matches_body(self, criteria: EmailFilterCriteria, msg: Message) -> bool:
        keywords = [kw.strip() for kw in criteria.body_keywords if kw.strip()]
        if not keywords:
            return True
        body_text = self._extract_text(msg)
        return self._match_tokens(body_text, keywords, criteria.normalized_operator())

    def _matches_from(self, criteria: EmailFilterCriteria, from_address: str) -> bool:
        tokens = [t.strip().lower() for t in criteria.from_contains if t.strip()]
        domains = [d.strip().lower() for d in criteria.from_domains if d.strip()]
        if not tokens and not domains:
            return True

        haystack = from_address.lower()
        checks = []
        for token in tokens:
            checks.append(token in haystack)
        for domain in domains:
            checks.append(haystack.endswith(f"@{domain}") or f"@{domain}" in haystack)

        if not checks:
            return True

        return all(checks) if criteria.normalized_operator() == "AND" else any(checks)

    def _matches_attachment(self, criteria: EmailFilterCriteria, has_attachment: bool) -> bool:
        if criteria.has_attachments is None:
            return True
        return has_attachment is criteria.has_attachments

    # ------------------------------------------------------------------ #
    # Record building                                                    #
    # ------------------------------------------------------------------ #
    def _build_record(self, uid: int, msg: Message, has_attachments: bool) -> EmailRecord:
        subject = self._decode_header(msg.get("Subject", ""))
        from_address = self._format_address(msg.get("From", ""))
        date_header = msg.get("Date")
        sent_at = self._parse_date(date_header)
        snippet = self._extract_text(msg, max_length=200)
        to_recipients = self._extract_addresses(msg.get_all("To", []))
        cc_recipients = self._extract_addresses(msg.get_all("Cc", []))
        bcc_recipients = self._extract_addresses(msg.get_all("Bcc", []))
        mailbox = self.connector.config.folder

        return EmailRecord(
            uid=uid,
            subject=subject,
            from_address=from_address,
            sent_at=sent_at,
            snippet=snippet,
            to_recipients=to_recipients,
            cc_recipients=cc_recipients,
            bcc_recipients=bcc_recipients,
            has_attachments=has_attachments,
            mailbox=mailbox,
        )

    # ------------------------------------------------------------------ #
    # Utilities                                                          #
    # ------------------------------------------------------------------ #
    def _extract_text(self, msg: Message, max_length: Optional[int] = None) -> str:
        parts: List[str] = []
        if msg.is_multipart():
            for part in msg.walk():
                if part.get_content_type() == "text/plain" and not part.get_filename():
                    try:
                        charset = part.get_content_charset() or "utf-8"
                        text = part.get_payload(decode=True).decode(charset, errors="replace")
                        parts.append(text)
                    except Exception:  # noqa: BLE001
                        continue
        else:
            payload = msg.get_payload(decode=True) or b""
            charset = msg.get_content_charset() or "utf-8"
            try:
                parts.append(payload.decode(charset, errors="replace"))
            except LookupError:
                parts.append(payload.decode("utf-8", errors="replace"))

        combined = "\n".join(parts)
        combined = re.sub(r"\s+", " ", combined).strip()
        if max_length:
            return combined[:max_length]
        return combined

    def _has_attachments(self, msg: Message) -> bool:
        if msg.is_multipart():
            for part in msg.walk():
                if part.get_content_disposition() in {"attachment", "inline"}:
                    if part.get_filename():
                        return True
        else:
            if msg.get_filename():
                return True
        return False

    def _extract_addresses(self, values: Sequence[str]) -> List[str]:
        if not values:
            return []
        addresses = getaddresses(values)
        normalized = {email_addr.lower() for _, email_addr in addresses if email_addr}
        return sorted(normalized)

    def _decode_header(self, raw_value: str) -> str:
        if not raw_value:
            return ""
        try:
            decoded = make_header(decode_header(raw_value))
            return str(decoded)
        except Exception:  # noqa: BLE001
            return raw_value
    
    def _build_message_from_envelope(self, message_parts: dict) -> Message:
        """Build a minimal email.Message from ENVELOPE data."""
        msg = Message()
        envelope = message_parts.get(b"ENVELOPE")
        if envelope:
            if envelope.subject:
                if isinstance(envelope.subject, bytes):
                    msg["Subject"] = envelope.subject.decode("utf-8", errors="replace")
                else:
                    msg["Subject"] = str(envelope.subject)
            if envelope.from_:
                from_addr = self._format_envelope_address(envelope.from_[0])
                msg["From"] = from_addr
            if envelope.date:
                # envelope.date is already a datetime object
                if isinstance(envelope.date, bytes):
                    msg["Date"] = envelope.date.decode("utf-8", errors="replace")
                else:
                    msg["Date"] = envelope.date.isoformat() if hasattr(envelope.date, 'isoformat') else str(envelope.date)
            
            # Add To, Cc, Bcc from envelope
            if envelope.to:
                to_addrs = [self._format_envelope_address(addr) for addr in envelope.to]
                msg["To"] = ", ".join(to_addrs)
            if envelope.cc:
                cc_addrs = [self._format_envelope_address(addr) for addr in envelope.cc]
                msg["Cc"] = ", ".join(cc_addrs)
            if envelope.bcc:
                bcc_addrs = [self._format_envelope_address(addr) for addr in envelope.bcc]
                msg["Bcc"] = ", ".join(bcc_addrs)
        
        # Add body text if available
        text_part = message_parts.get(b"BODY[TEXT]")
        if text_part:
            msg.set_payload(text_part.decode("utf-8", errors="replace"))
        
        return msg
    
    def _format_envelope_address(self, addr) -> str:
        """Format address from ENVELOPE Address object or tuple."""
        if not addr:
            return ""
        
        # Handle imapclient Address object
        if hasattr(addr, 'mailbox') and hasattr(addr, 'host'):
            mailbox = addr.mailbox
            host = addr.host
            if mailbox and host:
                if isinstance(mailbox, bytes):
                    mailbox = mailbox.decode('utf-8', errors='replace')
                if isinstance(host, bytes):
                    host = host.decode('utf-8', errors='replace')
                return f"{mailbox}@{host}"
            return ""
        
        # Handle tuple format (name, route, mailbox, host)
        if isinstance(addr, (tuple, list)) and len(addr) >= 4:
            mailbox = addr[2]
            host = addr[3]
            if mailbox and host:
                if isinstance(mailbox, bytes):
                    mailbox = mailbox.decode('utf-8', errors='replace')
                if isinstance(host, bytes):
                    host = host.decode('utf-8', errors='replace')
                return f"{mailbox}@{host}"
        
        return ""
    
    def _has_attachments_from_structure(self, bodystructure) -> bool:
        """Quick check for attachments from BODYSTRUCTURE."""
        if not bodystructure:
            return False
        try:
            return self._check_structure_recursive(bodystructure)
        except Exception:  # noqa: BLE001
            return False
    
    def _check_structure_recursive(self, structure) -> bool:
        """Recursively check BODYSTRUCTURE for attachments."""
        if not isinstance(structure, (list, tuple)):
            return False
        
        for item in structure:
            if isinstance(item, (list, tuple)):
                if self._check_structure_recursive(item):
                    return True
            elif isinstance(item, bytes):
                item_lower = item.lower()
                if b"attachment" in item_lower or b"filename" in item_lower:
                    return True
        return False

    def _match_tokens(self, haystack: str, keywords: Iterable[str], operator: str) -> bool:
        # Case-insensitive for ASCII, case-sensitive for Unicode (CJK, etc.)
        checks = []
        for kw in keywords:
            # Try case-insensitive first (for English)
            if kw.lower() in haystack.lower():
                checks.append(True)
            # Fallback to exact match (for CJK and other Unicode)
            elif kw in haystack:
                checks.append(True)
            else:
                checks.append(False)
        
        if not any(checks):
            logger.debug("Token match failed: haystack='%s', keywords=%s", haystack[:100], list(keywords))
        return all(checks) if operator == "AND" else any(checks)

    def _parse_date(self, value: Optional[str]) -> datetime:
        if not value:
            return datetime.now(timezone.utc)
        try:
            parsed = parsedate_to_datetime(value)
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            return parsed.astimezone(timezone.utc)
        except Exception:  # noqa: BLE001
            return datetime.now(timezone.utc)

    def _format_address(self, raw: str) -> str:
        if not raw:
            return ""
        addresses = self._extract_addresses([raw])
        return addresses[0] if addresses else raw

    def _to_utc(self, value: datetime) -> datetime:
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)


def collect_unique_recipients(records: Sequence[EmailRecord]) -> List[str]:
    """Aggregate recipients from To/Cc/Bcc fields and de-duplicate."""
    recipients: set[str] = set()
    for record in records:
        recipients.update(record.to_recipients)
        recipients.update(record.cc_recipients)
        recipients.update(record.bcc_recipients)
    return sorted(recipients)

