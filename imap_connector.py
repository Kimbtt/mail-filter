"""Utilities for connecting to IMAP servers."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Iterable, List, Optional, Sequence

from imapclient import IMAPClient

logger = logging.getLogger(__name__)


@dataclass
class IMAPConnectionConfig:
    """Configuration values required to establish an IMAP connection."""

    server: str
    port: int = 993
    use_ssl: bool = True
    folder: str = "INBOX"
    auth_mechanism: str = "LOGIN"  # LOGIN | XOAUTH2


class IMAPConnector:
    """Lightweight wrapper around :class:`imapclient.IMAPClient`."""

    def __init__(self, config: IMAPConnectionConfig):
        self.config = config
        self._client: Optional[IMAPClient] = None

    # ------------------------------------------------------------------ #
    # Connection lifecycle                                               #
    # ------------------------------------------------------------------ #
    def connect(self) -> None:
        if self._client is not None:
            return
        logger.debug(
            "Opening IMAP connection to %s:%s (ssl=%s)",
            self.config.server,
            self.config.port,
            self.config.use_ssl,
        )
        self._client = IMAPClient(
            host=self.config.server, port=self.config.port, ssl=self.config.use_ssl
        )
        # Set UTF-8 encoding to support Unicode characters in credentials and search
        if hasattr(self._client, '_imap') and hasattr(self._client._imap, '_encoding'):
            self._client._imap._encoding = 'utf-8'

    def login(self, username: str, secret: str) -> None:
        if self._client is None:
            self.connect()

        mechanism = self.config.auth_mechanism.upper()
        if mechanism == "XOAUTH2":
            logger.info("Authenticating via XOAUTH2 for %s", username)
            self._client.oauth2_login(username, secret)
        else:
            logger.info("Authenticating via LOGIN for %s", username)
            self._client.login(username, secret)

        self._client.select_folder(self.config.folder, readonly=False)
        logger.debug("Selected folder %s", self.config.folder)

    def logout(self) -> None:
        if self._client is None:
            return
        try:
            self._client.logout()
        finally:
            self._client = None
            logger.debug("IMAP connection closed")

    # ------------------------------------------------------------------ #
    # Helpers                                                            #
    # ------------------------------------------------------------------ #
    @property
    def client(self) -> IMAPClient:
        if self._client is None:
            raise RuntimeError("IMAP client is not connected. Call connect/login first.")
        return self._client

    def list_folders(self) -> List[bytes]:
        folders = self.client.list_folders()
        logger.debug("Fetched %d folders", len(folders))
        return folders

    def change_folder(self, folder: str, readonly: bool = False) -> None:
        logger.info("Switching to folder %s", folder)
        self.client.select_folder(folder, readonly=readonly)

    def search(self, criteria: Sequence[str]) -> List[int]:
        logger.debug("Running IMAP search with criteria: %s", criteria)
        # Try UTF-8 first for better Unicode support, but catch specific errors
        try:
            uids = self.client.search(criteria, charset='UTF-8')
            logger.info("Search returned %d UIDs", len(uids))
            return uids
        except (UnicodeEncodeError, TypeError) as e:
            # UTF-8 failed, try without charset (ASCII)
            logger.warning("UTF-8 search failed (%s), using default charset", type(e).__name__)
            try:
                uids = self.client.search(criteria)
                logger.info("Search returned %d UIDs", len(uids))
                return uids
            except Exception as e:
                logger.error("Search failed: %s", e)
                raise

    def fetch(self, uids: Iterable[int], fetch_parts: Sequence[str]) -> dict:
        uid_list = list(uids)
        if not uid_list:
            return {}
        logger.debug("Fetching %d messages with parts %s", len(uid_list), fetch_parts)
        return self.client.fetch(uid_list, fetch_parts)

    # ------------------------------------------------------------------ #
    # Message actions                                                    #
    # ------------------------------------------------------------------ #
    def add_flags(self, uids: Iterable[int], flags: Sequence[str]) -> None:
        uid_list = list(uids)
        if not uid_list:
            return
        self.client.add_flags(uid_list, list(flags))
        logger.info("Added flags %s to %d messages", flags, len(uid_list))

    def remove_flags(self, uids: Iterable[int], flags: Sequence[str]) -> None:
        uid_list = list(uids)
        if not uid_list:
            return
        self.client.remove_flags(uid_list, list(flags))
        logger.info("Removed flags %s from %d messages", flags, len(uid_list))

    def add_gmail_labels(self, uids: Iterable[int], labels: Sequence[str]) -> None:
        """Add Gmail labels when supported."""
        uid_list = list(uids)
        if not uid_list or not labels:
            return
        client = self.client
        if hasattr(client, "add_gmail_labels"):
            client.add_gmail_labels(uid_list, list(labels))
            logger.info("Added Gmail labels %s to %d messages", labels, len(uid_list))
        else:
            logger.warning("add_gmail_labels not supported by this IMAP server")

    def move(self, uids: Iterable[int], destination: str) -> None:
        uid_list = list(uids)
        if not uid_list:
            return
        self.client.move(uid_list, destination)
        logger.info("Moved %d messages to %s", len(uid_list), destination)

    def delete(self, uids: Iterable[int]) -> None:
        uid_list = list(uids)
        if not uid_list:
            return
        self.client.delete_messages(uid_list)
        self.client.expunge()
        logger.info("Deleted %d messages", len(uid_list))

