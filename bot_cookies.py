from __future__ import annotations

import json
import os
import re
import tempfile
from pathlib import Path
from typing import Any

from cryptography.fernet import Fernet, InvalidToken


class CookieStore:
    """Stores per-user Netscape cookie files encrypted at rest."""

    def __init__(self, directory: Path, key: bytes) -> None:
        self.directory = directory
        self.fernet = Fernet(key)
        self.directory.mkdir(parents=True, exist_ok=True)
        self.directory.chmod(0o700)
        self._purge_materialized()

    def _purge_materialized(self) -> None:
        """Remove plaintext cookie files left by an interrupted process."""
        for path in self.directory.glob("*.cookies.txt"):
            try:
                path.unlink()
            except OSError:
                continue

    def _path(self, user_id: int) -> Path:
        return self.directory / f"{user_id}.cookies.enc"

    def save(self, user_id: int, contents: bytes) -> None:
        normalized = normalize_cookie_export(contents)
        path = self._path(user_id)
        encrypted = self.fernet.encrypt(normalized)
        fd, temporary_name = tempfile.mkstemp(
            prefix=f".{user_id}.",
            suffix=".cookies.enc",
            dir=self.directory,
        )
        temporary = Path(temporary_name)
        try:
            os.fchmod(fd, 0o600)
            with os.fdopen(fd, "wb") as handle:
                fd = -1
                handle.write(encrypted)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, path)
        finally:
            if fd != -1:
                os.close(fd)
            temporary.unlink(missing_ok=True)
        path.chmod(0o600)

    def path_for(self, user_id: int) -> Path | None:
        path = self._path(user_id)
        if not path.exists():
            return None
        try:
            decrypted = self.fernet.decrypt(path.read_bytes())
        except InvalidToken as exc:
            raise RuntimeError("Stored cookie file cannot be decrypted.") from exc
        fd, temporary_name = tempfile.mkstemp(
            suffix=".cookies.txt",
            prefix=f"{user_id}-",
            dir=self.directory,
        )
        temp = Path(temporary_name)
        completed = False
        try:
            os.fchmod(fd, 0o600)
            with os.fdopen(fd, "wb") as handle:
                fd = -1
                handle.write(decrypted)
                handle.flush()
                os.fsync(handle.fileno())
            completed = True
        finally:
            if fd != -1:
                os.close(fd)
            if not completed:
                temp.unlink(missing_ok=True)
        return temp

    def sync_back(self, user_id: int, path: Path | None) -> bool:
        """Persist cookie updates yt-dlp wrote into the materialized file.

        yt-dlp (and the underlying ``http.cookiejar``) rewrites the
        ``cookiefile`` it was given at the end of every run, picking up
        rotated/renewed YouTube session cookies along the way. Previously
        that materialized copy was thrown away by ``cleanup_materialized``
        immediately after use, so the encrypted store never advanced past
        the exact snapshot a user originally uploaded -- one of the main
        reasons stored YouTube cookies quietly go stale after a while.
        Calling this before cleanup lets the store track those updates, the
        same way a plain ``--cookies cookies.txt`` file would on disk.

        Best-effort: any read/validation failure here must never turn into
        a user-facing error for what was otherwise a successful download,
        so problems are swallowed and simply skipped.
        """
        if not path:
            return False
        try:
            if not path.exists():
                return False
            raw = path.read_bytes()
            normalized = normalize_cookie_export(raw)
        except (OSError, ValueError):
            return False
        existing_path = self._path(user_id)
        if existing_path.exists():
            try:
                if self.fernet.decrypt(existing_path.read_bytes()) == normalized:
                    return False
            except InvalidToken:
                pass
        try:
            self.save(user_id, normalized)
        except OSError:
            return False
        return True

    @staticmethod
    def cleanup_materialized(path: Path | None) -> None:
        if path:
            path.unlink(missing_ok=True)

    def delete(self, user_id: int) -> bool:
        path = self._path(user_id)
        if not path.exists():
            return False
        path.unlink()
        return True

    def has(self, user_id: int) -> bool:
        return self._path(user_id).exists()

    def health(self, user_id: int) -> dict[str, Any]:
        """Return safe cookie diagnostics without exposing cookie values."""
        path = self._path(user_id)
        if not path.exists():
            return {
                "present": False,
                "records": 0,
                "youtube_domains": [],
                "expired": 0,
            }
        try:
            contents = self.fernet.decrypt(path.read_bytes())
            lines = contents.decode("utf-8", "strict").splitlines()
        except (InvalidToken, UnicodeError, ValueError):
            return {
                "present": True,
                "valid": False,
                "records": 0,
                "youtube_domains": [],
                "expired": 0,
            }

        import time

        domains: set[str] = set()
        expired = 0
        records = 0
        now = int(time.time())
        for line in lines:
            if not line.strip() or line.startswith("#"):
                continue
            fields = line.split("\t")
            if len(fields) < 7:
                continue
            records += 1
            domain = fields[0].lower()
            if "youtube" in domain or "google" in domain:
                domains.add(domain)
            try:
                expiry = int(fields[4])
                if expiry and expiry < now:
                    expired += 1
            except ValueError:
                continue
        return {
            "present": True,
            "valid": records > 0,
            "records": records,
            "youtube_domains": sorted(domains),
            "expired": expired,
        }


def looks_like_cookie_document(file_name: str | None) -> bool:
    return bool(re.search(r"cookie", file_name or "", re.IGNORECASE)) or (
        (file_name or "").lower().endswith((".txt", ".json"))
    )


def normalize_cookie_export(contents: bytes) -> bytes:
    """Accept Netscape cookies.txt and common browser JSON exports."""
    if not contents.strip():
        raise ValueError("Cookie file is empty.")
    if len(contents) > 2_000_000:
        raise ValueError("Cookie file is too large.")
    if b"\x00" in contents:
        raise ValueError("Cookie file must be text.")
    text = contents.decode("utf-8", "strict")
    if text.lstrip().startswith(("# Netscape", "# HTTP Cookie File")):
        lines = text.splitlines()
        meaningful = [line for line in lines if line.strip() and not line.startswith("#")]
        if meaningful and any(len(line.split("\t")) < 7 for line in meaningful):
            raise ValueError("Cookie file is not valid Netscape format.")
        return contents if contents.endswith(b"\n") else contents + b"\n"

    try:
        payload = json.loads(text)
    except json.JSONDecodeError as exc:
        raise ValueError(
            "Upload a Netscape cookies.txt file or a browser JSON cookie export."
        ) from exc
    records: list[dict[str, Any]]
    if isinstance(payload, list):
        records = [record for record in payload if isinstance(record, dict)]
    elif isinstance(payload, dict):
        candidates = payload.get("cookies")
        records = [record for record in candidates if isinstance(record, dict)] if isinstance(candidates, list) else []
    else:
        records = []
    if not records:
        raise ValueError("No browser cookie records were found.")

    output = ["# Netscape HTTP Cookie File"]
    accepted = 0
    for record in records:
        domain = str(record.get("domain", "")).strip()
        name = str(record.get("name", "")).strip()
        value = record.get("value")
        path = str(record.get("path") or "/")
        if not domain or not name or not isinstance(value, str):
            continue
        if not domain.startswith(".") and not bool(record.get("hostOnly", False)):
            domain = f".{domain}"
        secure = "TRUE" if bool(record.get("secure", False)) else "FALSE"
        include_subdomains = "FALSE" if bool(record.get("hostOnly", False)) else "TRUE"
        expiration = record.get("expirationDate", 0)
        try:
            expiry = str(max(0, int(float(expiration or 0))))
        except (TypeError, ValueError):
            expiry = "0"
        output.append(
            "\t".join(
                [domain, include_subdomains, path, secure, expiry, name, value]
            )
        )
        accepted += 1
    if not accepted:
        raise ValueError("No usable browser cookies were found.")
    return ("\n".join(output) + "\n").encode("utf-8")