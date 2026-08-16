from __future__ import annotations

import asyncio
import secrets
import sqlite3
import threading
import time
from dataclasses import dataclass
from pathlib import Path

from bot_urls import normalize_url


@dataclass(frozen=True)
class StoredFile:
    token: str
    channel_id: int
    message_id: int
    name: str
    title: str
    url: str
    mime_type: str
    size: int
    owner_id: int
    created_at: float
    archive_key: str | None = None


@dataclass(frozen=True)
class StoreStats:
    count: int
    total_size: int


class FileStore:
    """Persistent metadata index for Telegram channel-backed file storage.

    Media remains in Telegram. SQLite only stores the channel/message reference
    and searchable metadata, so restarts do not invalidate stored files.
    """

    def __init__(self, path: Path) -> None:
        self.path = Path(path)
        self._connection: sqlite3.Connection | None = None
        self._lock = threading.RLock()

    async def start(self) -> None:
        await asyncio.to_thread(self._start)

    def _start(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(
            self.path,
            timeout=10,
            check_same_thread=False,
        )
        connection.row_factory = sqlite3.Row
        with self._lock:
            self._connection = connection
            connection.execute("PRAGMA journal_mode=WAL")
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS stored_files (
                    token TEXT PRIMARY KEY,
                    channel_id INTEGER NOT NULL,
                    message_id INTEGER NOT NULL,
                    name TEXT NOT NULL,
                    title TEXT NOT NULL,
                    url TEXT NOT NULL DEFAULT '',
                    mime_type TEXT NOT NULL DEFAULT 'application/octet-stream',
                    size INTEGER NOT NULL DEFAULT 0,
                    owner_id INTEGER NOT NULL,
                    created_at REAL NOT NULL,
                    archive_key TEXT,
                    UNIQUE(channel_id, message_id)
                )
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS banned_users (
                    user_id INTEGER PRIMARY KEY,
                    banned_by INTEGER NOT NULL,
                    banned_at REAL NOT NULL
                )
                """
            )
            columns = {
                str(row["name"])
                for row in connection.execute("PRAGMA table_info(stored_files)").fetchall()
            }
            if "archive_key" not in columns:
                connection.execute("ALTER TABLE stored_files ADD COLUMN archive_key TEXT")
            connection.execute("DROP INDEX IF EXISTS idx_stored_files_archive_key")
            connection.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS idx_stored_files_archive_key "
                "ON stored_files(channel_id, archive_key) "
                "WHERE archive_key IS NOT NULL"
            )
            connection.execute(
                "CREATE INDEX IF NOT EXISTS idx_stored_files_search "
                "ON stored_files(title, name, created_at)"
            )
            connection.execute(
                "CREATE INDEX IF NOT EXISTS idx_stored_files_owner "
                "ON stored_files(owner_id, created_at)"
            )
            connection.commit()

    async def ban_user(self, user_id: int, banned_by: int) -> bool:
        return await asyncio.to_thread(self._ban_user, user_id, banned_by)

    def _ban_user(self, user_id: int, banned_by: int) -> bool:
        connection = self._require_connection()
        with self._lock:
            cursor = connection.execute(
                """
                INSERT OR IGNORE INTO banned_users (user_id, banned_by, banned_at)
                VALUES (?, ?, ?)
                """,
                (int(user_id), int(banned_by), time.time()),
            )
            connection.commit()
            return cursor.rowcount > 0

    async def unban_user(self, user_id: int) -> bool:
        return await asyncio.to_thread(self._unban_user, user_id)

    def _unban_user(self, user_id: int) -> bool:
        connection = self._require_connection()
        with self._lock:
            cursor = connection.execute(
                "DELETE FROM banned_users WHERE user_id = ?",
                (int(user_id),),
            )
            connection.commit()
            return cursor.rowcount > 0

    async def is_user_banned(self, user_id: int) -> bool:
        return await asyncio.to_thread(self._is_user_banned, user_id)

    def _is_user_banned(self, user_id: int) -> bool:
        connection = self._require_connection()
        with self._lock:
            row = connection.execute(
                "SELECT 1 FROM banned_users WHERE user_id = ?",
                (int(user_id),),
            ).fetchone()
        return row is not None

    async def close(self) -> None:
        await asyncio.to_thread(self._close)

    def _close(self) -> None:
        with self._lock:
            if self._connection:
                self._connection.close()
                self._connection = None

    async def add(
        self,
        *,
        channel_id: int,
        message_id: int,
        name: str,
        title: str | None = None,
        url: str = "",
        mime_type: str | None = None,
        size: int = 0,
        owner_id: int = 0,
        archive_key: str | None = None,
    ) -> StoredFile:
        return await asyncio.to_thread(
            self._add,
            channel_id,
            message_id,
            name,
            title or name,
            url,
            mime_type or "application/octet-stream",
            max(0, int(size)),
            owner_id,
            archive_key,
        )

    def _add(
        self,
        channel_id: int,
        message_id: int,
        name: str,
        title: str,
        url: str,
        mime_type: str,
        size: int,
        owner_id: int,
        archive_key: str | None,
    ) -> StoredFile:
        connection = self._require_connection()
        safe_name = Path(name or "download").name or "download"
        safe_title = (title or safe_name).strip()[:500] or safe_name
        with self._lock:
            existing = connection.execute(
                "SELECT * FROM stored_files WHERE channel_id = ? AND message_id = ?",
                (channel_id, message_id),
            ).fetchone()
            if existing:
                return self._row(existing)
            if archive_key:
                existing = connection.execute(
                    "SELECT * FROM stored_files WHERE channel_id = ? AND archive_key = ?",
                    (channel_id, archive_key),
                ).fetchone()
                if existing:
                    return self._row(existing)
            for _ in range(5):
                token = secrets.token_urlsafe(12)
                try:
                    connection.execute(
                        """
                        INSERT INTO stored_files (
                            token, channel_id, message_id, name, title, url,
                            mime_type, size, owner_id, created_at, archive_key
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            token,
                            channel_id,
                            message_id,
                            safe_name,
                            safe_title,
                            url,
                            mime_type[:160],
                            size,
                            owner_id,
                            time.time(),
                            archive_key,
                        ),
                    )
                    connection.commit()
                    row = connection.execute(
                        "SELECT * FROM stored_files WHERE token = ?", (token,)
                    ).fetchone()
                    if row:
                        return self._row(row)
                except sqlite3.IntegrityError:
                    if archive_key:
                        existing = connection.execute(
                            "SELECT * FROM stored_files "
                            "WHERE channel_id = ? AND archive_key = ?",
                            (channel_id, archive_key),
                        ).fetchone()
                        if existing:
                            connection.rollback()
                            return self._row(existing)
                    continue
        raise RuntimeError("Could not create a unique file-store token.")

    async def find_archive(
        self,
        *,
        channel_id: int,
        archive_key: str,
        url: str,
        audio_only: bool,
        size: int,
    ) -> StoredFile | None:
        return await asyncio.to_thread(
            self._find_archive,
            channel_id,
            archive_key,
            url,
            audio_only,
            max(0, int(size)),
        )

    async def find_archive_by_key(
        self,
        *,
        channel_id: int,
        archive_key: str,
    ) -> StoredFile | None:
        return await asyncio.to_thread(
            self._find_archive_by_key,
            channel_id,
            archive_key,
        )

    async def find_archives_by_urls(
        self,
        *,
        channel_id: int,
        urls: list[str],
    ) -> list[StoredFile]:
        if not urls:
            return []
        return await asyncio.to_thread(
            self._find_archives_by_urls,
            channel_id,
            tuple(urls),
        )

    def _find_archives_by_urls(
        self,
        channel_id: int,
        urls: tuple[str, ...],
    ) -> list[StoredFile]:
        connection = self._require_connection()
        placeholders = ",".join("?" for _ in urls)
        with self._lock:
            rows = connection.execute(
                f"SELECT * FROM stored_files "
                f"WHERE channel_id = ? AND url IN ({placeholders})",
                (channel_id, *urls),
            ).fetchall()
        by_url = {str(row["url"]): self._row(row) for row in rows}
        return [by_url[url] for url in urls if url in by_url]

    def _find_archive_by_key(
        self,
        channel_id: int,
        archive_key: str,
    ) -> StoredFile | None:
        connection = self._require_connection()
        with self._lock:
            row = connection.execute(
                "SELECT * FROM stored_files "
                "WHERE channel_id = ? AND archive_key = ?",
                (channel_id, archive_key),
            ).fetchone()
        return self._row(row) if row else None

    def _find_archive(
        self,
        channel_id: int,
        archive_key: str,
        url: str,
        audio_only: bool,
        size: int,
    ) -> StoredFile | None:
        connection = self._require_connection()
        with self._lock:
            row = connection.execute(
                "SELECT * FROM stored_files "
                "WHERE channel_id = ? AND archive_key = ?",
                (channel_id, archive_key),
            ).fetchone()
            if row:
                return self._row(row)

            # Older archive rows predate archive_key. Match them conservatively
            # by normalized source, media kind, and exact byte size so they can
            # still benefit from deduplication after the schema migration.
            rows = connection.execute(
                "SELECT * FROM stored_files "
                "WHERE channel_id = ? AND archive_key IS NULL "
                "AND url <> '' AND size = ? "
                "ORDER BY created_at DESC LIMIT 100",
                (channel_id, size),
            ).fetchall()
        normalized = normalize_url(url)
        for candidate in rows:
            candidate_url = str(candidate["url"])
            if normalize_url(candidate_url) != normalized:
                continue
            candidate_name = str(candidate["name"]).lower()
            candidate_mime = str(candidate["mime_type"]).lower()
            candidate_audio = (
                candidate_mime.startswith("audio/")
                or candidate_name.endswith((".mp3", ".m4a", ".aac", ".ogg", ".opus", ".wav", ".flac"))
            )
            if candidate_audio == audio_only:
                return self._row(candidate)
        return None

    async def get(self, token: str) -> StoredFile | None:
        return await asyncio.to_thread(self._get, token)

    def _get(self, token: str) -> StoredFile | None:
        connection = self._require_connection()
        with self._lock:
            row = connection.execute(
                "SELECT * FROM stored_files WHERE token = ?", (token,)
            ).fetchone()
        return self._row(row) if row else None

    async def search(
        self,
        query: str,
        *,
        owner_id: int | None = None,
        limit: int = 10,
    ) -> list[StoredFile]:
        return await asyncio.to_thread(
            self._search,
            query.strip(),
            owner_id,
            max(1, min(50, limit)),
        )

    def _search(
        self,
        query: str,
        owner_id: int | None,
        limit: int,
    ) -> list[StoredFile]:
        connection = self._require_connection()
        pattern = f"%{query}%"
        sql = (
            "SELECT * FROM stored_files "
            "WHERE (title LIKE ? OR name LIKE ? OR url LIKE ?)"
        )
        params: list[object] = [pattern, pattern, pattern]
        if owner_id is not None:
            sql += " AND owner_id = ?"
            params.append(owner_id)
        sql += " ORDER BY created_at DESC LIMIT ?"
        params.append(limit)
        with self._lock:
            rows = connection.execute(sql, params).fetchall()
        return [self._row(row) for row in rows]

    async def recent(
        self,
        *,
        owner_id: int | None = None,
        limit: int = 10,
    ) -> list[StoredFile]:
        return await asyncio.to_thread(
            self._recent,
            owner_id,
            max(1, min(50, limit)),
        )

    def _recent(self, owner_id: int | None, limit: int) -> list[StoredFile]:
        connection = self._require_connection()
        sql = "SELECT * FROM stored_files"
        params: list[object] = []
        if owner_id is not None:
            sql += " WHERE owner_id = ?"
            params.append(owner_id)
        sql += " ORDER BY created_at DESC LIMIT ?"
        params.append(limit)
        with self._lock:
            rows = connection.execute(sql, params).fetchall()
        return [self._row(row) for row in rows]

    async def stats(self, owner_id: int | None = None) -> StoreStats:
        return await asyncio.to_thread(self._stats, owner_id)

    def _stats(self, owner_id: int | None) -> StoreStats:
        connection = self._require_connection()
        sql = "SELECT COUNT(*) AS count, COALESCE(SUM(size), 0) AS total_size FROM stored_files"
        params: tuple[object, ...] = ()
        if owner_id is not None:
            sql += " WHERE owner_id = ?"
            params = (owner_id,)
        with self._lock:
            row = connection.execute(sql, params).fetchone()
        return StoreStats(int(row["count"]), int(row["total_size"]))

    async def delete(self, token: str) -> StoredFile | None:
        return await asyncio.to_thread(self._delete, token)

    def _delete(self, token: str) -> StoredFile | None:
        connection = self._require_connection()
        with self._lock:
            row = connection.execute(
                "SELECT * FROM stored_files WHERE token = ?", (token,)
            ).fetchone()
            if not row:
                return None
            connection.execute("DELETE FROM stored_files WHERE token = ?", (token,))
            connection.commit()
        return self._row(row)

    def _require_connection(self) -> sqlite3.Connection:
        if not self._connection:
            raise RuntimeError("File store is not started.")
        return self._connection

    @staticmethod
    def _row(row: sqlite3.Row) -> StoredFile:
        return StoredFile(
            token=str(row["token"]),
            channel_id=int(row["channel_id"]),
            message_id=int(row["message_id"]),
            name=str(row["name"]),
            title=str(row["title"]),
            url=str(row["url"]),
            mime_type=str(row["mime_type"]),
            size=int(row["size"]),
            owner_id=int(row["owner_id"]),
            created_at=float(row["created_at"]),
            archive_key=str(row["archive_key"]) if row["archive_key"] else None,
        )