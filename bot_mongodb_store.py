from __future__ import annotations

import asyncio
import logging
import re
import secrets
import sqlite3
import time
from pathlib import Path
from typing import Any

from pymongo import ASCENDING, DESCENDING, MongoClient
from pymongo.errors import DuplicateKeyError

from bot_file_store import FileStore, StoreStats, StoredFile
from bot_urls import normalize_url

logger = logging.getLogger("ytdlbot.mongodb")


class MongoFileStore(FileStore):
    """MongoDB-backed metadata index compatible with the SQLite FileStore API."""

    def __init__(self, url: str, database_name: str = "ytdlbot") -> None:
        self.url = url
        self.database_name = database_name.strip() or "ytdlbot"
        self.client: MongoClient[Any] | None = None
        self.collection: Any = None
        self.banned_collection: Any = None

    async def start(self) -> None:
        await asyncio.to_thread(self._start)

    def _start(self) -> None:
        client = MongoClient(
            self.url,
            serverSelectionTimeoutMS=10_000,
            connectTimeoutMS=10_000,
            appname="ytdlbot",
        )
        client.admin.command("ping")
        database = client[self.database_name]
        collection = database["stored_files"]
        banned_collection = database["banned_users"]
        collection.create_index([("token", ASCENDING)], unique=True)
        collection.create_index(
            [("channel_id", ASCENDING), ("message_id", ASCENDING)],
            unique=True,
        )
        collection.create_index(
            [("channel_id", ASCENDING), ("archive_key", ASCENDING)],
            unique=True,
            partialFilterExpression={"archive_key": {"$type": "string"}},
        )
        collection.create_index(
            [("title", ASCENDING), ("name", ASCENDING), ("created_at", DESCENDING)]
        )
        collection.create_index([("owner_id", ASCENDING), ("created_at", DESCENDING)])
        banned_collection.create_index([("user_id", ASCENDING)], unique=True)
        self.client = client
        self.collection = collection
        self.banned_collection = banned_collection

    async def close(self) -> None:
        await asyncio.to_thread(self._close)

    async def migrate_from_sqlite(self, path: Path) -> int:
        """Idempotently copy legacy SQLite metadata into MongoDB."""
        return await asyncio.to_thread(self._migrate_from_sqlite, Path(path))

    def _migrate_from_sqlite(self, path: Path) -> int:
        collection = self._require_collection()
        if not path.exists():
            return 0
        connection = sqlite3.connect(path)
        connection.row_factory = sqlite3.Row
        migrated = 0
        try:
            tables = {
                str(row["name"])
                for row in connection.execute(
                    "SELECT name FROM sqlite_master WHERE type = 'table'"
                ).fetchall()
            }
            if "stored_files" not in tables:
                return 0
            rows = connection.execute("SELECT * FROM stored_files").fetchall()
            for row in rows:
                columns = set(row.keys())
                document = {
                    "token": str(row["token"] or ""),
                    "channel_id": int(row["channel_id"] or 0),
                    "message_id": int(row["message_id"] or 0),
                    "name": str(row["name"] or "download"),
                    "title": str(row["title"] or row["name"] or "download"),
                    "url": str(row["url"] or ""),
                    "mime_type": str(
                        row["mime_type"] or "application/octet-stream"
                    ),
                    "size": int(row["size"] or 0),
                    "owner_id": int(row["owner_id"] or 0),
                    "created_at": float(row["created_at"] or 0),
                    "archive_key": (
                        str(row["archive_key"])
                        if "archive_key" in columns and row["archive_key"]
                        else None
                    ),
                }
                if not document["token"] or not document["channel_id"] or not document["message_id"]:
                    logger.warning("Skipping malformed legacy stored_files row during migration")
                    continue
                result = collection.update_one(
                    {
                        "channel_id": document["channel_id"],
                        "message_id": document["message_id"],
                    },
                    {"$setOnInsert": document},
                    upsert=True,
                )
                migrated += int(result.upserted_id is not None)
            if self.banned_collection is not None and "banned_users" in tables:
                for row in connection.execute("SELECT * FROM banned_users"):
                    columns = set(row.keys())
                    self.banned_collection.update_one(
                        {"user_id": int(row["user_id"])},
                        {
                            "$setOnInsert": {
                                "user_id": int(row["user_id"]),
                                "banned_by": int(row["banned_by"] or 0)
                                if "banned_by" in columns
                                else 0,
                                "banned_at": float(row["banned_at"] or 0)
                                if "banned_at" in columns
                                else 0,
                            }
                        },
                        upsert=True,
                    )
        finally:
            connection.close()
        return migrated

    def _close(self) -> None:
        if self.client:
            self.client.close()
        self.client = None
        self.collection = None
        self.banned_collection = None

    def _require_collection(self) -> Any:
        if self.collection is None or self.banned_collection is None:
            raise RuntimeError("MongoDB file store is not started.")
        return self.collection

    async def ban_user(self, user_id: int, banned_by: int) -> bool:
        return await asyncio.to_thread(self._ban_user, user_id, banned_by)

    def _ban_user(self, user_id: int, banned_by: int) -> bool:
        if self.banned_collection is None:
            raise RuntimeError("MongoDB file store is not started.")
        try:
            self.banned_collection.insert_one(
                {
                    "user_id": int(user_id),
                    "banned_by": int(banned_by),
                    "banned_at": time.time(),
                }
            )
            return True
        except DuplicateKeyError:
            return False

    async def unban_user(self, user_id: int) -> bool:
        return await asyncio.to_thread(self._unban_user, user_id)

    def _unban_user(self, user_id: int) -> bool:
        if self.banned_collection is None:
            raise RuntimeError("MongoDB file store is not started.")
        return self.banned_collection.delete_one({"user_id": int(user_id)}).deleted_count > 0

    async def is_user_banned(self, user_id: int) -> bool:
        return await asyncio.to_thread(self._is_user_banned, user_id)

    def _is_user_banned(self, user_id: int) -> bool:
        if self.banned_collection is None:
            raise RuntimeError("MongoDB file store is not started.")
        return self.banned_collection.find_one(
            {"user_id": int(user_id)}, {"_id": 1}
        ) is not None

    @staticmethod
    def _document_to_stored(document: dict[str, Any] | None) -> StoredFile | None:
        if not document:
            return None
        return StoredFile(
            token=str(document["token"]),
            channel_id=int(document["channel_id"]),
            message_id=int(document["message_id"]),
            name=str(document["name"]),
            title=str(document["title"]),
            url=str(document.get("url") or ""),
            mime_type=str(document.get("mime_type") or "application/octet-stream"),
            size=int(document.get("size") or 0),
            owner_id=int(document.get("owner_id") or 0),
            created_at=float(document.get("created_at") or 0),
            archive_key=(
                str(document["archive_key"])
                if document.get("archive_key")
                else None
            ),
        )

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
            int(channel_id),
            int(message_id),
            name,
            title or name,
            url,
            mime_type or "application/octet-stream",
            max(0, int(size)),
            int(owner_id),
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
        collection = self._require_collection()
        safe_name = Path(name or "download").name or "download"
        document = {
            "token": secrets.token_urlsafe(12),
            "channel_id": channel_id,
            "message_id": message_id,
            "name": safe_name,
            "title": (title or safe_name).strip()[:500] or safe_name,
            "url": url,
            "mime_type": mime_type[:160],
            "size": size,
            "owner_id": owner_id,
            "created_at": time.time(),
            "archive_key": archive_key,
        }
        existing = collection.find_one(
            {"channel_id": channel_id, "message_id": message_id}
        )
        if existing:
            return self._document_to_stored(existing)  # type: ignore[return-value]
        if archive_key:
            existing = collection.find_one(
                {"channel_id": channel_id, "archive_key": archive_key}
            )
            if existing:
                return self._document_to_stored(existing)  # type: ignore[return-value]
        for _ in range(5):
            try:
                collection.insert_one(document)
                return self._document_to_stored(document)  # type: ignore[return-value]
            except DuplicateKeyError:
                existing = collection.find_one(
                    {"channel_id": channel_id, "message_id": message_id}
                )
                if not existing and archive_key:
                    existing = collection.find_one(
                        {"channel_id": channel_id, "archive_key": archive_key}
                    )
                if existing:
                    return self._document_to_stored(existing)  # type: ignore[return-value]
                document["token"] = secrets.token_urlsafe(12)
        raise RuntimeError("Could not create a unique MongoDB file-store token.")

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
            int(channel_id),
            archive_key,
            url,
            audio_only,
            max(0, int(size)),
        )

    def _find_archive(
        self,
        channel_id: int,
        archive_key: str,
        url: str,
        audio_only: bool,
        size: int,
    ) -> StoredFile | None:
        collection = self._require_collection()
        document = collection.find_one(
            {"channel_id": channel_id, "archive_key": archive_key}
        )
        if document:
            return self._document_to_stored(document)
        candidates = collection.find(
            {
                "channel_id": channel_id,
                "archive_key": None,
                "url": {"$ne": ""},
                "size": size,
            }
        ).sort("created_at", DESCENDING).limit(100)
        normalized = normalize_url(url)
        for candidate in candidates:
            if normalize_url(str(candidate.get("url") or "")) != normalized:
                continue
            name = str(candidate.get("name") or "").lower()
            mime_type = str(candidate.get("mime_type") or "").lower()
            candidate_audio = mime_type.startswith("audio/") or name.endswith(
                (".mp3", ".m4a", ".aac", ".ogg", ".opus", ".wav", ".flac")
            )
            if candidate_audio == audio_only:
                return self._document_to_stored(candidate)
        return None

    async def find_archive_by_key(
        self, *, channel_id: int, archive_key: str
    ) -> StoredFile | None:
        return await asyncio.to_thread(
            self._find_archive_by_key, int(channel_id), archive_key
        )

    def _find_archive_by_key(
        self, channel_id: int, archive_key: str
    ) -> StoredFile | None:
        document = self._require_collection().find_one(
            {"channel_id": channel_id, "archive_key": archive_key}
        )
        return self._document_to_stored(document)

    async def find_archives_by_urls(
        self, *, channel_id: int, urls: list[str]
    ) -> list[StoredFile]:
        return await asyncio.to_thread(
            self._find_archives_by_urls, int(channel_id), tuple(urls)
        )

    def _find_archives_by_urls(
        self, channel_id: int, urls: tuple[str, ...]
    ) -> list[StoredFile]:
        if not urls:
            return []
        documents = self._require_collection().find(
            {"channel_id": channel_id, "url": {"$in": list(urls)}}
        )
        by_url = {
            str(document.get("url") or ""): self._document_to_stored(document)
            for document in documents
        }
        return [by_url[url] for url in urls if by_url.get(url) is not None]  # type: ignore[misc]

    async def get(self, token: str) -> StoredFile | None:
        return await asyncio.to_thread(self._get, token)

    def _get(self, token: str) -> StoredFile | None:
        return self._document_to_stored(
            self._require_collection().find_one({"token": token})
        )

    async def search(
        self, query: str, *, owner_id: int | None = None, limit: int = 10
    ) -> list[StoredFile]:
        return await asyncio.to_thread(
            self._search, query.strip(), owner_id, max(1, min(50, limit))
        )

    def _search(
        self, query: str, owner_id: int | None, limit: int
    ) -> list[StoredFile]:
        pattern = re.escape(query)
        condition: dict[str, Any] = {
            "$or": [
                {"title": {"$regex": pattern, "$options": "i"}},
                {"name": {"$regex": pattern, "$options": "i"}},
                {"url": {"$regex": pattern, "$options": "i"}},
            ]
        }
        if owner_id is not None:
            condition["owner_id"] = int(owner_id)
        return [
            self._document_to_stored(document)
            for document in self._require_collection()
            .find(condition)
            .sort("created_at", DESCENDING)
            .limit(limit)
        ]  # type: ignore[misc]

    async def recent(
        self, *, owner_id: int | None = None, limit: int = 10
    ) -> list[StoredFile]:
        return await asyncio.to_thread(
            self._recent, owner_id, max(1, min(50, limit))
        )

    def _recent(self, owner_id: int | None, limit: int) -> list[StoredFile]:
        query = {"owner_id": int(owner_id)} if owner_id is not None else {}
        return [
            self._document_to_stored(document)
            for document in self._require_collection()
            .find(query)
            .sort("created_at", DESCENDING)
            .limit(limit)
        ]  # type: ignore[misc]

    async def stats(self, owner_id: int | None = None) -> StoreStats:
        return await asyncio.to_thread(self._stats, owner_id)

    def _stats(self, owner_id: int | None) -> StoreStats:
        query = {"owner_id": int(owner_id)} if owner_id is not None else {}
        collection = self._require_collection()
        result = next(
            iter(
                collection.aggregate(
                    [
                        {"$match": query},
                        {
                            "$group": {
                                "_id": None,
                                "count": {"$sum": 1},
                                "total_size": {"$sum": {"$ifNull": ["$size", 0]}},
                            }
                        },
                    ]
                )
            ),
            None,
        )
        if not result:
            return StoreStats(count=0, total_size=0)
        return StoreStats(
            count=int(result.get("count") or 0),
            total_size=int(result.get("total_size") or 0),
        )

    async def delete(self, token: str) -> StoredFile | None:
        return await asyncio.to_thread(self._delete, token)

    def _delete(self, token: str) -> StoredFile | None:
        document = self._require_collection().find_one_and_delete({"token": token})
        return self._document_to_stored(document)