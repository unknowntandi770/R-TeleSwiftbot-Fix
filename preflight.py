"""Validate deployment environment without importing third-party packages."""

from __future__ import annotations

import os
import sys


def _value(name: str) -> str:
    return os.getenv(name, "").strip()


def _check_port(name: str, errors: list[str]) -> None:
    raw = _value(name)
    if not raw:
        return
    try:
        port = int(raw)
    except ValueError:
        errors.append(f"{name} must be a whole-number TCP port (got: {raw!r}).")
        return
    if not 1 <= port <= 65535:
        errors.append(f"{name} must be between 1 and 65535 (got: {port}).")


def main() -> int:
    errors: list[str] = []

    # Required Telegram credentials — support legacy aliases
    alias_map = {"API_ID": "APP_ID", "API_HASH": "APP_HASH", "BOT_TOKEN": "TOKEN"}
    missing = [
        name
        for name in ("API_ID", "API_HASH", "BOT_TOKEN")
        if not _value(name) and not _value(alias_map[name])
    ]
    if missing:
        errors.append("Missing required Telegram variable(s): " + ", ".join(missing))

    # Cookie encryption — warn rather than block startup
    if not (_value("SESSION_SECRET") or _value("COOKIE_ENCRYPTION_KEY")):
        print(
            "INFO: No COOKIE_ENCRYPTION_KEY or SESSION_SECRET set. "
            "The bot will auto-create a private key at WORK_DIR/.cookie-encryption-key.",
            file=sys.stderr,
        )

    # Validate API_ID is a positive integer
    api_id = _value("API_ID") or _value("APP_ID")
    if api_id:
        try:
            if int(api_id) <= 0:
                errors.append("API_ID must be a positive integer.")
        except ValueError:
            errors.append("API_ID must be a numeric Telegram application ID.")

    # Validate all port env vars
    for name in ("PORT", "HEALTH_PORT", "FILE_URL_PORT"):
        _check_port(name, errors)

    if errors:
        print("Startup preflight failed:", file=sys.stderr)
        for error in errors:
            print(f"  • {error}", file=sys.stderr)
        print(
            "\nFix: add the missing values as private environment variables in\n"
            "your hosting dashboard (Railway, Render, Fly.io, Docker .env, etc.),\n"
            "then restart the container.",
            file=sys.stderr,
        )
        return 1

    # Compute effective ports — on one-port PaaS hosts PORT drives everything
    platform_port = _value("PORT") or "8080"
    health = _value("HEALTH_PORT") or platform_port
    file_url = _value("FILE_URL_PORT") or health

    print(
        f"Startup preflight passed "
        f"(health={health}, file-link={file_url}, platform-port={platform_port}).",
        flush=True,
    )

    if health != file_url:
        print(
            "WARNING: HEALTH_PORT and FILE_URL_PORT differ — the host must expose\n"
            "both ports. For single-port PaaS hosting leave both unset so they\n"
            "share PORT automatically.",
            file=sys.stderr,
            flush=True,
        )

    # Check for optional but recommended tooling
    import shutil
    recommendations: list[str] = []
    if not shutil.which("ffmpeg"):
        recommendations.append("ffmpeg is not installed — audio extraction and thumbnail generation will fail.")
    if not shutil.which("aria2c"):
        recommendations.append("aria2c is not installed — /leech torrent downloads will not work.")
    if not shutil.which("bun") and not shutil.which("deno"):
        recommendations.append(
            "Neither bun nor deno found — YouTube may require a JavaScript runtime for some extractions."
        )
    for rec in recommendations:
        print(f"WARNING: {rec}", file=sys.stderr)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
