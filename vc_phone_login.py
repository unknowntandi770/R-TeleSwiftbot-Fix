"""Authorize the Telegram voice-chat assistant with one device.

Run this once from the host shell:

    python vc_phone_login.py

Pyrogram asks for the user's phone number, Telegram login code, and (if
enabled) the 2FA password in this terminal. The resulting local session file
is reused by the bot; no session string is printed or required.
"""

from __future__ import annotations

import asyncio
import logging

from pyrogram import Client

from bot_config import Settings


async def main() -> None:
    settings = Settings.from_env()
    settings.vc_session_path.parent.mkdir(parents=True, exist_ok=True)
    client = Client(
        settings.vc_session_path.name,
        api_id=settings.api_id,
        api_hash=settings.api_hash,
        workdir=str(settings.vc_session_path.parent),
        hide_password=True,
    )
    try:
        await client.start()
        me = await client.get_me()
        if getattr(me, "is_bot", False):
            raise RuntimeError(
                "This login is a bot account. Use a real Telegram user account."
            )
        print(
            f"Voice assistant login complete for user id={me.id}. "
            "Restart the bot process now."
        )
    finally:
        if client.is_connected:
            await client.stop()
        settings.vc_session_path.parent.chmod(0o700)
        session_file = settings.vc_session_path.with_suffix(".session")
        if session_file.exists():
            session_file.chmod(0o600)


if __name__ == "__main__":
    logging.basicConfig(level=logging.WARNING)
    asyncio.run(main())