"""Generate a Pyrogram user session string for voice-chat playback.

Run this one-off command from the host shell:

    python generate_vc_session.py

The phone number, login code, and optional 2FA password stay in the Shell.
Store the printed session string only as the private VC_SESSION_STRING secret.
"""

from __future__ import annotations

import asyncio
import logging

from pyrogram import Client

from bot_config import Settings


async def main() -> None:
    settings = Settings.from_env()
    client = Client(
        "vc-session-generator",
        api_id=settings.api_id,
        api_hash=settings.api_hash,
        workdir=str(settings.work_dir),
        in_memory=True,
        hide_password=True,
    )
    try:
        await client.start()
        me = await client.get_me()
        if getattr(me, "is_bot", False):
            raise RuntimeError(
                "This login is a bot account. Use a real Telegram user account."
            )
        session_string = await client.export_session_string()
        print("\nVC_SESSION_STRING (store this as a private host secret):")
        print(session_string)
        print(f"\nAuthorized user id: {me.id}")
    finally:
        if client.is_connected:
            await client.stop()


if __name__ == "__main__":
    logging.basicConfig(level=logging.WARNING)
    asyncio.run(main())