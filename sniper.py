import asyncio
import time
import logging
import os
from datetime import datetime
from pyrogram import Client, raw
from pyrogram.errors import (
    FloodWait, UsernameOccupied,
    UsernameInvalid, UsernameNotModified, PeerIdInvalid
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%H:%M:%S"
)
log = logging.getLogger("sniper")

# ─── CONFIG ───────────────────────────────────────────────────────────────────
API_ID         = int(os.environ.get("API_ID", 0))
API_HASH       = os.environ.get("API_HASH", "")
SESSION_STRING = os.environ.get("SESSION_STRING", "")

# Map each target username → your channel ID
TARGET_CHANNEL_MAP = {
    "tamil_b":        -1004469497392,
    # "tamilgroup":   -100XXXXXXXXXX,  ← add more here
}

CHECK_INTERVAL = 0.1    # normal polling
FAST_INTERVAL  = 0.02   # when username detected free
NOTIFY_SELF    = True
STAGGER_DELAY  = 0.1
# ─────────────────────────────────────────────────────────────────────────────

_claim_lock = asyncio.Lock()
_done       = set()

# {channel_id: chat.id} — cached at startup
_channel_cache: dict[int, int] = {}


async def is_available(client: Client, username: str) -> bool:
    try:
        await client.invoke(
            raw.functions.contacts.ResolveUsername(username=username)
        )
        return False
    except FloodWait:
        raise
    except Exception as e:
        msg = str(e).lower()
        if any(k in msg for k in (
            "username_not_occupied",
            "not occupied",
            "username invalid",
            "not found",
            "no match",
        )):
            return True
        return False


async def cache_channels(client: Client):
    log.info("📌 Caching channels ...")
    for cid in set(TARGET_CHANNEL_MAP.values()):
        try:
            chat = await client.get_chat(cid)
            _channel_cache[cid] = chat.id
            log.info(f"   ✅ {chat.title} ({cid})")
        except Exception as e:
            log.error(f"   ❌ Failed to cache {cid}: {e}")


async def watch(client: Client, username: str, index: int):
    username = username.lstrip("@").lower()

    channel_id = TARGET_CHANNEL_MAP.get(username)
    if not channel_id or channel_id not in _channel_cache:
        log.error(f"❌ No valid channel for @{username} — skipping!")
        return

    cached_id = _channel_cache[channel_id]

    if index > 0:
        await asyncio.sleep(index * STAGGER_DELAY)

    log.info(f"👁  Watching @{username} ...")
    check_no = 0
    t_start  = time.time()

    while True:
        if username in _done:
            log.info(f"  @{username} done. Exiting.")
            return

        try:
            check_no += 1
            available = await is_available(client, username)

            if check_no % 500 == 0:
                elapsed = time.time() - t_start
                log.info(f"  @{username} — {check_no} checks ({check_no/elapsed:.1f}/s)")

            if available:
                log.info(f"🟢 @{username} FREE! Attempting claim ...")

                async with _claim_lock:
                    if username in _done:
                        return

                    for attempt in range(5):
                        t = time.time()
                        try:
                            await client.set_chat_username(cached_id, username)
                            ms = (time.time() - t) * 1000
                            log.info(f"✅ @{username} CLAIMED in {ms:.0f}ms!")
                            _done.add(username)

                            if NOTIFY_SELF:
                                try:
                                    await client.send_message(
                                        "me",
                                        f"🎯 **Claimed @{username}!**\n"
                                        f"🕐 `{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}`\n"
                                        f"📢 Channel: `{channel_id}`"
                                    )
                                except Exception:
                                    pass
                            return

                        except UsernameOccupied:
                            ms = (time.time() - t) * 1000
                            log.warning(f"  Attempt {attempt+1}: USERNAME_OCCUPIED ({ms:.0f}ms) retrying in 1s...")
                            await asyncio.sleep(1)

                        except FloodWait as e:
                            log.warning(f"  FloodWait {e.value}s on claim ...")
                            await asyncio.sleep(e.value)

                        except (UsernameInvalid, UsernameNotModified) as e:
                            log.warning(f"  ❌ @{username}: {e}")
                            break

                        except Exception as e:
                            if "CHANNELS_ADMIN_PUBLIC_TOO_MUCH" in str(e):
                                log.error("❌ Too many public channels on this account!")
                            else:
                                log.warning(f"  ❌ Unexpected: {e}")
                            break

                if username not in _done:
                    log.warning(f"  @{username} — all attempts failed, back to watching ...")
                    await asyncio.sleep(CHECK_INTERVAL)

            else:
                await asyncio.sleep(CHECK_INTERVAL)

        except FloodWait as e:
            log.warning(f"⏳ FloodWait {e.value}s @{username}")
            await asyncio.sleep(e.value)

        except Exception as e:
            log.error(f"  Error @{username}: {e}")
            await asyncio.sleep(2)


async def main():
    client = Client(
        name="sniper",
        api_id=API_ID,
        api_hash=API_HASH,
        session_string=SESSION_STRING
    ) if SESSION_STRING else Client(
        name="sniper_session",
        api_id=API_ID,
        api_hash=API_HASH
    )

    async with client:
        me = await client.get_me()
        log.info(f"🤖 {me.first_name} (@{me.username or 'no username'})")
        log.info(f"⚡ Targets: {list(TARGET_CHANNEL_MAP.keys())}")

        await cache_channels(client)

        await asyncio.gather(*[
            watch(client, username, i)
            for i, username in enumerate(TARGET_CHANNEL_MAP.keys())
        ])

        log.info("✅ All done.")


if __name__ == "__main__":
    asyncio.run(main())
