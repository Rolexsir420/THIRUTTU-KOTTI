"""
Telegram Username Sniper Userbot ⚡
- Single account, maximum efficiency
- resolve_peer for fastest availability check
- Adaptive polling: 0.2s normal → 0.1s when free detected
- Burst x5 + retry on failure
- Pre-resolve peers on startup (warms cache)
- Response time logging for tuning
- Channel pool: assigns each claimed username to next available channel
- Skips done targets, continues watching rest
- String session support via environment variables
"""

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

# ─── CONFIG (all from Railway environment variables) ───────────────────────────
API_ID         = int(os.environ.get("API_ID", 0))
API_HASH       = os.environ.get("API_HASH", "")
SESSION_STRING = os.environ.get("SESSION_STRING", "")

TARGETS = [
    "tamilfriendship",
    "tamilgroup",
    "tamil_b",
]

# Pool of your private channel IDs (in order of priority)
# First claimed username → channel 1, second → channel 2, etc.
# Get ID by forwarding a message from each channel to @userinfobot
CHANNEL_POOL = [
    -1004469497392,   # Target 1
]

CHANNEL_TITLE  = "Tamil Chat"  # used only if CHANNEL_POOL is empty
CHECK_INTERVAL = 0.2           # normal polling interval (seconds)
FAST_INTERVAL  = 0.1           # adaptive interval once username goes free
CLAIM_BURST    = 5             # simultaneous claim attempts
NOTIFY_SELF    = True
STAGGER_DELAY  = 0.2           # startup stagger between watchers
# ───────────────────────────────────────────────────────────────────────────────

_claim_lock = asyncio.Lock()
_done       = set()
_pool_index = 0
_pool_lock  = asyncio.Lock()


async def is_available(client: Client, username: str) -> bool:
    t = time.time()
    try:
        await client.resolve_peer(username)
        ms = (time.time() - t) * 1000
        log.debug(f"@{username} check took {ms:.0f}ms → taken")
        return False
    except PeerIdInvalid:
        ms = (time.time() - t) * 1000
        log.debug(f"@{username} check took {ms:.0f}ms → FREE")
        return True
    except FloodWait:
        raise
    except Exception as e:
        msg = str(e).lower()
        if any(k in msg for k in (
            "username_not_occupied",
            "no match",
            "not found",
            "username invalid",
        )):
            return True
        return False


async def get_next_channel() -> int | None:
    global _pool_index
    async with _pool_lock:
        if _pool_index < len(CHANNEL_POOL):
            channel_id = CHANNEL_POOL[_pool_index]
            _pool_index += 1
            return channel_id
        return None


async def try_claim(client: Client, username: str) -> bool:
    log.info(f"⚡ BURST CLAIM x{CLAIM_BURST} for @{username}!")
    channel_id = await get_next_channel()

    # Cache channel peer once before burst to avoid repeated GetFullChannel calls
    cached_chat_id = None
    if channel_id:
        try:
            chat = await client.get_chat(channel_id)
            cached_chat_id = chat.id
        except Exception as e:
            log.warning(f"Could not cache channel {channel_id}: {e}")

    async def single_attempt(n: int):
        try:
            if cached_chat_id:
                await client.set_chat_username(cached_chat_id, username)
                log.info(f"✅ @{username} → channel {channel_id} (attempt #{n})")
                return channel_id
            else:
                channel = await client.create_channel(
                    title=CHANNEL_TITLE,
                    description=f"Claimed @{username}"
                )
                await asyncio.sleep(0.1)
                await client.set_chat_username(channel.id, username)
                log.info(f"✅ @{username} → new channel {channel.id} (attempt #{n})")
                return channel.id
        except UsernameOccupied:
            log.warning(f"  #{n}: race lost — taken by someone else")
            return None
        except (UsernameInvalid, UsernameNotModified) as e:
            log.warning(f"  #{n}: {e}")
            return None
        except Exception as e:
            if "CHANNELS_ADMIN_PUBLIC_TOO_MUCH" in str(e):
                log.error("❌ Too many public channels! Add more to CHANNEL_POOL.")
            else:
                log.warning(f"  #{n} error: {e}")
            return None

    # First burst
    results = await asyncio.gather(*[single_attempt(i + 1) for i in range(CLAIM_BURST)])
    claimed_id = next((r for r in results if r), None)

    # Retry once if all burst attempts failed (network hiccup)
    if not claimed_id:
        log.warning(f"  Burst failed — retrying once after 100ms ...")
        await asyncio.sleep(0.1)
        claimed_id = await single_attempt(CLAIM_BURST + 1)

    if claimed_id and NOTIFY_SELF:
        try:
            await client.send_message(
                "me",
                f"🎯 **Snagged @{username}!**\n"
                f"🕐 `{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}`\n"
                f"📢 Channel ID: `{claimed_id}`"
            )
        except Exception:
            pass

    return claimed_id is not None


async def pre_resolve(client: Client):
    """Warm up Pyrogram's peer cache for all targets before watching starts."""
    log.info("🔥 Pre-resolving all targets to warm peer cache ...")
    for t in TARGETS:
        try:
            await client.resolve_peer(t)
            log.info(f"   @{t} — cached ✅")
        except PeerIdInvalid:
            log.info(f"   @{t} — already free on startup!")
        except FloodWait as e:
            log.warning(f"   FloodWait {e.value}s during pre-resolve, waiting ...")
            await asyncio.sleep(e.value)
        except Exception as e:
            log.warning(f"   @{t} pre-resolve error: {e}")
        await asyncio.sleep(0.1)
    log.info("✅ Pre-resolve done. Starting watchers ...")


async def watch(client: Client, username: str, stagger_index: int):
    username = username.lstrip("@").lower()

    if stagger_index > 0:
        await asyncio.sleep(stagger_index * STAGGER_DELAY)

    # ── STARTUP CHECK ────────────────────────────────────────────────────────
    log.info(f"🔍 @{username} — startup check ...")
    try:
        if await is_available(client, username):
            log.info(f"🟢 @{username} FREE on startup!")
            async with _claim_lock:
                success = await try_claim(client, username)
            _done.add(username)
            log.info(f"  @{username} {'secured 🏆' if success else 'lost — skipping'}.")
            return
    except FloodWait as e:
        log.warning(f"FloodWait {e.value}s on startup @{username}")
        await asyncio.sleep(e.value)
    except Exception as e:
        log.warning(f"Startup error @{username}: {e}")

    # ── HOT LOOP ─────────────────────────────────────────────────────────────
    log.info(f"👁  @{username} — watching (normal: {CHECK_INTERVAL}s, fast: {FAST_INTERVAL}s) ...")
    check_no = 0
    t_start  = time.time()
    errors   = 0
    interval = CHECK_INTERVAL

    while True:
        if username in _done:
            log.info(f"  @{username} done. Watcher exits.")
            return

        if _claim_lock.locked():
            await asyncio.sleep(0.05)
            continue

        try:
            check_no += 1
            available = await is_available(client, username)
            errors    = 0

            if check_no % 100 == 0:
                elapsed = time.time() - t_start
                log.info(f"  @{username} — {check_no} checks ({check_no/elapsed:.1f}/s) — taken")

            if available:
                interval = FAST_INTERVAL
                log.info(f"🟢 @{username} FREE! Firing claim ...")
                async with _claim_lock:
                    if username in _done:
                        return
                    still_free = await is_available(client, username)
                    if still_free:
                        success = await try_claim(client, username)
                    else:
                        log.warning(f"  @{username} taken before lock. Resuming.")
                        success = False
                        interval = CHECK_INTERVAL

                if success or not still_free:
                    _done.add(username)
                    log.info(f"  @{username} {'secured 🏆' if success else 'lost — skipping'}. Others continue.")
                    return
                interval = CHECK_INTERVAL
            else:
                interval = CHECK_INTERVAL

            await asyncio.sleep(interval)

        except FloodWait as e:
            log.warning(f"⏳ FloodWait {e.value}s @{username}")
            await asyncio.sleep(e.value)
            interval = CHECK_INTERVAL

        except Exception as e:
            errors += 1
            log.error(f"  Error #{errors} @{username}: {e}")
            await asyncio.sleep(min(2 ** errors, 10))


async def main():
    # Use string session if available, otherwise fall back to session file
    if SESSION_STRING:
        client = Client(
            name="sniper",
            api_id=API_ID,
            api_hash=API_HASH,
            session_string=SESSION_STRING
        )
        log.info("🔑 Using string session from environment")
    else:
        client = Client(
            name="sniper_session",
            api_id=API_ID,
            api_hash=API_HASH
        )
        log.info("🔑 Using local session file")

    async with client:
        me = await client.get_me()

        # Cache all pool channels by joining/getting them
        log.info("📌 Caching pool channels ...")
        for cid in CHANNEL_POOL:
            try:
                chat = await client.get_chat(cid)
                log.info(f"   Channel cached: {chat.title} ({cid})")
            except Exception as e:
                log.warning(f"   Could not cache channel {cid}: {e}")
        log.info(f"🤖 Logged in as {me.first_name} (@{me.username or 'no username'})")
        log.info(f"⚡ Targets: {len(TARGETS)} | Pool: {len(CHANNEL_POOL)} channels")
        log.info(f"   Interval: {CHECK_INTERVAL}s → {FAST_INTERVAL}s (adaptive) | Burst: {CLAIM_BURST}x + retry")

        await pre_resolve(client)

        await asyncio.gather(*[
            watch(client, t, i) for i, t in enumerate(TARGETS)
        ])

        log.info("✅ All targets processed.")


if __name__ == "__main__":
    asyncio.run(main())
