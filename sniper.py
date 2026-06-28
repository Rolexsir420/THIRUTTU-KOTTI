"""
Telegram Username Sniper Userbot ⚡
- Single account, maximum efficiency
- No create_channel fallback — pool channels only
- Single clean claim attempt (burst = wasted FloodWait on one account)
- Channel cached at startup, never re-fetched during claim
- Skip re-check inside lock — claim immediately on free detection
- Adaptive polling: 0.2s normal → 0.05s when free detected
- Pre-resolve peers on startup
- Response time logging for tuning
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

# ─── CONFIG ───────────────────────────────────────────────────────────────────
API_ID         = int(os.environ.get("API_ID", 0))
API_HASH       = os.environ.get("API_HASH", "")
SESSION_STRING = os.environ.get("SESSION_STRING", "")

TARGETS = [
    "tamilfriendship",
    "tamil_friends",
    "tamilgroup",
    "tamilvip",
    "tamilboys",
    "tamilgirls",
    "tamilonly",
    "tamilzone",
    "tamilclub",
    "tamil_b",
]

# One channel per target (in order). Add enough for all targets.
CHANNEL_POOL = [
    -1004469497392,   # Channel 1
    # -100xxxxxxxxxx, # Channel 2 — add more here!
]

CHECK_INTERVAL = 0.2    # normal polling (seconds)
FAST_INTERVAL  = 0.05   # adaptive once username goes free
NOTIFY_SELF    = True
STAGGER_DELAY  = 0.15   # startup stagger between watchers
# ─────────────────────────────────────────────────────────────────────────────

_claim_lock  = asyncio.Lock()
_done        = set()
_pool_index  = 0
_pool_lock   = asyncio.Lock()

# Channel cache: {channel_id: peer} — populated at startup
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
            "username invalid",
            "no match",
            "not found",
            "not occupied",
        )):
            return True
        return False


async def get_next_channel() -> int | None:
    global _pool_index
    async with _pool_lock:
        if _pool_index < len(CHANNEL_POOL):
            cid = CHANNEL_POOL[_pool_index]
            _pool_index += 1
            return cid
        return None


async def try_claim(client: Client, username: str) -> bool:
    channel_id = await get_next_channel()

    if not channel_id:
        log.error("❌ No channels left in pool! Add more to CHANNEL_POOL.")
        return False

    if channel_id not in _channel_cache:
        log.error(f"❌ Channel {channel_id} not cached at startup — skipping claim!")
        return False

    cached_id = _channel_cache[channel_id]
    t = time.time()

    try:
        log.info(f"⚡ Claiming @{username} → channel {channel_id} ...")
        await client.set_chat_username(cached_id, username)
        ms = (time.time() - t) * 1000
        log.info(f"✅ @{username} CLAIMED in {ms:.0f}ms → channel {channel_id}")

        if NOTIFY_SELF:
            try:
                await client.send_message(
                    "me",
                    f"🎯 **Snagged @{username}!**\n"
                    f"🕐 `{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}`\n"
                    f"📢 Channel ID: `{channel_id}`"
                )
            except Exception:
                pass
        return True

    except UsernameOccupied:
        ms = (time.time() - t) * 1000
        log.warning(f"❌ @{username} race lost ({ms:.0f}ms) — taken by someone else")
        return False

    except (UsernameInvalid, UsernameNotModified) as e:
        log.warning(f"❌ @{username} claim error: {e}")
        return False

    except FloodWait as e:
        log.warning(f"⏳ UpdateUsername FloodWait {e.value}s for @{username}")
        await asyncio.sleep(e.value)
        return False

    except Exception as e:
        if "CHANNELS_ADMIN_PUBLIC_TOO_MUCH" in str(e):
            log.error("❌ Too many public channels! Add more to CHANNEL_POOL.")
        else:
            log.warning(f"❌ @{username} unexpected error: {e}")
        return False


async def cache_channels(client: Client):
    """Cache all pool channels at startup. Abort if any fails."""
    log.info("📌 Caching pool channels ...")
    for cid in CHANNEL_POOL:
        try:
            chat = await client.get_chat(cid)
            _channel_cache[cid] = chat.id
            log.info(f"   ✅ Cached: {chat.title} ({cid})")
        except Exception as e:
            log.error(f"   ❌ FAILED to cache channel {cid}: {e}")
            log.error("   Fix CHANNEL_POOL and redeploy!")


async def pre_resolve(client: Client):
    log.info("🔥 Pre-resolving targets ...")
    for t in TARGETS:
        try:
            await client.resolve_peer(t)
            log.info(f"   @{t} — cached ✅")
        except PeerIdInvalid:
            log.info(f"   @{t} — free on startup!")
        except FloodWait as e:
            log.warning(f"   FloodWait {e.value}s, waiting ...")
            await asyncio.sleep(e.value)
        except Exception as e:
            log.warning(f"   @{t} pre-resolve: {e}")
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

            if check_no % 200 == 0:
                elapsed = time.time() - t_start
                log.info(f"  @{username} — {check_no} checks ({check_no/elapsed:.1f}/s)")

            if available:
                interval = FAST_INTERVAL
                log.info(f"🟢 @{username} FREE! Firing claim ...")
                async with _claim_lock:
                    if username in _done:
                        return
                    success = await try_claim(client, username)

                if success:
                    _done.add(username)
                    log.info(f"  @{username} secured 🏆. Others continue.")
                    return
                else:
                    # Failed — back to normal polling
                    interval = CHECK_INTERVAL

            else:
                interval = CHECK_INTERVAL

            await asyncio.sleep(interval)

        except FloodWait as e:
            log.warning(f"⏳ FloodWait {e.value}s @{username} — waiting ...")
            await asyncio.sleep(e.value)
            interval = CHECK_INTERVAL

        except Exception as e:
            errors += 1
            log.error(f"  Error #{errors} @{username}: {e}")
            await asyncio.sleep(min(2 ** errors, 10))


async def main():
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
        log.info(f"🤖 Logged in as {me.first_name} (@{me.username or 'no username'})")
        log.info(f"⚡ Targets: {len(TARGETS)} | Pool: {len(CHANNEL_POOL)} channels")
        log.info(f"   Interval: {CHECK_INTERVAL}s → {FAST_INTERVAL}s adaptive")

        await cache_channels(client)
        await pre_resolve(client)

        await asyncio.gather(*[
            watch(client, t, i) for i, t in enumerate(TARGETS)
        ])

        log.info("✅ All targets processed.")


if __name__ == "__main__":
    asyncio.run(main())
