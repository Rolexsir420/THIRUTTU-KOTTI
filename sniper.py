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

TARGET_CHANNEL_MAP = {
    "tamil_b": -1004390966952,
}

CHECK_INTERVAL = 0.2    # normal polling
FAST_INTERVAL  = 0.05   # when free detected
NOTIFY_SELF    = True
STAGGER_DELAY  = 0.15
# ─────────────────────────────────────────────────────────────────────────────

_claim_lock  = asyncio.Lock()
_done        = set()
_channel_cache: dict[int, int] = {}
_last_claim_attempt: dict[str, float] = {}  # Track last claim attempt per username


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
            "username_not_occupied", "not occupied", "username invalid",
            "not found", "no match",
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


async def try_claim(client: Client, username: str) -> bool:
    channel_id = TARGET_CHANNEL_MAP.get(username)

    if not channel_id or channel_id not in _channel_cache:
        log.error(f"❌ No valid channel for @{username}")
        return False

    cached_id = _channel_cache[channel_id]
    t = time.time()

    try:
        log.info(f"⚡ Claiming @{username} → channel {channel_id} ...")
        await client.set_chat_username(cached_id, username)
        ms = (time.time() - t) * 1000
        log.info(f"✅ @{username} CLAIMED in {ms:.0f}ms!")

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
        return True

    except UsernameOccupied:
        ms = (time.time() - t) * 1000
        log.warning(f"⏳ @{username} unavailable ({ms:.0f}ms) — cooldown active")
        _last_claim_attempt[username] = time.time()
        return False

    except FloodWait as e:
        log.warning(f"⚠️  FloodWait {e.value}s — account rate limited")
        await asyncio.sleep(e.value)
        return False

    except (UsernameInvalid, UsernameNotModified) as e:
        log.warning(f"❌ @{username}: {e}")
        return False

    except Exception as e:
        log.warning(f"❌ @{username} error: {e}")
        return False


async def watch(client: Client, username: str, index: int):
    username = username.lstrip("@").lower()
    _last_claim_attempt[username] = 0

    if index > 0:
        await asyncio.sleep(index * STAGGER_DELAY)

    # ── STARTUP CHECK ────────────────────────────────────────────────────────
    log.info(f"🔍 @{username} — startup check ...")
    try:
        if await is_available(client, username):
            log.info(f"🟢 @{username} FREE on startup!")
            async with _claim_lock:
                success = await try_claim(client, username)
            _done.add(username)
            log.info(f"  @{username} {'secured 🏆' if success else 'cooling down'}.")
            return
    except FloodWait as e:
        log.warning(f"FloodWait {e.value}s on startup — sleeping ...")
        await asyncio.sleep(e.value)
    except Exception as e:
        log.warning(f"Startup error @{username}: {e}")

    # ── HOT LOOP ─────────────────────────────────────────────────────────────
    log.info(f"👁  @{username} — watching ...")
    check_no = 0
    t_start  = time.time()
    interval = CHECK_INTERVAL
    errors   = 0

    while True:
        if username in _done:
            log.info(f"  @{username} done.")
            return

        if _claim_lock.locked():
            await asyncio.sleep(0.05)
            continue

        try:
            check_no += 1
            available = await is_available(client, username)
            errors    = 0

            if check_no % 500 == 0:
                elapsed = time.time() - t_start
                log.info(f"  @{username} — {check_no} checks ({check_no/elapsed:.1f}/s)")

            if available:
                # ── SMART BACKOFF: Only attempt if enough time passed ──
                last_attempt = _last_claim_attempt.get(username, 0)
                time_since_attempt = time.time() - last_attempt
                
                if time_since_attempt < 30:
                    # Last attempt was <30s ago, skip this one
                    interval = FAST_INTERVAL
                    await asyncio.sleep(interval)
                    continue

                log.info(f"🟢 @{username} FREE! Firing claim ...")
                async with _claim_lock:
                    if username in _done:
                        return
                    success = await try_claim(client, username)

                if success:
                    _done.add(username)
                    log.info(f"  @{username} secured 🏆")
                    return

                interval = CHECK_INTERVAL

            else:
                interval = CHECK_INTERVAL

            await asyncio.sleep(interval)

        except FloodWait as e:
            log.warning(f"⏳ FloodWait {e.value}s — pausing all watchers")
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
        log.info("🔑 String session")
    else:
        client = Client(
            name="sniper_session",
            api_id=API_ID,
            api_hash=API_HASH
        )
        log.info("🔑 Local session")

    async with client:
        me = await client.get_me()
        log.info(f"🤖 {me.first_name} (@{me.username or 'no username'})")
        log.info(f"⚡ Targets: {list(TARGET_CHANNEL_MAP.keys())}")
        log.info(f"   Strategy: 30s backoff, single account, no spam")

        await cache_channels(client)

        await asyncio.gather(*[
            watch(client, username, i)
            for i, username in enumerate(TARGET_CHANNEL_MAP.keys())
        ])

        log.info("✅ Done.")


if __name__ == "__main__":
    asyncio.run(main())
