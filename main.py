"""
Auto Link Forwarder
-------------------
Control bot (buttons only) + tumhara userbot account (QR login).

Flow:
  Tum bot ko post bhejte ho (photo/video + caption with link)
  -> userbot (tumhara account) link ko target bot (@diskwala_576_bot) ko bhejta hai
  -> target bot ka video aate hi wait khatam
  -> pehle original post, phir naya video -> Backup Channel me forward
Koi download nahi hota, sirf forward.
"""
import asyncio
import html
import io
import json
import logging
import os
import re

import qrcode
from aiohttp import web
from telethon import Button, TelegramClient, events
from telethon.errors import (
    MessageNotModifiedError,
    PasswordHashInvalidError,
    SessionPasswordNeededError,
)
from telethon.sessions import StringSession
from telethon.tl.types import (
    Channel,
    InputPeerChannel,
    MessageEntityTextUrl,
    MessageEntityUrl,
    PeerChannel,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("autobot")

# ───────────────────────── Config ─────────────────────────
API_ID = int(os.environ["API_ID"])
API_HASH = os.environ["API_HASH"]
BOT_TOKEN = os.environ["BOT_TOKEN"]
OWNER_ID = int(os.environ["OWNER_ID"])
MONGO_URI = os.getenv("MONGO_URI")  # recommended on Render (free disk nahi hoti)
PORT = int(os.getenv("PORT", "10000"))
DATA_FILE = "data.json"

DEFAULTS = {"target": "diskwala_576_bot", "backup": None, "timeout": 180, "session": ""}
URL_RE = re.compile(r"https?://[^\s<>\"']+")
# Sirf ye pattern copy hoga. Badalna ho to LINK_PATTERN env var set karo.
LINK_RE = re.compile(os.getenv("LINK_PATTERN", r"https?://(?:www\.)?diskwala\.com/app/[A-Za-z0-9]+"))
# Target bot ye message bheje ("Sorry! Download fail ho gaya...") to link skip. SKIP_PATTERN se badlo.
FAIL_RE = re.compile(os.getenv("SKIP_PATTERN", r"download\s*fail"), re.I)
FAIL, CANCEL = "FAIL", "CANCEL"

bot: TelegramClient = None
user: TelegramClient = None
queue: asyncio.Queue = None
login_task = None
state = {
    "mode": None,        # None | set_target | set_backup | password
    "pw_future": None,
    "me": None,
    "busy": False,
    "dialogs": False,
    "target_id": None,
    "inbox": None,
    "jobs": {},
    "counter": 0,
    "current": None,
}


# ───────────────────────── Storage ─────────────────────────
class Store:
    def __init__(self):
        self.data = dict(DEFAULTS)
        self.col = None

    async def load(self):
        saved = {}
        if MONGO_URI:
            from motor.motor_asyncio import AsyncIOMotorClient

            self.col = AsyncIOMotorClient(MONGO_URI)["autobot"]["state"]
            saved = await self.col.find_one({"_id": "main"}) or {}
            saved.pop("_id", None)
        elif os.path.exists(DATA_FILE):
            with open(DATA_FILE) as f:
                saved = json.load(f)
        self.data = {**DEFAULTS, **saved}

    async def set(self, **kw):
        self.data.update(kw)
        if self.col is not None:
            await self.col.replace_one({"_id": "main"}, {"_id": "main", **self.data}, upsert=True)
        else:
            with open(DATA_FILE, "w") as f:
                json.dump(self.data, f)


store = Store()


# ───────────────────────── Texts ─────────────────────────
GUIDE = """📖 <b>Kaise use karein</b>

<b>Step 1 — Login</b>
🔐 <i>QR Login</i> dabao → QR scan karo
(Telegram → Settings → Devices → Link Desktop Device)

<b>Step 2 — Backup Channel</b>
📦 <i>Backup Channel</i> dabao → channel ka koi post forward karo
⚠️ Bot ko channel me <b>Admin</b> banao (post permission) aur apna account bhi member rakho.

<b>Step 3 — Post bhejo</b>
Photo/Video + caption (jisme link ho) seedha is chat me bhejo.

<b>Ab bot khud karega:</b>
1️⃣ Link tumhare account se target bot ko jayega
2️⃣ Target bot ka video aane tak wait
3️⃣ Pehle tumhara post, phir naya video → Backup me forward

🎯 Target bot aur ⏱ timeout menu se badal sakte ho."""

QR_TEXT = """🔐 <b>Login — QR Scan karo</b>

1️⃣ Telegram kholo
2️⃣ <b>Settings → Devices → Link Desktop Device</b>
3️⃣ Is QR ko scan karo

⏳ QR har 30 sec me naya ban jayega."""

PW_TEXT = """🔑 <b>2-Step Verification</b>

Apna cloud password is chat me bhejo.
🧹 Message turant delete ho jayega."""


def home_btn():
    return [Button.inline("🏠 Menu", b"menu")]


# ───────────────────────── Helpers ─────────────────────────
async def safe_delete(m):
    try:
        if m:
            await m.delete()
    except Exception:
        pass


async def is_logged():
    try:
        return user.is_connected() and await user.is_user_authorized()
    except Exception:
        return False


def make_qr(url):
    img = qrcode.make(url)
    bio = io.BytesIO()
    img.save(bio, "PNG")
    bio.seek(0)
    bio.name = "qr.png"
    return bio


def extract_links(msg):
    links = []
    try:
        for ent, inner in msg.get_entities_text():
            if isinstance(ent, MessageEntityTextUrl):
                links.append(ent.url)
            elif isinstance(ent, MessageEntityUrl):
                links.append(inner)
    except Exception:
        pass
    links += URL_RE.findall(msg.raw_text or "")
    links = [l.rstrip(".,;)]}>") for l in links]
    # sirf diskwala.com/app/<id> wale links, baaki sab ignore
    links = [l for l in links if LINK_RE.match(l)]
    return list(dict.fromkeys(links))


async def bot_peer():
    b = store.data["backup"]
    if b.get("username"):
        return await bot.get_input_entity(b["username"])
    return InputPeerChannel(b["id"], b["hash"])


async def user_peer():
    b = store.data["backup"]
    if b.get("username"):
        return await user.get_input_entity(b["username"])
    if not state["dialogs"]:
        await user.get_dialogs()
        state["dialogs"] = True
    return await user.get_input_entity(PeerChannel(b["id"]))


# ───────────────────────── Menu ─────────────────────────
async def menu_view():
    logged = await is_logged()
    d = store.data
    acc = f"✅ {html.escape(state['me'] or 'Connected')}" if logged else "❌ Login nahi hai"
    bk = d["backup"]
    bk_txt = html.escape(bk["title"]) if bk else "❌ Set nahi hai"
    pending = queue.qsize() + (1 if state["busy"] else 0)
    text = (
        "🤖 <b>Auto Link Forwarder</b>\n\n"
        f"👤 <b>Account:</b> {acc}\n"
        f"🎯 <b>Target Bot:</b> @{html.escape(d['target'])}\n"
        f"📦 <b>Backup:</b> {bk_txt}\n"
        f"⏱ <b>Timeout:</b> {d['timeout']}s\n"
        f"📥 <b>Queue:</b> {pending}\n\n"
        "➡️ Post (photo/video + link caption) bhejo, baaki main sambhal lunga."
    )
    first = (
        Button.inline("🚪 Logout", b"logout")
        if logged
        else Button.inline("🔐 QR Login", b"login")
    )
    btns = [
        [first],
        [Button.inline("🎯 Target Bot", b"target"), Button.inline("📦 Backup Channel", b"backup")],
        [Button.inline("⏱ Timeout", b"timeout"), Button.inline("📖 Guide", b"guide")],
        [Button.inline("🔄 Refresh", b"menu")],
    ]
    return text, btns


async def render(event=None):
    state["mode"] = None
    text, btns = await menu_view()
    if event is not None:
        try:
            await event.edit(text, buttons=btns, parse_mode="html")
            return
        except MessageNotModifiedError:
            return
        except Exception:
            pass
    await bot.send_message(OWNER_ID, text, buttons=btns, parse_mode="html")


# ───────────────────────── Login ─────────────────────────
async def ask_password():
    state["mode"] = "password"
    state["pw_future"] = asyncio.get_running_loop().create_future()
    m = await bot.send_message(
        OWNER_ID, PW_TEXT, buttons=[[Button.inline("❌ Cancel", b"login_cancel")]], parse_mode="html"
    )
    try:
        return await state["pw_future"]
    finally:
        state["mode"] = None
        await safe_delete(m)


async def do_login():
    qr_msg = None
    try:
        if not user.is_connected():
            await user.connect()
        qr = await user.qr_login()
        while True:
            qr_msg = await bot.send_file(
                OWNER_ID,
                make_qr(qr.url),
                caption=QR_TEXT,
                buttons=[[Button.inline("❌ Cancel", b"login_cancel")]],
                parse_mode="html",
            )
            try:
                await qr.wait()
                break
            except asyncio.TimeoutError:
                await safe_delete(qr_msg)
                qr_msg = None
                await qr.recreate()
            except SessionPasswordNeededError:
                await safe_delete(qr_msg)
                qr_msg = None
                for _ in range(3):
                    pw = await ask_password()
                    try:
                        await user.sign_in(password=pw)
                        break
                    except PasswordHashInvalidError:
                        await bot.send_message(OWNER_ID, "❌ Password galat hai, dobara bhejo.")
                else:
                    raise RuntimeError("3 baar galat password")
                break
        await safe_delete(qr_msg)
        me = await user.get_me()
        state["me"] = me.first_name or str(me.id)
        await store.set(session=user.session.save())
        await bot.send_message(
            OWNER_ID, f"✅ <b>Login ho gaya!</b>\nWelcome, {html.escape(state['me'])}", parse_mode="html"
        )
        await user.get_dialogs()
        state["dialogs"] = True
        await render()
    except asyncio.CancelledError:
        await safe_delete(qr_msg)
        await render()
    except Exception as e:
        log.exception("login failed")
        await safe_delete(qr_msg)
        await bot.send_message(
            OWNER_ID,
            f"⚠️ <b>Login fail hua</b>\n<code>{html.escape(str(e))}</code>",
            buttons=[[Button.inline("🔐 Dobara Try", b"login")], home_btn()],
            parse_mode="html",
        )


# ───────────────────────── Userbot ─────────────────────────
async def on_user_message(ev):
    """Target bot ka video/file inbox me daalo."""
    tid = state["target_id"]
    if tid is None or ev.sender_id != tid:
        return
    m = ev.message
    if m.video or (m.document and not m.sticker):
        state["inbox"].put_nowait(m)
    elif FAIL_RE.search(m.raw_text or ""):
        state["inbox"].put_nowait(FAIL)


def make_user(session=""):
    c = TelegramClient(StringSession(session), API_ID, API_HASH)
    c.add_event_handler(on_user_message, events.NewMessage(incoming=True))
    return c


# ───────────────────────── Worker ─────────────────────────
async def run_job(msg, st, job):
    def cancel_btn():
        return [[Button.inline("❌ Cancel", f"cancel_{job['id']}".encode())]]

    async def status(text, buttons=None):
        try:
            await st.edit(text, buttons=buttons, parse_mode="html")
        except Exception:
            pass

    async def cancelled():
        await status("🛑 <b>Cancel ho gaya.</b>\nKuch forward nahi hua.", [home_btn()])

    links = extract_links(msg)
    target = store.data["target"]
    timeout = store.data["timeout"]
    ent = await user.get_entity(target)
    state["target_id"] = ent.id

    videos, skipped = [], []
    for i, link in enumerate(links, 1):
        if job["cancelled"]:
            return await cancelled()
        await status(
            f"⏳ <b>Link {i}/{len(links)}</b> target bot ko bheja\nVideo ka wait ho raha hai…",
            cancel_btn(),
        )
        while not state["inbox"].empty():
            state["inbox"].get_nowait()
        await user.send_message(ent, link)
        try:
            res = await asyncio.wait_for(state["inbox"].get(), timeout)
        except asyncio.TimeoutError:
            skipped.append("timeout")
            continue
        if res == CANCEL or job["cancelled"]:
            return await cancelled()
        if res == FAIL:
            skipped.append("download fail")
            continue
        videos.append(res)

    if job["cancelled"]:
        return await cancelled()

    if not videos:
        await status(
            "⚠️ <b>Koi video nahi mila</b>\n"
            f"⏭ {len(skipped)} link skip hue ({', '.join(sorted(set(skipped)))}).",
            [home_btn()],
        )
        return

    await status("📤 Backup channel me forward ho raha hai…")
    await bot.forward_messages(await bot_peer(), msg)
    peer = await user_peer()
    for v in videos:
        await user.forward_messages(peer, v)
        await asyncio.sleep(1)

    text = f"✅ <b>Backup complete!</b>\n📝 1 post + 🎬 {len(videos)} video forward ho gaye."
    if skipped:
        text += f"\n\n⏭ {len(skipped)} link skip hue ({', '.join(sorted(set(skipped)))})."
    await status(text, [home_btn()])


async def worker():
    while True:
        msg, st, job = await queue.get()
        if job["cancelled"]:
            state["jobs"].pop(job["id"], None)
            queue.task_done()
            continue
        state["busy"] = True
        state["current"] = job["id"]
        try:
            await run_job(msg, st, job)
        except Exception as e:
            log.exception("job failed")
            try:
                await st.edit(
                    f"❌ <b>Error</b>\n<code>{html.escape(str(e))}</code>\n\n"
                    "Check karo: bot channel me admin hai? Account login hai?",
                    buttons=[home_btn()],
                    parse_mode="html",
                )
            except Exception:
                pass
        finally:
            state["busy"] = False
            state["current"] = None
            state["jobs"].pop(job["id"], None)
            queue.task_done()


# ───────────────────────── Bot handlers ─────────────────────────
async def handle_post(event):
    msg = event.message
    if not await is_logged():
        await event.reply(
            "🔐 Pehle account login karo.",
            buttons=[[Button.inline("🔐 QR Login", b"login")]],
        )
        return
    if not store.data["backup"]:
        await event.reply(
            "📦 Pehle Backup Channel set karo.",
            buttons=[[Button.inline("📦 Backup Channel", b"backup")]],
        )
        return
    if not extract_links(msg):
        await event.reply(
            "🔗 Is post me <b>diskwala.com/app/…</b> wala link nahi mila.",
            buttons=[home_btn()],
            parse_mode="html",
        )
        return
    state["counter"] += 1
    job = {"id": state["counter"], "cancelled": False}
    state["jobs"][job["id"]] = job
    st = await event.reply(
        f"📥 Queue me add ho gaya (#{queue.qsize() + (1 if state['busy'] else 0) + 1})",
        buttons=[[Button.inline("❌ Cancel", f"cancel_{job['id']}".encode())], home_btn()],
    )
    await queue.put((msg, st, job))


async def on_message(event):
    if event.sender_id != OWNER_ID:
        return
    text = (event.raw_text or "").strip()
    mode = state["mode"]

    if text.startswith("/"):
        await render()
        return

    if mode == "password":
        fut = state["pw_future"]
        if fut and not fut.done():
            fut.set_result(text)
        await safe_delete(event.message)
        return

    if mode == "set_target":
        name = text.replace("https://t.me/", "").replace("@", "").split("?")[0].strip("/")
        if not re.fullmatch(r"[A-Za-z0-9_]{4,32}", name):
            await event.reply("❌ Valid bot username bhejo (jaise @diskwala_576_bot).")
            return
        await store.set(target=name)
        state["target_id"] = None
        await event.reply(f"✅ Target bot set: @{html.escape(name)}", parse_mode="html")
        await render()
        return

    if mode == "set_backup":
        try:
            fwd = event.message.forward
            chat = fwd.chat if fwd and fwd.chat else None
            if chat is None and text:
                chat = await bot.get_entity(text.replace("https://t.me/", "").lstrip("@"))
            if not isinstance(chat, Channel):
                raise ValueError("Ye channel/supergroup nahi lag raha.")
            await store.set(
                backup={
                    "id": chat.id,
                    "hash": chat.access_hash,
                    "username": chat.username,
                    "title": chat.title,
                }
            )
            state["dialogs"] = False
            await event.reply(f"✅ Backup set: <b>{html.escape(chat.title)}</b>", parse_mode="html")
            await render()
        except Exception as e:
            await event.reply(
                f"❌ Channel nahi mila.\n<code>{html.escape(str(e))}</code>\n\n"
                "Channel ka koi post forward karo ya @username bhejo.",
                parse_mode="html",
            )
        return

    await handle_post(event)


async def on_callback(event):
    global login_task, user
    if event.sender_id != OWNER_ID:
        await event.answer("Ye bot private hai 🔒", alert=True)
        return
    d = event.data.decode()
    cancel = [Button.inline("❌ Cancel", b"menu")]

    if d == "menu":
        await event.answer()
        await render(event)

    elif d.startswith("cancel_"):
        job = state["jobs"].get(int(d[7:]))
        if not job:
            await event.answer("Ye job already khatam ho chuka hai", alert=True)
            return
        job["cancelled"] = True
        if state["current"] == job["id"]:
            state["inbox"].put_nowait(CANCEL)
        await event.answer("Cancel kar diya 🛑")
        try:
            await event.edit(
                "🛑 <b>Cancel ho gaya.</b>\nKuch forward nahi hua.",
                buttons=[home_btn()],
                parse_mode="html",
            )
        except Exception:
            pass

    elif d == "guide":
        await event.edit(GUIDE, buttons=[home_btn()], parse_mode="html")

    elif d == "login":
        if await is_logged():
            await event.answer("Pehle se login hai ✅", alert=True)
            return
        if login_task and not login_task.done():
            await event.answer("Login already chal raha hai", alert=True)
            return
        await event.answer("QR ban raha hai…")
        await safe_delete(await event.get_message())
        login_task = asyncio.create_task(do_login())

    elif d == "login_cancel":
        await event.answer("Cancel ho gaya")
        if login_task and not login_task.done():
            login_task.cancel()
        else:
            await safe_delete(await event.get_message())

    elif d == "logout":
        await event.edit(
            "🚪 <b>Logout karna hai?</b>\nSession delete ho jayega, dobara QR scan karna padega.",
            buttons=[[Button.inline("✅ Haan, Logout", b"logout_yes"), Button.inline("❌ Nahi", b"menu")]],
            parse_mode="html",
        )

    elif d == "logout_yes":
        try:
            await user.log_out()
        except Exception:
            pass
        await store.set(session="")
        state.update(me=None, dialogs=False, target_id=None)
        user = make_user("")
        await event.answer("Logout ho gaya")
        await render(event)

    elif d == "target":
        state["mode"] = "set_target"
        await event.edit(
            "🎯 <b>Target Bot</b>\n\nJis bot ko link bhejna hai uska <b>@username</b> bhejo.\n"
            f"Abhi: @{html.escape(store.data['target'])}",
            buttons=[[Button.inline("↩️ Default (@diskwala_576_bot)", b"target_default")], cancel],
            parse_mode="html",
        )

    elif d == "target_default":
        await store.set(target=DEFAULTS["target"])
        state["target_id"] = None
        await event.answer("Default set ✅")
        await render(event)

    elif d == "backup":
        state["mode"] = "set_backup"
        await event.edit(
            "📦 <b>Backup Channel</b>\n\n"
            "Channel ka <b>koi bhi post yahan forward karo</b>, ya channel ka <b>@username</b> bhejo.\n\n"
            "⚠️ Bot ko channel me Admin banao (post permission) aur apna account bhi member rakho.",
            buttons=[cancel],
            parse_mode="html",
        )

    elif d == "timeout":
        cur = store.data["timeout"]
        row = [Button.inline(("✅ " if cur == s else "") + f"{s}s", f"tmo_{s}".encode()) for s in (60, 120, 180, 300, 600)]
        await event.edit(
            "⏱ <b>Video Timeout</b>\n\nTarget bot ke video ka kitni der wait karna hai?",
            buttons=[row[:3], row[3:], home_btn()],
            parse_mode="html",
        )

    elif d.startswith("tmo_"):
        await store.set(timeout=int(d[4:]))
        await event.answer("Timeout set ✅")
        await render(event)


# ───────────────────────── Web (Render health) ─────────────────────────
async def start_web():
    app = web.Application()
    app.router.add_get("/", lambda r: web.Response(text="ok"))
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", PORT).start()


# ───────────────────────── Main ─────────────────────────
async def main():
    global bot, user, queue
    queue = asyncio.Queue()
    state["inbox"] = asyncio.Queue()
    await store.load()
    await start_web()

    bot = TelegramClient(StringSession(), API_ID, API_HASH)
    await bot.start(bot_token=BOT_TOKEN)
    bot.add_event_handler(on_message, events.NewMessage(incoming=True, func=lambda e: e.is_private))
    bot.add_event_handler(on_callback, events.CallbackQuery)

    user = make_user(store.data.get("session", ""))
    await user.connect()
    if await user.is_user_authorized():
        me = await user.get_me()
        state["me"] = me.first_name or str(me.id)
        asyncio.create_task(user.get_dialogs())
        state["dialogs"] = True

    asyncio.create_task(worker())
    try:
        await render()
    except Exception:
        log.warning("Owner ne bot ko /start nahi kiya abhi tak")
    log.info("Bot online")
    await bot.run_until_disconnected()


if __name__ == "__main__":
    asyncio.run(main())
