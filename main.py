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
import glob
import html
import io
import json
import logging
import os
import random
import re
import secrets
import time

import aiohttp
import qrcode
from aiohttp import web
from telethon import Button, TelegramClient, events, utils
from telethon.errors import (
    MessageNotModifiedError,
    PasswordHashInvalidError,
    SessionPasswordNeededError,
)
from telethon.sessions import StringSession
from telethon.tl import functions
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
POSTER_BOT_TOKEN = os.getenv("POSTER_BOT_TOKEN")  # bulk ke liye (poster bot ka token)
BACKUP_FILE = os.getenv("BACKUP_FILE")  # default: repo ki autobackup-*.json
DATA_FILE = "data.json"

DEFAULTS = {"target": "diskwala_576_bot", "backup": None, "dests": None, "timeout": 180, "session": ""}
URL_RE = re.compile(r"https?://[^\s<>\"']+")
# Sirf ye pattern copy hoga. Badalna ho to LINK_PATTERN env var set karo.
LINK_RE = re.compile(os.getenv("LINK_PATTERN", r"https?://(?:www\.)?diskwala\.com/app/[A-Za-z0-9]+"))
# Target bot ye message bheje ("Sorry! Download fail ho gaya...") to link skip. SKIP_PATTERN se badlo.
FAIL_RE = re.compile(os.getenv("SKIP_PATTERN", r"download\s*fail"), re.I)
FAIL, CANCEL, DEAD = "FAIL", "CANCEL", "DEAD"
DEAD_RE = re.compile(os.getenv("DEAD_PATTERN", r"link\s*kaam\s*nahi|galat\s*ya\s*remove"), re.I)
RETRY_HOURS = float(os.getenv("RETRY_HOURS", "3"))  # fail hue posts ka auto-retry (0 = sirf manual /bulkretry)
# Termux se pehle hi ho chuke 5 posts (reset ke baad dobara na ho)
SEED_DONE = ["6a6aeba797e52936a7479545", "6a6aeba897e52936a7479546", "6a6aebcc97e52936a7479562",
             "6a6aebd997e52936a747956c", "6a6aebe697e52936a7479576"]

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
        if self.data.get("dests") is None:  # purana single "backup" channel -> destinations list
            self.data["dests"] = []
            b = saved.get("backup")
            if b:
                self.data["dests"].append({
                    "id": "c1", "kind": "channel", "title": b.get("title") or "Backup",
                    "pid": utils.get_peer_id(PeerChannel(b["id"])), "username": b.get("username"),
                    "hash": b.get("hash"), "on": True,
                })

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

<b>Step 2 — Destinations</b>
📦 <i>Destinations</i> dabao → ek ya <b>zyada</b> jagah add karo:
• 💾 <b>Saved Messages</b> (ek tap)
• 📢 Channel/Group — post forward karo ya @username/link bhejo
• 💬 Personal chat — naam, @username ya ID bhejo
Har destination ko tap karke on/off kar sakte ho.
⚠️ Channel me bot ko <b>Admin</b> banao (nahi to account se forward hoga, account member ho).

<b>Step 3 — Post bhejo</b>
Photo/Video + caption (jisme link ho) seedha is chat me bhejo.

<b>Ab bot khud karega:</b>
1️⃣ Link tumhare account se target bot ko jayega
2️⃣ Target bot ka video aane tak wait
3️⃣ Pehle tumhara post, phir naya video → <b>sab active destinations</b> me forward

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


def active_dests():
    return [d for d in (store.data.get("dests") or []) if d.get("on", True)]


async def bot_peer(d):
    if d.get("username"):
        return await bot.get_input_entity(d["username"])
    return InputPeerChannel(utils.resolve_id(d["pid"])[0], d["hash"])


async def user_peer(d):
    if d["kind"] == "saved":
        return await user.get_input_entity("me")
    if d.get("username"):
        return await user.get_input_entity(d["username"])
    if not state["dialogs"]:
        await user.get_dialogs()
        state["dialogs"] = True
    return await user.get_input_entity(d["pid"])


async def make_dest(ent, bot_hash=None):
    """Telethon entity -> destination dict. Channel + bot ko access ho to bot se bhi post jayega."""
    pid = utils.get_peer_id(ent)
    h = None
    if isinstance(ent, Channel):
        h = bot_hash
        if h is None and ent.username:
            try:
                h = (await bot.get_input_entity(ent.username)).access_hash
            except Exception:
                h = None
    return {
        "id": secrets.token_hex(3),
        "kind": "channel" if h is not None else "chat",
        "title": utils.get_display_name(ent) or str(pid),
        "pid": pid,
        "username": getattr(ent, "username", None),
        "hash": h,
        "on": True,
    }


async def add_dest(d):
    """True = add hua, False = pehle se hai."""
    ds = list(store.data.get("dests") or [])
    if any(x["pid"] == d["pid"] for x in ds):
        return False
    ds.append(d)
    await store.set(dests=ds)
    state["dialogs"] = False
    return True


async def find_user_copy(msg):
    """Control bot chat me tumhare account ki apni copy dhundo (private chat me msg id alag hote hain)."""
    if not state.get("ctrl"):
        state["ctrl"] = (await bot.get_me()).username
    want = (msg.raw_text or "").strip()
    for _ in range(3):
        async for m in user.iter_messages(state["ctrl"], limit=20):
            if (m.out and (m.raw_text or "").strip() == want
                    and bool(m.video) == bool(msg.video) and bool(m.photo) == bool(msg.photo)):
                return m
        await asyncio.sleep(1.5)
    return None


async def find_candidates(q):
    """@username / link / numeric ID / chat ka naam -> entities (max 8)."""
    name = re.sub(r"^https?://t\.me/", "", q.strip()).lstrip("@").split("?")[0].strip("/")
    out = []
    if not state["dialogs"]:
        await user.get_dialogs()
        state["dialogs"] = True
    try:
        if re.fullmatch(r"-?\d{5,}", name):
            out.append(await user.get_entity(int(name)))
        elif re.fullmatch(r"[A-Za-z0-9_]{4,32}", name):
            out.append(await user.get_entity(name))
    except Exception:
        pass
    if not out:
        ql = q.strip().lower()
        async for dlg in user.iter_dialogs(limit=400):
            if ql and ql in (dlg.name or "").lower():
                out.append(dlg.entity)
                if len(out) >= 8:
                    break
    return out


# ───────────────────────── Menu ─────────────────────────
async def menu_view():
    logged = await is_logged()
    d = store.data
    acc = f"✅ {html.escape(state['me'] or 'Connected')}" if logged else "❌ Login nahi hai"
    act = active_dests()
    if act:
        names = ", ".join(html.escape(x["title"]) for x in act[:3]) + (f" +{len(act) - 3}" if len(act) > 3 else "")
        bk_txt = f"{len(act)} active — {names}"
    else:
        bk_txt = "❌ Set nahi hai"
    pending = queue.qsize() + (1 if state["busy"] else 0)
    text = (
        "🤖 <b>Auto Link Forwarder</b>\n\n"
        f"👤 <b>Account:</b> {acc}\n"
        f"🎯 <b>Target Bot:</b> @{html.escape(d['target'])}\n"
        f"📦 <b>Destinations:</b> {bk_txt}\n"
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
        [Button.inline("🎯 Target Bot", b"target"), Button.inline("📦 Destinations", b"dests")],
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
    """Target bot ka video/file inbox me daalo (reply_to id ke saath, taaki purane link ka result na mile)."""
    tid = state["target_id"]
    if tid is None or ev.sender_id != tid:
        return
    m = ev.message
    rt = m.reply_to_msg_id
    txt = m.raw_text or ""
    if m.video or (m.document and not m.sticker):
        state["inbox"].put_nowait((rt, m))
    elif DEAD_RE.search(txt):
        state["inbox"].put_nowait((rt, DEAD))
    elif FAIL_RE.search(txt):
        state["inbox"].put_nowait((rt, FAIL))


def make_user(session=""):
    c = TelegramClient(StringSession(session), API_ID, API_HASH)
    c.add_event_handler(on_user_message, events.NewMessage(incoming=True))
    return c


# ───────────────────────── Worker ─────────────────────────
async def fwd(client, msg, to_peer, drop_caps=False):
    """Server-side forward (bandwidth 0): 'Forwarded from' tag nahi, optional caption hata ke."""
    await client(functions.messages.ForwardMessagesRequest(
        from_peer=await msg.get_input_chat(),
        id=[msg.id],
        random_id=[random.randrange(-2**63, 2**63)],
        to_peer=to_peer,
        drop_author=True,
        drop_media_captions=drop_caps,
    ))


async def wait_result(sent_id, timeout):
    """Isi link ka result lo; purane (timeout hue) links ke late replies ignore."""
    loop = asyncio.get_event_loop()
    end = loop.time() + timeout
    while True:
        left = end - loop.time()
        if left <= 0:
            raise asyncio.TimeoutError
        item = await asyncio.wait_for(state["inbox"].get(), left)
        if item == CANCEL:
            return CANCEL
        rt, payload = item
        if rt is not None and rt != sent_id:
            continue  # stale
        return payload


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
        sent = await user.send_message(ent, link)
        try:
            res = await wait_result(sent.id, timeout)
        except asyncio.TimeoutError:
            skipped.append("timeout")
            continue
        if res == CANCEL or job["cancelled"]:
            return await cancelled()
        if res == FAIL:
            skipped.append("download fail")
            continue
        if res == DEAD:
            skipped.append("link dead")
            continue
        videos.append(res)

    if job["cancelled"]:
        return await cancelled()

    job["why"] = ", ".join(sorted(set(skipped)))
    if not videos:
        job["result"] = "failed"
        await status(
            "⚠️ <b>Koi video nahi mila</b>\n"
            f"⏭ {len(skipped)} link skip hue ({', '.join(sorted(set(skipped)))}).",
            [home_btn()],
        )
        return

    dests = active_dests()
    if not dests:
        job["result"] = "failed"
        job["why"] = "destination nahi"
        await status(
            "⚠️ <b>Koi active destination nahi hai</b>\nVideo mil gaya par kahin forward nahi hua.",
            [[Button.inline("📦 Destinations", b"dests")], home_btn()],
        )
        return

    ok, bad, post_missing, user_copy = [], [], 0, None
    for n, d in enumerate(dests, 1):
        await status(f"📤 <b>Forward {n}/{len(dests)}</b>\n{html.escape(d['title'])}…")
        try:
            posted = False
            if d["kind"] == "channel":  # bot se: caption rahega, tag nahi
                try:
                    await fwd(bot, msg, await bot_peer(d))
                    posted = True
                except Exception as e:
                    log.warning("bot forward fail (%s): %s", d["title"], e)
            peer = await user_peer(d)
            if not posted:  # saved / personal chat / bot-admin nahi: account se forward
                if user_copy is None:
                    user_copy = await find_user_copy(msg) or False
                if user_copy:
                    await fwd(user, user_copy, peer)
                    posted = True
            if not posted:
                post_missing += 1
            for v in videos:
                await fwd(user, v, peer, drop_caps=True)  # video: caption + tag dono nahi
                await asyncio.sleep(1)
            ok.append(d["title"])
        except Exception as e:
            log.exception("forward failed: %s", d["title"])
            bad.append((d["title"], str(e)[:60]))
        await asyncio.sleep(1)

    if not ok:
        job["result"] = "failed"
        job["why"] = "forward fail"
        reasons = "\n".join(f"• {html.escape(t)}: <code>{html.escape(r)}</code>" for t, r in bad)
        await status(f"❌ <b>Error</b> — kisi destination me forward nahi hua\n\n{reasons}", [home_btn()])
        return

    job["result"] = "done"
    text = f"✅ <b>Backup complete!</b>\n📝 1 post + 🎬 {len(videos)} video → {len(ok)}/{len(dests)} destination"
    text += "\n\n" + "\n".join(f"✅ {html.escape(t)}" for t in ok)
    for t, r in bad:
        text += f"\n❌ {html.escape(t)} — <code>{html.escape(r)}</code>"
    if post_missing:
        text += f"\n\n⚠️ {post_missing} jagah post nahi gaya (account me post ki copy nahi mili), sirf video gaye."
    if skipped:
        text += f"\n\n⏭ {len(skipped)} link skip hue ({', '.join(sorted(set(skipped)))})."
    await status(text, [home_btn()])


async def worker():
    while True:
        msg, st, job = await queue.get()
        if job["cancelled"]:
            job["result"] = "cancelled"
            state["jobs"].pop(job["id"], None)
            queue.task_done()
            continue
        state["busy"] = True
        state["current"] = job["id"]
        try:
            await run_job(msg, st, job)
        except Exception as e:
            job["result"] = "error"
            job["why"] = str(e)[:80]
            log.exception("job failed")
            try:
                await st.edit(
                    f"❌ <b>Error</b>\n<code>{html.escape(str(e))}</code>\n\n"
                    "Check karo: destination sahi hai? Bot channel me admin hai? Account login hai?",
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


# ───────────────────────── Bulk (backup JSON -> queue) ─────────────────────────
bulk_task = None


async def tell(text):
    try:
        await bot.send_message(OWNER_ID, text, parse_mode="html")
    except Exception:
        pass


def bulk_get():
    b = dict(store.data.get("bulk") or {})
    b.setdefault("done", [])
    b.setdefault("failed", {})  # post_id -> reason
    return b


def load_posts(chat_id):
    path = BACKUP_FILE or (sorted(glob.glob("autobackup-*.json")) or [None])[-1]
    if not path or not os.path.exists(path):
        return None
    with open(path, encoding="utf-8") as f:
        posts = json.load(f)["posts"].get(str(chat_id), [])
    return [p for p in posts if p.get("active", True) and "diskwala.com/app/" in (p.get("caption") or "")]


def bulk_summary(b, total):
    why = {}
    for r in b["failed"].values():
        for w in (r or "?").split(", "):
            why[w] = why.get(w, 0) + 1
    w = ", ".join(f"{k}: {v}" for k, v in sorted(why.items())) or "-"
    return f"✅ done {len(b['done'])}/{total} · ❌ failed {len(b['failed'])} ({w})"


def parse_bulk_args(text):
    """'/bulk 10' -> (None, 10) · '/bulk -1004471150773 10' -> ('-1004471150773', 10)"""
    cid, cnt = None, None
    for t in text.split()[1:]:
        if t.lstrip("-").isdigit():
            if t.startswith("-") or len(t) > 6:
                cid = t
            else:
                cnt = int(t) or None
    return cid, cnt


async def bulk_run(chat_id, mode="main"):
    """mode=main: jo na done na failed. mode=retry: sirf failed wale."""
    try:
        if not POSTER_BOT_TOKEN:
            return await tell("❌ Render env <b>POSTER_BOT_TOKEN</b> set nahi hai.")
        if not await is_logged() or not active_dests():
            return await tell("❌ Pehle QR Login aur kam se kam ek Destination set karo.")
        posts = load_posts(chat_id)
        if posts is None:
            return await tell("❌ Backup JSON file repo me nahi mili.")
        b = bulk_get()
        done = set(b["done"])
        failed = dict(b["failed"])
        if mode == "retry":
            todo = [p for p in posts if p["_id"] in failed and p["_id"] not in done]
        else:
            todo = [p for p in posts if p["_id"] not in done and p["_id"] not in failed]

        stop_at = b.get("stop_at")  # itne done hote hi ruk jao (None = sab)

        async def save(running=True, retry_at=None):
            await store.set(bulk={"chat_id": str(chat_id), "running": running, "mode": mode,
                                  "done": sorted(done), "failed": failed, "retry_at": retry_at,
                                  "stop_at": stop_at})

        await save()
        lim = f" · limit: {stop_at - len(done)} download" if stop_at else ""
        await tell(f"🚀 <b>Bulk {mode}</b>: {len(todo)} posts{lim} · {bulk_summary(bulk_get(), len(posts))}")

        async with aiohttp.ClientSession() as http:
            async with http.get(f"https://api.telegram.org/bot{POSTER_BOT_TOKEN}/getMe") as r:
                poster = (await r.json())["result"]["username"]
            poster_ent = await user.get_entity(poster)
            control = await user.get_entity((await bot.get_me()).username)
            methods = {"video": ("sendVideo", "video"), "photo": ("sendPhoto", "photo"),
                       "animation": ("sendAnimation", "animation"), "document": ("sendDocument", "document")}

            for n, p in enumerate(todo, 1):
                if not (store.data.get("bulk") or {}).get("running"):
                    await save(running=False)
                    return await tell(f"⏹ Bulk ruk gaya. {bulk_summary(bulk_get(), len(posts))}")
                if stop_at and len(done) >= stop_at:
                    break
                pid = p["_id"]
                result, why = "failed", "send fail"
                m = methods.get(p.get("media_type"))
                if m and p.get("file_id"):
                    payload = {"chat_id": OWNER_ID, m[1]: p["file_id"], "caption": p.get("caption") or "",
                               "parse_mode": p.get("parse_mode") or "HTML"}
                    async with http.post(f"https://api.telegram.org/bot{POSTER_BOT_TOKEN}/{m[0]}", json=payload) as r:
                        data = await r.json()
                    if data.get("ok"):
                        await asyncio.sleep(1.5)
                        msgs = await user.get_messages(poster_ent, limit=1)
                        c0 = state["counter"]
                        await user.forward_messages(control, msgs[0])
                        job = None
                        for _ in range(30):  # job queue me aane tak
                            if state["counter"] > c0:
                                job = state["jobs"].get(state["counter"]) or {"id": state["counter"]}
                                break
                            await asyncio.sleep(1)
                        if job is None:
                            why = "queue me nahi aaya"
                        else:
                            while job["id"] in state["jobs"]:  # job khatam hone tak
                                if not (store.data.get("bulk") or {}).get("running"):
                                    break
                                await asyncio.sleep(3)
                            result = job.get("result", "failed")
                            why = job.get("why") or result
                    else:
                        why = (data.get("description") or "send fail")[:60]
                        log.warning("poster send fail: %s", why)
                else:
                    why = "media/file_id nahi"

                if result == "done":
                    done.add(pid)
                    failed.pop(pid, None)
                else:
                    failed[pid] = why
                await save()
                if n % 50 == 0:
                    await tell(f"📊 {bulk_summary(bulk_get(), len(posts))}")
                await asyncio.sleep(2)

        retry_at = None
        if failed and RETRY_HOURS > 0 and not stop_at:
            retry_at = time.time() + RETRY_HOURS * 3600
        await save(running=False, retry_at=retry_at)
        extra = ""
        if failed:
            extra = (f"\n⏰ Failed posts <b>{RETRY_HOURS:g} ghante</b> baad auto-retry honge, "
                     "ya abhi /bulkretry bhejo.") if RETRY_HOURS > 0 else "\nRetry ke liye /bulkretry bhejo."
        if stop_at:
            stop_at = None
            await save(running=False, retry_at=retry_at)
        await tell(f"🏁 <b>Bulk {mode} complete</b>\n{bulk_summary(bulk_get(), len(posts))}{extra}")
    except Exception as e:
        log.exception("bulk failed")
        await tell(f"❌ Bulk error:\n<code>{html.escape(str(e))}</code>\n/bulk dobara bhejo, resume ho jayega.")


async def bulk_start(chat_id, mode="main"):
    global bulk_task
    if bulk_task and not bulk_task.done():
        return False
    bulk_task = asyncio.create_task(bulk_run(chat_id, mode))
    return True


async def retry_watcher():
    """Failed posts ka auto-retry (RETRY_HOURS baad)."""
    while True:
        await asyncio.sleep(60)
        try:
            b = bulk_get()
            idle = not (bulk_task and not bulk_task.done())
            if idle and b["failed"] and b.get("retry_at") and time.time() >= b["retry_at"] and b.get("chat_id"):
                await store.set(bulk={**b, "retry_at": None})
                await bulk_start(b["chat_id"], "retry")
        except Exception:
            log.exception("retry_watcher")


# ───────────────────────── Bot handlers ─────────────────────────
async def handle_post(event):
    msg = event.message
    if not await is_logged():
        await event.reply(
            "🔐 Pehle account login karo.",
            buttons=[[Button.inline("🔐 QR Login", b"login")]],
        )
        return
    if not active_dests():
        await event.reply(
            "📦 Pehle Destination add karo (kam se kam ek active).",
            buttons=[[Button.inline("📦 Destinations", b"dests")]],
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


async def show_dests(event=None):
    state["mode"] = None
    text, btns = dests_view()
    if event is not None:
        try:
            await event.edit(text, buttons=btns, parse_mode="html")
            return
        except MessageNotModifiedError:
            return
        except Exception:
            pass
    await bot.send_message(OWNER_ID, text, buttons=btns, parse_mode="html")


def dests_view():
    ds = store.data.get("dests") or []
    icon = {"saved": "💾", "channel": "📢", "chat": "💬"}
    lines = [f"{'✅' if d['on'] else '⏸'} {icon.get(d['kind'], '📦')} {html.escape(d['title'])}" for d in ds]
    text = (
        "📦 <b>Destinations</b>\n\n"
        "Post + videos in sab jagah forward honge (✅ wale):\n\n"
        + ("\n".join(lines) if lines else "<i>Abhi koi destination nahi hai.</i>")
        + "\n\n👆 Tap = on/off · 🗑 = hatao"
    )
    btns = []
    for d in ds:
        btns.append([
            Button.inline(f"{'✅' if d['on'] else '⏸'} {d['title'][:26]}", f"dt_{d['id']}".encode()),
            Button.inline("🗑", f"dd_{d['id']}".encode()),
        ])
    btns.append([Button.inline("➕ Channel / Chat", b"dadd"), Button.inline("💾 Saved Messages", b"dsaved")])
    btns.append(home_btn())
    return text, btns


async def handle_add_dest(event, text):
    back = [[Button.inline("❌ Cancel", b"dests")]]
    f = event.message.forward
    chat = f.chat if f and f.chat else None
    if isinstance(chat, Channel):  # channel ka post forward kiya (bot ko access milta hai)
        d = await make_dest(chat, bot_hash=chat.access_hash)
        fresh = await add_dest(d)
        await event.reply(f"✅ Add hua: <b>{html.escape(d['title'])}</b>" if fresh else "ℹ️ Ye pehle se list me hai.", parse_mode="html")
        return await show_dests()
    if not text:
        await event.reply("Channel ka post forward karo, ya @username / link / ID / chat ka naam bhejo.", buttons=back)
        return
    if not await is_logged():
        await event.reply("🔐 Personal chat/group ke liye pehle QR Login karo.", buttons=[[Button.inline("🔐 QR Login", b"login")], home_btn()])
        return
    try:
        cands = await find_candidates(text)
    except Exception as e:
        await event.reply(f"❌ Search fail: <code>{html.escape(str(e))}</code>", buttons=back, parse_mode="html")
        return
    if not cands:
        await event.reply("❌ Kuch nahi mila. Naam ka thoda hissa, @username ya numeric ID try karo.", buttons=back)
    elif len(cands) == 1:
        d = await make_dest(cands[0])
        fresh = await add_dest(d)
        await event.reply(f"✅ Add hua: <b>{html.escape(d['title'])}</b>" if fresh else "ℹ️ Ye pehle se list me hai.", parse_mode="html")
        await show_dests()
    else:
        state["cands"] = cands
        rows = [[Button.inline(f"{'💬' if not isinstance(c, Channel) else '📢'} {(utils.get_display_name(c) or str(c.id))[:30]}", f"dpick_{i}".encode())]
                for i, c in enumerate(cands)]
        await event.reply("Kaun sa chat add karna hai?", buttons=rows + back)


async def on_message(event):
    if event.sender_id != OWNER_ID:
        return
    text = (event.raw_text or "").strip()
    mode = state["mode"]

    if text.startswith("/bulkstop"):
        b = bulk_get()
        await store.set(bulk={**b, "running": False})
        await event.reply("⏹ Bulk rokne ka signal bhej diya (current post khatam hone par ruk jayega).")
        return

    if text.startswith("/bulkstatus"):
        b = bulk_get()
        posts = load_posts(b.get("chat_id")) if b.get("chat_id") else None
        total = len(posts) if posts else "?"
        run = bool(bulk_task and not bulk_task.done())
        ra = b.get("retry_at")
        left = f"\n⏰ Auto-retry {max(0, int((ra - time.time()) / 60))} min me" if ra else ""
        await event.reply(f"📊 chat {b.get('chat_id')} · running={run}\n{bulk_summary(b, total)}{left}")
        return

    if text.startswith("/bulkreset"):
        if bulk_task and not bulk_task.done():
            await event.reply("⚠️ Pehle /bulkstop karke ruk jane do.")
            return
        if "confirm" not in text:
            await event.reply("⚠️ Saara progress hat jayega (jo ho chuke wo dobara honge).\n"
                              "Pakka? Bhejo: <code>/bulkreset confirm</code>", parse_mode="html")
            return
        b = bulk_get()
        await store.set(bulk={"chat_id": b.get("chat_id"), "running": False, "mode": "main",
                              "done": list(SEED_DONE), "failed": {}, "retry_at": None})
        await event.reply(f"🧹 Progress reset. {len(SEED_DONE)} posts (Termux se ho chuke) done maane gaye.\n"
                          "Ab <code>/bulk -1004471150773</code> bhejo.", parse_mode="html")
        return

    if text.startswith("/bulkretry"):
        b = bulk_get()
        if not b.get("chat_id") or not b["failed"]:
            await event.reply("Koi failed post nahi hai.")
            return
        _, cnt = parse_bulk_args(text)
        stop = len(b["done"]) + cnt if cnt else None
        await store.set(bulk={**b, "retry_at": None, "stop_at": stop})
        ok = await bulk_start(b["chat_id"], "retry")
        await event.reply(f"🔁 {len(b['failed'])} failed posts retry ho rahe hain…" if ok else "⚠️ Bulk pehle se chal raha hai.")
        return

    if text.startswith("/bulk"):
        cid, cnt = parse_bulk_args(text)
        cid = cid or bulk_get().get("chat_id")
        if not cid:
            await event.reply("Use: <code>/bulk 10</code> ya <code>/bulk -1004471150773 10</code>", parse_mode="html")
            return
        b = bulk_get()
        if str(b.get("chat_id")) != str(cid):
            b = {"done": list(SEED_DONE) if str(cid) == "-1004471150773" else [], "failed": {}}
        stop = len(b["done"]) + cnt if cnt else None
        await store.set(bulk={**b, "chat_id": str(cid), "running": True, "mode": "main", "stop_at": stop})
        ok = await bulk_start(cid, "main")
        msg = f"🚀 Bulk shuru: {cnt} download" if cnt else "🚀 Bulk shuru: sab pending posts"
        await event.reply(msg if ok else "⚠️ Bulk pehle se chal raha hai.")
        return

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

    if mode == "add_dest":
        await handle_add_dest(event, text)
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

    elif d == "dests":
        await event.answer()
        await show_dests(event)

    elif d == "dadd":
        state["mode"] = "add_dest"
        await event.edit(
            "➕ <b>Destination add karo</b>\n\n"
            "• 📢 Channel ka <b>koi post forward karo</b>\n"
            "• ya <b>@username / t.me link / numeric ID</b> bhejo\n"
            "• ya personal chat/group ka <b>naam</b> likho (search hoga)\n\n"
            "💡 Channel me bot ko Admin banao. Personal chat/Saved ke liye account login hona chahiye.",
            buttons=[[Button.inline("❌ Cancel", b"dests")]],
            parse_mode="html",
        )

    elif d == "dsaved":
        fresh = await add_dest({"id": "saved", "kind": "saved", "title": "Saved Messages", "pid": "saved",
                                "username": None, "hash": None, "on": True})
        await event.answer("Saved Messages add ✅" if fresh else "Pehle se add hai")
        await show_dests(event)

    elif d.startswith("dpick_"):
        cands = state.get("cands") or []
        i = int(d[6:])
        if i >= len(cands):
            await event.answer("List purani ho gayi, dobara search karo", alert=True)
            return
        nd = await make_dest(cands[i])
        fresh = await add_dest(nd)
        state["cands"] = []
        await event.answer("Add ho gaya ✅" if fresh else "Pehle se hai")
        await show_dests(event)

    elif d.startswith("dt_"):
        ds = store.data.get("dests") or []
        for x in ds:
            if x["id"] == d[3:]:
                x["on"] = not x.get("on", True)
        await store.set(dests=ds)
        await event.answer("Update ✅")
        await show_dests(event)

    elif d.startswith("dd_"):
        ds = [x for x in (store.data.get("dests") or []) if x["id"] != d[3:]]
        await store.set(dests=ds)
        await event.answer("Hata diya 🗑")
        await show_dests(event)

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
    b = bulk_get()
    if b.get("running") and b.get("chat_id"):
        await bulk_start(b["chat_id"], b.get("mode", "main"))  # restart ke baad auto-resume
    asyncio.create_task(retry_watcher())
    try:
        await render()
    except Exception:
        log.warning("Owner ne bot ko /start nahi kiya abhi tak")
    log.info("Bot online")
    await bot.run_until_disconnected()


if __name__ == "__main__":
    asyncio.run(main())
