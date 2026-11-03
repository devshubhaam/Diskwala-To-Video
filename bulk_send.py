"""
bulk_send.py - backup JSON ke posts (video + caption) ek-ek karke
tumhare Diskwala-To-Video control bot ko bhejta hai.

Flow (har post ke liye):
  1. Autoposter bot (jiske file_id hain) -> post tumhare DM me bhejta hai
  2. Tumhara account (Telethon) us message ko control bot ko forward karta hai
  3. Control bot ka "Backup complete / skip / error" status aane tak wait
  4. progress.json me save (crash ho to wahi se resume)

Install:  pip install telethon aiohttp
Env vars:
  API_ID, API_HASH      -> my.telegram.org
  POSTER_BOT_TOKEN      -> jis bot ne ye posts originally bheje (file_id usi ke hain)
  CONTROL_BOT           -> control bot ka username, e.g. my_diskwala_bot
  OWNER_ID              -> tumhara numeric Telegram ID (control bot ka OWNER_ID)
Pehle poster bot ko apne DM me /start kar lo.

Run:
  python bulk_send.py autobackup.json -1004471150773
  python bulk_send.py autobackup.json -1004471150773 --start 750 --limit 100
"""
import argparse
import asyncio
import json
import os
import re

import aiohttp
from telethon import TelegramClient, events

API_ID = int(os.environ["API_ID"])
API_HASH = os.environ["API_HASH"]
POSTER_TOKEN = os.environ["POSTER_BOT_TOKEN"]
CONTROL = os.environ["CONTROL_BOT"].lstrip("@")
OWNER_ID = int(os.environ["OWNER_ID"])

DONE_RE = re.compile(r"Backup complete|Koi video nahi|Error|Cancel ho gaya|nahi mila|Pehle", re.I)
ABORT_RE = re.compile(r"Pehle account login|Pehle Backup Channel", re.I)
PER_POST_TIMEOUT = 20 * 60  # seconds
DISKWALA = re.compile(r"diskwala\.com/app/", re.I)

METHODS = {
    "video": ("sendVideo", "video"),
    "photo": ("sendPhoto", "photo"),
    "animation": ("sendAnimation", "animation"),
    "document": ("sendDocument", "document"),
}


async def poster_send(http, post):
    mt = post.get("media_type")
    if mt not in METHODS or not post.get("file_id"):
        return False
    method, field = METHODS[mt]
    payload = {
        "chat_id": OWNER_ID,
        field: post["file_id"],
        "caption": post.get("caption") or "",
        "parse_mode": post.get("parse_mode") or "HTML",
    }
    async with http.post(f"https://api.telegram.org/bot{POSTER_TOKEN}/{method}", json=payload) as r:
        data = await r.json()
    if not data.get("ok"):
        print("  poster send fail:", data.get("description"))
        return False
    return True


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("backup")
    ap.add_argument("chat_id")
    ap.add_argument("--start", type=int, default=0)
    ap.add_argument("--limit", type=int, default=None)
    a = ap.parse_args()

    posts = json.load(open(a.backup, encoding="utf-8"))["posts"][a.chat_id]
    posts = [p for p in posts if p.get("active", True) and DISKWALA.search(p.get("caption") or "")]
    posts = posts[a.start : (a.start + a.limit) if a.limit else None]

    pf = f"progress_{a.chat_id}.json"
    done = set(json.load(open(pf))) if os.path.exists(pf) else set()
    todo = [p for p in posts if p["_id"] not in done]
    print(f"{len(posts)} posts selected, {len(todo)} baaki")

    client = TelegramClient("bulk_session", API_ID, API_HASH)
    await client.start()  # pehli baar phone + OTP maangega

    me_poster = None
    async with aiohttp.ClientSession() as http:
        async with http.get(f"https://api.telegram.org/bot{POSTER_TOKEN}/getMe") as r:
            me_poster = (await r.json())["result"]["username"]

        control = await client.get_entity(CONTROL)
        state = {"ev": asyncio.Event(), "text": "", "abort": False}

        async def on_ctrl(event):
            t = event.raw_text or ""
            if ABORT_RE.search(t):
                state["abort"] = True
                state["text"] = t
                state["ev"].set()
            elif DONE_RE.search(t):
                state["text"] = t
                state["ev"].set()

        client.add_event_handler(on_ctrl, events.NewMessage(from_users=control.id, incoming=True))
        client.add_event_handler(on_ctrl, events.MessageEdited(from_users=control.id))

        for n, p in enumerate(todo, 1):
            print(f"[{n}/{len(todo)}] {p['_id']}")
            state["ev"].clear()
            state["abort"] = False

            if not await poster_send(http, p):
                continue
            await asyncio.sleep(1.5)
            msgs = await client.get_messages(me_poster, limit=1)
            await client.forward_messages(control, msgs[0])

            try:
                await asyncio.wait_for(state["ev"].wait(), PER_POST_TIMEOUT)
            except asyncio.TimeoutError:
                print("  timeout, aage badh raha hoon")
            if state["abort"]:
                print("Control bot ne roka:", state["text"])
                return
            print("  ->", state["text"].replace("\n", " ")[:80])

            done.add(p["_id"])
            json.dump(sorted(done), open(pf, "w"))
            await asyncio.sleep(3)

    print("Khatam.")


asyncio.run(main())
