# app_full_admin_revised.py — LINE Bot ตอบอัตโนมัติ (ไม่ใช้ .env)
import logging
logging.getLogger('werkzeug').setLevel(logging.ERROR)

import re, time, os, io, json, hashlib, csv, threading
from datetime import datetime, timezone, timedelta
import regex as re2
import requests
from flask import Flask, request, abort
from linebot import LineBotApi, WebhookHandler
from linebot.exceptions import InvalidSignatureError
from linebot.models import (
    MessageEvent, TextMessage, ImageMessage, TextSendMessage, ImageSendMessage,
    UnsendEvent, FlexSendMessage
)
from linebot.v3.messaging import Configuration, ApiClient, MessagingApiBlob
from easy_slipcheck.verify_easyslip import verify_slip # ต้องมีการติดตั้งไลบรารีนี้

# ==============================================================================
# 🚀 1. CONFIGURATION & GLOBAL STATES
# ==============================================================================

# --- A. LINE & Server Credentials ---
SLIP2GO_SECRET_KEY = 'YiYE5OMCPMzt_j_3weY4NNQ3KkvoOPj1BG__+1OzgYI='
LINE_CHANNEL_ACCESS_TOKEN = "zBm4duYOnB1sDsRA//oaVnYFHp1aUbtfRtmytYa3WseksJSeTO7n9ZfSelOR2HVuylB9VQIssDmRDavTlnYeYfU0q2OvDVILPswa2IIG/rf2mRvbBNXj34IOCcN0g+dcDm9xM0u4QYOLla3vUnFg+QdB04t89/1O/w1cDnyilFU="
LINE_CHANNEL_SECRET = "b9d6064e91dedec3a7652cf35f5d911d"
BASE_PUBLIC_URL = "https://YOUR_DOMAIN"
OA_CHAT_URL = "https://page.line.me/073nobti"

# --- B. Admin & Group Config ---
ADMIN_UIDS = {
    "U255dd67c1fef32fb0eae127149c7cadc", "Uf7e207bfdd69d8e41806436fa7a86c14",
    "U163186c5013c8f1e4820291b7b1d86bd", "Uc2013ea8397da6d19cbe0f931a04c949",
    "U2f156aa5effee7c1ee349b9320a35381",  "Ua914df11d1747d2eea4fbdd06a9c1052",
    "Uf425373fafd5fddfc3a3a87a091d1cbe",  "U12c10eb2c9180da67129f881acb3d82c",
    "Uabd44b316349c4ae7c5709fcc2ac69d6",  "U511f830f04b19951a74a76e509f92ff6",
}
TARGET_GROUP_NAME = "🚀บั้งไฟแสน • เถ้าแก่น้อย •"

# --- B.1 Auto insufficient-credit trigger ---
AUTO_INSUFFICIENT_CREDIT_UIDS = {"ADMINXXXXXXXXXX"}
AUTO_INSUFFICIENT_CREDIT_KEYWORDS = {"ติด", "ต", "ตต", "จ"}

# --- C. File Paths & Locks ---
MEDIA_DIR = os.path.join(os.path.dirname(__file__), "media")
USERS_TXT_PATH = os.path.join(os.path.dirname(__file__), "oa_users.txt")
_USERS_LOCK = threading.Lock()



# --- D. Global States & Caches ---
PEH_LIST = {}    # dict[source_key] = [ "ข้อความ..." ]
SUMMARY_STATS = {"passed": 0, "failed": 0, "draw": 0}
USED_SLIP_REF = set()
MSG_CACHE = {}
CACHE_TTL_SEC = 3600
COOLDOWN_SEC = 10
_LAST_CMD_AT = {}
TZ_BKK = timezone(timedelta(hours=7))

# --- E. Slip Check Rules ---
VALID_RECEIVERS = [
    "กิตติเชษฐ์ บุญอินทร์", "นาย กิตติเชษฐ์ บุญอินทร์", "Mr. kittichet boonin", "Mr. Kittichet Boonin", "MR. KITTICHET BOONIN" ,"KITTICHET BOONIN"
]
MIN_AMOUNT = 1

# --- F. Standard Replies (Plain Text) ---
ACCOUNT_TEXT = (

    "📢แจ้งเปลี่ยนบัญชี นะครับ⚠️\n\n"

    "💵💰บัญชีฝากเงิน • เถ้าแก่น้อย • 💰💸\n\n"
    "©️ก๊อปข้อความแล้ววางในแอพได้เลย\n"
    "💰เลขบัญชี : 020424046959\n"
    "🎫ชื่อ : กิตติเชษฐ์ บุญอินทร์\n"
    "👛ธนาคาร : ออมสิน\n\n"
    "💰ฝาก-ถอน ต้องใช้บัญชี เดียวกันเท่านั้นนะครับ ✅\n\n"
    "🙏โอนแล้วกดปุ่มสีเขียวส่งสลิปให้แอดมินหลังบ้านได้เลย🚀"
)

# ==============================================================================
# 🛠️ 2. UTILITY FUNCTIONS
# ==============================================================================

def today_th():
    return datetime.now(TZ_BKK).strftime("%d/%m/%Y")

def count_result_from_items(items):
    passed = failed = draw = 0

    for item in items:
        tail = item.get("tail", "")

        # นับแค่ 1 ต่อรายการ (ต่อให้ emoji ซ้ำ)
        if "✅" in tail:
            passed += 1
        elif "❌" in tail:
            failed += 1
        elif "⛔" in tail or "⚖" in tail:
            draw += 1

    return passed, failed, draw

def _hit_cooldown(event, cmd_name: str) -> bool:
    now = time.time()
    key = f"{_source_key(event)}::{cmd_name}"
    last = _LAST_CMD_AT.get(key, 0)
    cooldowns = {"rules_exact": 240, "account": 60, "summary": 60, "slip_check": 10}
    cd = cooldowns.get(cmd_name, COOLDOWN_SEC)
    if now - last < cd:
        return True
    _LAST_CMD_AT[key] = now
    return False

def _source_key(event) -> str:
    src = event.source
    return getattr(src, "group_id", None) or getattr(src, "room_id", None) or getattr(src, "user_id", None) or "global"

def _display_name(event):
    src = event.source
    uid = getattr(src, "user_id", None)
    name = None
    try:
        if getattr(src, "group_id", None) and uid:
            prof = line_bot_api.get_group_member_profile(src.group_id, uid)
            name = prof.display_name
        elif getattr(src, "room_id", None) and uid:
            prof = line_bot_api.get_room_member_profile(src.room_id, uid)
            name = prof.display_name
        elif uid:
            prof = line_bot_api.get_profile(uid)
            name = prof.display_name
    except Exception:
        name = None
    return name or f"user:{uid[:6]}…" if uid else "ลูกค้า"

# ==============================================================================
def _push_to_source(event_source, message):
    st = getattr(event_source, "type", None)
    try:
        # ตรวจสอบว่าเป็น Group หรือ Room หรือ User
        if st == "group" and getattr(event_source, "group_id", None):
            line_bot_api.push_message(event_source.group_id, message)
        elif st == "room" and getattr(event_source, "room_id", None):
            line_bot_api.push_message(event_source.room_id, message)
        elif getattr(event_source, "user_id", None):
            line_bot_api.push_message(event_source.user_id, message)
    except Exception as e:
        # พิมพ์ Error ออกมาดูทางหน้าจอดำ (Console)
        print(f"❌ PUSH MESSAGE FAILED: {e}")

# --- File/Cache Utilities ---

def _cache_put(message_id, info):
    now = time.time()
    MSG_CACHE[message_id] = {"ts": now, **info}
    _cache_gc(now)

def _cache_get(message_id):
    data = MSG_CACHE.get(message_id)
    if not data: return None
    if time.time() - data.get("ts", 0) > CACHE_TTL_SEC:
        MSG_CACHE.pop(message_id, None)
        return None
    return data

def _cache_gc(now=None):
    now = now or time.time()
    expired = [mid for mid, v in MSG_CACHE.items() if now - v.get("ts", 0) > CACHE_TTL_SEC]
    for mid in expired:
        MSG_CACHE.pop(mid, None)

def _load_users_txt() -> dict:
    data = {}
    if os.path.exists(USERS_TXT_PATH):
        try:
            with open(USERS_TXT_PATH, "r", encoding="utf-8", newline="") as f:
                reader = csv.reader(f, delimiter="\t")
                for row in reader:
                    if not row: continue
                    uid = row[0].strip()
                    name = row[1].strip() if len(row) > 1 else ""
                    if uid: data[uid] = name
        except Exception:
            data = {}
    return data

def _save_user_to_txt(uid: str, display_name: str):
    if not uid: return
    display_name = display_name or ""
    with _USERS_LOCK:
        data = _load_users_txt()
        if data.get(uid) == display_name: return
        data[uid] = display_name
        tmp_path = USERS_TXT_PATH + ".tmp"
        with open(tmp_path, "w", encoding="utf-8", newline="") as f:
            writer = csv.writer(f, delimiter="\t")
            for k in sorted(data.keys()):
                writer.writerow([k, data[k]])
        os.replace(tmp_path, USERS_TXT_PATH)

def _search_uid_by_name(name_query: str, limit: int = 5):
    name_query = (name_query or "").strip().casefold()
    if not name_query: return []
    data = _load_users_txt()
    results = []
    for uid, name in data.items():
        if (name or "").casefold().find(name_query) != -1:
            results.append((uid, name))
        if len(results) >= limit: break
    return results

# --- Peh (List) Utilities ---

# ===== ใส่เพิ่มไว้แถว ๆ "Peh (List) Utilities" ก่อน _add_peh_item ก็ได้ =====

def _base_peh_name(name: str) -> str:
    """ตัดท้าย (ตัวเลข) ออก เพื่อใช้เป็นชื่อฐานในการเทียบซ้ำ"""
    name = (name or "").strip()
    # แก้ไข: ตรวจสอบเว้นวรรคที่ท้ายชื่อให้สะอาดขึ้น
    return re.sub(r"\s*\(\d+\)\s*$", "", name).strip()


def _dedupe_peh_name(existing_items, raw_name: str, max_len: int = 40) -> str:
    base = _base_peh_name(raw_name) or "-"
    max_idx = 0

    # เช็คค่าชื่อซ้ำ
    for it in (existing_items or []):
        n = (it.get("name") or "").strip()
        if _base_peh_name(n) != base:
            continue

        m = re.search(r"\((\d+)\)\s*$", n)
        idx = int(m.group(1)) if m else 1
        if idx > max_idx:
            max_idx = idx

    next_idx = 1 if max_idx == 0 else (max_idx + 1)
    suffix = "" if next_idx == 1 else f"({next_idx})"

    # ทำการตัดชื่อเมื่อมันยาวเกินกว่าความยาวที่กำหนด
    keep = max_len - len(suffix)
    base_cut = base[:keep] if len(base) > keep else base

    return f"{base_cut}{suffix}"



# ===== แก้ _add_peh_item เดิมให้เป็นแบบนี้ =====
def _add_peh_item(event, text):
    try:
        key = _source_key(event)
        if key not in PEH_LIST:
            PEH_LIST[key] = []

        name, tail = format_peh_text_anyway(text)
        name = name if name else "-"

        # ถ้าชื่อซ้ำ: ใช้ชื่อที่ได้จากการจัดการ
        deduped_name = _dedupe_peh_name(PEH_LIST[key], name, max_len=40)

        PEH_LIST[key].append({
            "name": deduped_name,  # ใช้ชื่อที่ผ่านการกันซ้ำแล้ว
            "tail": tail[:6]
        })

        # 🔥 HARD LIMIT 100 รายการ
        if len(PEH_LIST[key]) > 100:
            PEH_LIST[key] = PEH_LIST[key][-100:]

        return flex_peh_list_pages(
            "• สกอเถ้าแก่น้อย •",
            PEH_LIST[key],
            page_size=30
        )
    except Exception as e:
        print(f"Error while adding peh item: {e}")
        return None





def format_peh_text_anyway(raw_text):
    text = raw_text.strip()

    # 1) ตัดคำสั่งนำหน้า "เปะ" หรือ "ตึ้ง" ออกจากด้านหน้า (ถ้ามี)
    text = re.sub(r"^(?:เปะ|ตึ้ง)\s*", "", text).strip()

    # 2) รองรับเคสพิเศษ: ไม่มีตัวเลข แต่มีคำว่า "หาย"
    # ตัวอย่าง: "ทรัพย์สุวรรณ์ หาย⛔️⛔️"
    lost_match = re.match(r"^(.*?)\s+หาย\s*(.*)$", text)
    if lost_match:
        name = lost_match.group(1).strip() or text
        tail_raw = lost_match.group(2).strip()
        emojis = re.findall(r"[\U0001F300-\U0001FAFF\u2600-\u27BF]+", tail_raw)
        emoji_text = "".join(emojis)[:2]
        tail = f"หาย{emoji_text}"
        return name, tail

    # 3) ใช้ Regex แยกชื่อและราคา (ตัวเลขต้องมี 2 หลักขึ้นไปถึงจะถูกแยกออกมาเป็นราคา)
    # กลุ่ม 1 (.*?) คือชื่อ: จะหยุดเมื่อเจอตัวเลข 2 หลัก
    # กลุ่ม 2 (\d{2,}) คือราคา: ดึงตัวเลขตั้งแต่ 2 หลักขึ้นไป
    # กลุ่ม 3 (.*) คือส่วนท้าย: ข้อความหรือ Emoji ที่เหลือ
    match = re.search(r"^(.*?)(?=\d{2,})(\d{2,})(.*)$", text)

    if not match:
        # ถ้าหาตัวเลข 2 หลักไม่เจอ และไม่มีคำว่า "หาย" ให้มองว่าข้อความทั้งหมดคือชื่อ และไม่มีส่วนท้าย
        return text, ""

    name = match.group(1).strip()     # ส่วนของชื่อ
    number = match.group(2)           # ส่วนของตัวเลข (เช่น 290)
    tail_raw = match.group(3).strip() # ส่วนของ Emoji/ข้อความต่อท้าย

    # 4) ดึง Emoji จากส่วนท้าย และจำกัดให้เหลือสูงสุด 2 ตัว
    emojis = re.findall(
        r"[\U0001F300-\U0001FAFF\u2600-\u27BF]+",
        tail_raw
    )
    # รวม Emoji ทั้งหมดเข้าด้วยกัน แล้วตัดเอาแค่ 2 ตัวแรก
    emoji_text = "".join(emojis)[:2]

    # 5) รวมเลข + emoji (สูงสุด 2 ตัว) เพื่อแสดงผลในช่อง "ท้าย"
    tail = f"{number}{emoji_text}"

    return name, tail



def remove_item_and_shift(event, index_to_remove):
    key = _source_key(event)

    if key not in PEH_LIST or not PEH_LIST[key]:
        return "❌ ไม่มีรายการให้ลบ"

    if index_to_remove < 1 or index_to_remove > len(PEH_LIST[key]):
        return f"❌ ไม่พบรายการลำดับที่ {index_to_remove}"

    removed = PEH_LIST[key].pop(index_to_remove - 1)

    return f"✅ ลบรายการที่ {index_to_remove} เรียบร้อยแล้ว"

# --- Slip Check Utilities ---

def slip_fingerprint(data):
    sender_acc = data.get("sender", {}).get("account", {})
    receiver_acc = data.get("receiver", {}).get("account", {})
    core = {
        "s_no": sender_acc.get("number", ""),
        "r_no": receiver_acc.get("number", ""),
        "amount": data.get("amount", ""),
        "trans_at": data.get("dateTime") or data.get("transDate") or data.get("paidAt") or data.get("transactionDate") or data.get("createdAt") or "",
        "bank_ref": data.get("referenced", "")
    }
    raw = json.dumps(core, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()

def format_slip_datetime(dt_str):
    try:
        # ตัวอย่าง: 2024-11-19T10:45:22+07:00
        dt = datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
        dt_th = dt.astimezone(TZ_BKK)
        return dt_th.strftime("%d/%m/%Y %H:%M")
    except:
        return "-"

def diff_minutes(dt1, dt2):
    if not isinstance(dt1, datetime) or not isinstance(dt2, datetime): return "-"
    diff = dt2 - dt1
    secs = int(diff.total_seconds())
    mins = secs // 60

    if secs < 60:
        return f"{secs} วินาที"

    hours = mins // 60
    remain = mins % 60
    parts = []
    if hours > 0: parts.append(f"{hours} ชั่วโมง")
    if remain > 0: parts.append(f"{remain} นาที")
    return " ".join(parts) if parts else "ไม่ถึง 1 นาที"

# ==============================================================================
# 🎨 3. FLEX MESSAGE TEMPLATES
# ⚠️ (โค้ดทั้งหมดของ Flex Message ถูกรวมไว้ที่นี่)
# ==============================================================================

def _progress_bar(value: int, total: int, color: str, track="#E5E7EB", height="12px"):
    value = max(0, value)
    total = max(1, total)
    filled = value
    remain = max(0, total - value)
    return {
        "type": "box",
        "layout": "vertical",
        "contents": [{
            "type": "box", "layout": "horizontal", "height": height,
            "cornerRadius": "999px", "backgroundColor": track,
            "contents": [
                {"type": "box", "layout": "vertical", "cornerRadius": "999px", "backgroundColor": color, "contents": [], "flex": filled},
                {"type": "box", "layout": "vertical", "cornerRadius": "999px", "backgroundColor": "#00000000", "contents": [], "flex": remain}
            ]
        }]
    }

def flex_account_v2():
    return {
        "type": "bubble",
        "size": "kilo",
        "body": {
            "type": "box",
            "layout": "vertical",
            "backgroundColor": "#F8FAFC",
            "paddingAll": "0px",
            "contents": [
                {
                    "type": "box",
                    "layout": "vertical",
                    "paddingAll": "20px",
                    "contents": [
                        {
                            "type": "text",
                            "text": "ยืนยันยอดฝากเงิน",
                            "weight": "bold",
                            "size": "md",
                            "color": "#1E293B",
                            "align": "center"
                        },
                        {
                            "type": "text",
                            "text": "โอนแล้วส่งสลิปผ่านปุ่มด้านล่างครับ",
                            "size": "xs",
                            "color": "#64748B",
                            "align": "center",
                            "margin": "sm"
                        }
                    ]
                },
                {
                    "type": "box",
                    "layout": "vertical",
                    "paddingAll": "12px",
                    "paddingTop": "0px",
                    "contents": [
                        {
                            "type": "button",
                            "action": {
                                "type": "uri",
                                "label": "📤 ส่งสลิปให้แอดมิน",
                                "uri": OA_CHAT_URL
                            },
                            "style": "primary",
                            "color": "#10B981",
                            "height": "md",
                            "offsetBottom": "5px"
                        }
                    ]
                }
            ]
        },
        "styles": {
            "body": {
                "cornerRadius": "20px"
            }
        }
    }

def flex_peh_list_pages(title, items, page_size=30):
    MAX_PAGES = 5
    PAGE_SIZE = 30
    MAX_ITEMS = MAX_PAGES * PAGE_SIZE  # = 150

    items = items[:MAX_ITEMS]
    bubbles = []
    total_pages = (len(items) + PAGE_SIZE - 1) // PAGE_SIZE


    passed, failed, draw = count_result_from_items(items)
    summary_text = f"✅ ผ่าน {passed}   ❌ แพ้ {failed}   ⛔ จาว {draw}"

    for page in range(total_pages):
        start = page * PAGE_SIZE
        end = start + PAGE_SIZE
        page_items = items[start:end]

        contents = [
            {
                "type": "text",
                "text": f"• {title} •",
                "weight": "bold",
                "size": "md",
                "align": "center",
                "color": "#0F172A"
            },
            {
                "type": "text",
                "text": f"📅 วันที่ {today_th()}",
                "size": "xs",
                "align": "center",
                "color": "#64748B",
                "margin": "none"
            },
            {
                "type": "text",
                "text": summary_text,
                "size": "sm",
                "align": "center",
                "weight": "bold",
                "color": "#334155",
                "margin": "sm"
            },
            # 🔽 เพิ่มตรงนี้ 🔽
            {
                "type": "text",
                "text": "👉 เลื่อนขวาเพื่อดูรายการเพิ่มเติม",
                "size": "xs",
                "align": "center",
                "color": "#64748B",
                "margin": "xs"
            },
            {
                "type": "separator",
                "margin": "sm",
                "color": "#E5E7EB"
            }
        ]

        # =========
        # LIST
        # =========
        for i, item in enumerate(page_items, start=start + 1):
            contents.append({
                "type": "box",
                "layout": "horizontal",
                "alignItems": "center",
                "contents": [
                    # ลำดับ
                    {
                        "type": "text",
                        "text": f"{i}.",
                        "size": "sm",
                        "color": "#475569",
                        "weight": "bold",
                        "flex": 0
                    },

                    # ชื่อ
                    {
                        "type": "text",
                        "text": item["name"],
                        "size": "sm",
                        "color": "#111827",
                        "wrap": True,
                        "flex": 1,
                        "margin": "sm"
                    },

                    # ราคา + ผล (ชิดขวา)
                    {
                        "type": "text",
                        "text": item["tail"] or "",
                        "size": "sm",
                        "weight": "bold",
                        "color": "#0F172A",
                        "align": "end",
                        "flex": 0
                    }
                ]
            })

            # เส้นคั่นบางมาก
            contents.append({
                "type": "separator",
                "margin": "none",
                "color": "#F1F5F9"
            })

        bubbles.append({
            "type": "bubble",
            "size": "giga",
            "body": {
                "type": "box",
                "layout": "vertical",
                "paddingAll": "12px",  # เท่าเดิม
                "backgroundColor": "#FFFFFF",
                "contents": contents
            }
        })

    return {
        "type": "carousel",
        "contents": bubbles
    }


from linebot.models import FlexSendMessage

# ฟังก์ชันสำหรับส่งข้อความ Flex เกี่ยวกับวิธีการชม/คอล
def send_watch_rules_flex(event):
    # สร้าง Flex Message สำหรับการชม/คอล
    flex_message = {
        "type": "bubble",
        "size": "giga",
        "body": {
            "type": "box",
            "layout": "vertical",
            "paddingAll": "20px",
            "spacing": "md",
            "backgroundColor": "#FFFDF5",
            "contents": [
                {
                    "type": "text",
                    "text": "📜 วิธีการชมคอล / ไลฟ์สด",
                    "weight": "bold",
                    "size": "xl",
                    "color": "#E04126",
                    "align": "center"
                },
                {
                    "type": "separator",
                    "margin": "md",
                    "color": "#FCD34D"
                },
                {
                    "type": "text",
                    "text": "🚫 ห้ามเปิดไมค์ทุกกรณี หากเปิดไมค์แอดมินจะเตะออกกลุ่มทันที",
                    "wrap": True,
                    "size": "md",
                    "color": "#111827",
                    "margin": "md"
                },
                {
                    "type": "text",
                    "text": "🙏 รบกวรขอความร่วมมือจากสมาชิกทุกท่านด้วยนะครับ",
                    "wrap": True,
                    "size": "md",
                    "color": "#1F2937",
                    "margin": "md"
                },
                {
                    "type": "separator",
                    "margin": "md",
                    "color": "#FCD34D"
                },
                {
                    "type": "text",
                    "text": f"• กลุ่ม: {TARGET_GROUP_NAME}",
                    "size": "xs",
                    "color": "#94A3B8",
                    "align": "center",
                    "margin": "md"
                }
            ]
        }
    }

    # ส่งข้อความ Flex ไปยังผู้ใช้งาน
    line_bot_api.reply_message(
        event.reply_token,
        FlexSendMessage(alt_text="วิธีการชมคอล / ไลฟ์สด", contents=flex_message)
    )


def flex_summary_bungfai(passed, failed, draw, title_date="วันนี้"):
    total = max(1, passed + failed + draw)
    GREEN, RED, YELLOW = "#16A34A", "#DC2626", "#F59E0B"
    CHIP_BG = {"green": "#DCFCE7", "red": "#FEE2E2", "yellow": "#FEF9C3"}
    TEXT_PRIMARY, TEXT_SECONDARY = "#111827", "#64748B"
    ACCENT, SURFACE, CANVAS = "#0EA5E9", "#FFFFFF", "#F8FAFC"

    def chip(text, bg, fg):
        return {"type": "box", "layout": "baseline", "cornerRadius": "999px", "backgroundColor": bg, "paddingAll": "8px", "contents": [{"type": "text", "text": text, "size": "sm", "color": fg, "weight": "bold", "align": "center"}]}

    def row(label_left: str, value: int, color: str, chip_bg: str, emoji: str, track="#E5E8F0"):
        return {
            "type": "box", "layout": "vertical", "spacing": "sm", "margin": "md",
            "contents": [
                {"type": "box", "layout": "horizontal", "contents": [
                    {"type": "text", "text": f"{emoji} {label_left}", "size": "md", "weight": "bold", "color": TEXT_PRIMARY, "align": "center", "flex": 1},
                    {"type": "text", "text": f"{value} บั้ง", "size": "md", "align": "center", "color": TEXT_PRIMARY, "weight": "bold", "flex": 1}
                ]},
                _progress_bar(value, total, color=color, track=track, height="12px"),
                {"type": "box", "layout": "horizontal", "justifyContent": "center", "contents": [
                    {"type": "box", "layout": "baseline", "cornerRadius": "999px", "backgroundColor": chip_bg, "paddingAll": "6px", "contents": [
                        {"type": "text", "text": f"{(value * 100 / total):.0f}%", "size": "xs", "color": TEXT_PRIMARY, "weight": "bold", "align": "center"}
                    ]}
                ]}
            ]
        }

    subtitle = f"รวม {total} บั้ง • {title_date}"
    return {
        "type": "bubble", "size": "giga",
        "body": {
            "type": "box", "layout": "vertical", "backgroundColor": CANVAS,
            "contents": [{
                "type": "box", "layout": "vertical", "cornerRadius": "16px",
                "backgroundColor": SURFACE, "paddingAll": "20px", "spacing": "lg",
                "contents": [
                    {"type": "text", "text": "สรุปผลบั้งไฟวันนี้", "weight": "bold", "size": "xl", "align": "center", "color": ACCENT},
                    {"type": "text", "text": subtitle, "size": "sm", "align": "center", "color": TEXT_SECONDARY, "margin": "sm"},
                    {"type": "separator", "margin": "md", "color": "#E2E8F0"},
                    row("ผ่าน", passed, GREEN, CHIP_BG["green"], "✅", track="#E6F4EA"),
                    row("ไม่ผ่าน", failed, RED, CHIP_BG["red"], "❌", track="#F8E7E7"),
                    row("จาว", draw, YELLOW, CHIP_BG["yellow"], "⛔", track="#FFF5CC"),
                    {"type": "box", "layout": "horizontal", "spacing": "md", "margin": "lg", "justifyContent": "center", "contents": [
                        chip("✅ ผ่าน", CHIP_BG["green"], TEXT_PRIMARY),
                        chip("❌ ไม่ผ่าน", CHIP_BG["red"], TEXT_PRIMARY),
                        chip("⛔ จาว", CHIP_BG["yellow"], TEXT_PRIMARY)
                    ]},
                    {"type": "text", "text": f"• กลุ่ม: {TARGET_GROUP_NAME}", "size": "xs", "color": "#94A3B8", "align": "center", "margin": "md", "wrap": True}
                ]
            }]
        },
        "styles": {"body": {"backgroundColor": CANVAS}}
    }

def flex_wait_withdraw_bubble():
    return {
        "type": "bubble",
        "size": "giga",
        "body": {
            "type": "box",
            "layout": "vertical",
            "paddingAll": "0px",
            "contents": [
                {
                    "type": "box",
                    "layout": "vertical",
                    "paddingAll": "20px",
                    "background": {
                        "type": "linearGradient",
                        "angle": "0deg",
                        "startColor": "#1EB5AE",
                        "endColor": "#118D87"
                    },
                    "contents": [
                        {
                            "type": "box",
                            "layout": "horizontal",
                            "contents": [
                                {
                                    "type": "box",
                                    "layout": "vertical",
                                    "width": "42px",
                                    "height": "42px",
                                    "cornerRadius": "50px",
                                    "backgroundColor": "#F3F4F6",
                                    "alignItems": "center",
                                    "justifyContent": "center",
                                    "contents": [
                                        {
                                            "type": "text",
                                            "text": "⏳",
                                            "size": "xl"
                                        }
                                    ]
                                },
                                {
                                    "type": "box",
                                    "layout": "vertical",
                                    "margin": "md",
                                    "flex": 1,
                                    "contents": [
                                        {
                                            "type": "text",
                                            "text": "รอแอดมินทำรายการถอน",
                                            "weight": "bold",
                                            "size": "xl",
                                            "color": "#FFFFFF",
                                            "wrap": True
                                        },
                                        {
                                            "type": "text",
                                            "text": "แอดมินกำลังจัดเรียงคิวถอนให้นะครับ",
                                            "size": "sm",
                                            "color": "#E6FFFB",
                                            "margin": "sm",
                                            "wrap": True
                                        }
                                    ]
                                }
                            ]
                        }
                    ]
                },
                {
                    "type": "box",
                    "layout": "vertical",
                    "backgroundColor": "#DDE7E5",
                    "paddingAll": "18px",
                    "spacing": "md",
                    "contents": [
                        {
                            "type": "text",
                            "text": "กรุณารอสักครู่ แอดมินกำลังทำรายการถอนให้ครับ 🙏",
                            "wrap": True,
                            "align": "center",
                            "size": "md",
                            "weight": "bold",
                            "color": "#163C3A"
                        },
                        {
                            "type": "box",
                            "layout": "vertical",
                            "backgroundColor": "#BFE5DF",
                            "cornerRadius": "14px",
                            "paddingAll": "12px",
                            "contents": [
                                {
                                    "type": "text",
                                    "text": "ไม่เกิน 19:30 ของทุกวันนะครับ",
                                    "align": "center",
                                    "size": "md",
                                    "weight": "bold",
                                    "color": "#18504B",
                                    "wrap": True
                                }
                            ]
                        },
                        {
                            "type": "separator",
                            "color": "#8FCBC4",
                            "margin": "sm"
                        },
                        {
                            "type": "text",
                            "text": "ขออภัยหากเกิดความล่าช้า และขอขอบคุณที่รอครับ 💚",
                            "wrap": True,
                            "align": "center",
                            "size": "sm",
                            "color": "#264A47"
                        },
                        {
                            "type": "text",
                            "text": f"• กลุ่ม: {TARGET_GROUP_NAME} •",
                            "wrap": True,
                            "align": "center",
                            "size": "xs",
                            "color": "#58716D"
                        }
                    ]
                }
            ]
        },
        "styles": {
            "body": {
                "backgroundColor": "#DDE7E5"
            }
        }
    }


def flex_calc_admin_bubble():
    """FLEX แสดงวิธีการคิดยอดของแอดมิน"""
    return {
        "type": "bubble",
        "size": "giga",
        "body": {
            "type": "box",
            "layout": "vertical",
            "paddingAll": "20px",
            "spacing": "md",
            "backgroundColor": "#FFF9EB",
            "contents": [
                {
                    "type": "text",
                    "text": "📊 วิธีการคิดยอดของแอดมิน",
                    "weight": "bold",
                    "size": "xl",
                    "color": "#E04126",
                    "align": "center"
                },
                {
                    "type": "separator",
                    "margin": "sm",
                    "color": "#FCD34D"
                },
                {
                    "type": "text",
                    "text": (
                        "⏱ แอดมินจะใช้เวลาในการคิดยอด\n"
                        "ไม่เกิน 3–5 นาที ต่อบั้ง\n\n"
                        "📋 หากสมาชิกเล่นหลายแผล\n"
                        "แอดมินจะคิดให้ตั้งแต่แผลแรกลงมาเรื่อย ๆ\n"
                        "จนถึงแผลสุดท้ายครับ\n\n"
                        "📩 หากมียอดตกหล่น หรือผิดพลาด\n"
                        "สามารถแจ้งแอดมินได้เลยนะครับ 🙏"
                    ),
                    "wrap": True,
                    "color": "#1F2937",
                    "size": "md",
                    "margin": "md"
                },
                {
                    "type": "separator",
                    "margin": "md",
                    "color": "#FCD34D"
                },
                {
                    "type": "text",
                    "text": "#กลุ่มบั้งไฟแสน  #เถ้าแก่น้อย",
                    "size": "xs",
                    "color": "#A16207",
                    "align": "center",
                    "margin": "md",
                    "weight": "bold"
                }
            ]
        }
    }

def flex_thanks_bubble():
    return {
        "type": "bubble",
        "size": "giga",
        "hero": {
            "type": "image",
            "url": "https://img2.pic.in.th/S__7667715.jpg",
            "size": "full",
            "aspectRatio": "20:13",
            "aspectMode": "cover"
        },
        "body": {
            "type": "box",
            "layout": "vertical",
            "spacing": "sm",
            "paddingAll": "16px",
            "contents": [
                {
                    "type": "text",
                    "text": "วันนี้จบการรายงาน ขอกราบขอบคุณครับ 💖",
                    "weight": "bold",
                    "size": "xl",
                    "align": "center",
                    "color": "#E04126",
                    "wrap": True
                },
                {
                    "type": "text",
                    "text": "ผิดพลาดประการใด ทางทีมงาน กลุ่มเถ้าเเก่น้อยขอโทษ ขออภัย ด้วยนะครับพี่ๆ🙏",
                    "size": "md",
                    "align": "center",
                    "color": "#623112",
                    "wrap": True
                },
                {
                    "type": "separator",
                    "margin": "md"
                },
                {
                    "type": "text",
                    "text": f"• กลุ่ม: {TARGET_GROUP_NAME}",
                    "size": "xs",
                    "align": "center",
                    "color": "#888888",
                    "wrap": True
                }
            ]
        }
    }

def flex_passed(amount, sender_name, receiver_name, trans_date, delay_text):
    def info_row(label, value, value_color="#111827", bold=False):
        return {
            "type": "box",
            "layout": "horizontal",
            "margin": "md",
            "contents": [
                {
                    "type": "text",
                    "text": label,
                    "size": "sm",
                    "color": "#6B7280",
                    "flex": 3,
                    "wrap": True
                },
                {
                    "type": "text",
                    "text": str(value) if value else "-",
                    "size": "sm",
                    "color": value_color,
                    "weight": "bold" if bold else "regular",
                    "align": "end",
                    "flex": 5,
                    "wrap": True
                }
            ]
        }

    try:
        amount_text = f"฿{float(amount):,.2f}"
    except:
        amount_text = f"฿{amount}"

    return {
        "type": "bubble",
        "size": "giga",
        "styles": {
            "body": {
                "backgroundColor": "#F3F4F6"
            }
        },
        "body": {
            "type": "box",
            "layout": "vertical",
            "paddingAll": "0px",
            "contents": [
                # HEADER
                {
                    "type": "box",
                    "layout": "vertical",
                    "paddingTop": "14px",
                    "paddingBottom": "18px",
                    "paddingStart": "16px",
                    "paddingEnd": "16px",
                    "background": {
                        "type": "linearGradient",
                        "angle": "0deg",
                        "startColor": "#10B981",
                        "endColor": "#14B8A6"
                    },
                    "contents": [
                        {
                            "type": "text",
                            "text": "PREMIUM SLIP CHECK",
                            "size": "xs",
                            "color": "#D1FAE5",
                            "weight": "bold"
                        },
                        {
                            "type": "box",
                            "layout": "horizontal",
                            "margin": "md",
                            "alignItems": "center",
                            "contents": [
                                {
                                    "type": "box",
                                    "layout": "vertical",
                                    "width": "42px",
                                    "height": "42px",
                                    "cornerRadius": "100px",
                                    "backgroundColor": "#F3F4F6",
                                    "alignItems": "center",
                                    "justifyContent": "center",
                                    "contents": [
                                        {
                                            "type": "text",
                                            "text": "✓",
                                            "size": "xl",
                                            "weight": "bold",
                                            "color": "#10B981",
                                            "align": "center"
                                        }
                                    ]
                                },
                                {
                                    "type": "box",
                                    "layout": "vertical",
                                    "margin": "md",
                                    "contents": [
                                        {
                                            "type": "text",
                                            "text": "สลิปถูกต้อง",
                                            "size": "xl",
                                            "weight": "bold",
                                            "color": "#FFFFFF"
                                        },
                                        {
                                            "type": "text",
                                            "text": "ระบบตรวจสอบรายการสำเร็จแล้ว",
                                            "size": "xs",
                                            "color": "#ECFDF5",
                                            "margin": "xs"
                                        }
                                    ]
                                }
                            ]
                        }
                    ]
                },

                # CONTENT WRAP
                {
                    "type": "box",
                    "layout": "vertical",
                    "paddingAll": "14px",
                    "spacing": "md",
                    "backgroundColor": "#F3F4F6",
                    "contents": [
                        # AMOUNT CARD
                        {
                            "type": "box",
                            "layout": "vertical",
                            "backgroundColor": "#EAF7EE",
                            "cornerRadius": "18px",
                            "paddingAll": "16px",
                            "contents": [
                                {
                                    "type": "text",
                                    "text": "ยอดที่ตรวจสอบผ่าน",
                                    "size": "sm",
                                    "color": "#6B7280",
                                    "align": "center"
                                },
                                {
                                    "type": "text",
                                    "text": amount_text,
                                    "size": "xxl",
                                    "weight": "bold",
                                    "color": "#065F46",
                                    "align": "center",
                                    "margin": "md"
                                },
                                {
                                    "type": "box",
                                    "layout": "horizontal",
                                    "margin": "md",
                                    "backgroundColor": "#CFF4D8",
                                    "cornerRadius": "999px",
                                    "paddingTop": "8px",
                                    "paddingBottom": "8px",
                                    "paddingStart": "12px",
                                    "paddingEnd": "12px",
                                    "contents": [
                                        {
                                            "type": "text",
                                            "text": "สถานะ : ยืนยันเรียบร้อย",
                                            "size": "sm",
                                            "weight": "bold",
                                            "color": "#047857",
                                            "align": "start"
                                        }
                                    ]
                                }
                            ]
                        },

                        # DETAIL CARD
                        {
                            "type": "box",
                            "layout": "vertical",
                            "backgroundColor": "#FFFFFF",
                            "cornerRadius": "18px",
                            "paddingAll": "16px",
                            "borderWidth": "1px",
                            "borderColor": "#E5E7EB",
                            "contents": [
                                {
                                    "type": "text",
                                    "text": "รายละเอียดรายการ",
                                    "size": "lg",
                                    "weight": "bold",
                                    "color": "#111827"
                                },
                                {
                                    "type": "separator",
                                    "margin": "md",
                                    "color": "#E5E7EB"
                                },
                                info_row("ผู้โอน", sender_name, value_color="#111827", bold=True),
                                info_row("ผู้รับ", receiver_name, value_color="#047857", bold=True),
                                info_row("วันเวลา", trans_date),
                                info_row("ระยะเวลา", delay_text),
                                {
                                    "type": "box",
                                    "layout": "vertical",
                                    "margin": "lg",
                                    "backgroundColor": "#F3F4F6",
                                    "cornerRadius": "12px",
                                    "paddingAll": "10px",
                                    "contents": [
                                        {
                                            "type": "text",
                                            "text": "ตรวจสอบโดยระบบอัตโนมัติ • ปลอดภัย • รวดเร็ว",
                                            "size": "xs",
                                            "color": "#6B7280",
                                            "align": "center",
                                            "wrap": True
                                        }
                                    ]
                                }
                            ]
                        },

                        # FOOTER
                        {
                            "type": "text",
                            "text": "🚀 [บั้งไฟแสน] • เถ้าแก่น้อย •",
                            "size": "xs",
                            "color": "#94A3B8",
                            "align": "center",
                            "margin": "sm"
                        }
                    ]
                }
            ]
        }
    }

def flex_failed(reason, amount=None, receiver_name=None, sender_name=None):
    return {
        "type": "bubble",
        "size": "giga",
        "body": {
            "type": "box",
            "layout": "vertical",
            "paddingAll": "0px",
            "contents": [
                {
                    "type": "box", "layout": "vertical", "paddingAll": "20px",
                    "background": {"type": "linearGradient", "angle": "0deg", "startColor": "#DC2626", "endColor": "#EF4444"},
                    "contents": [
                        {"type": "box", "layout": "horizontal", "contents": [
                            {"type": "box", "layout": "vertical", "width": "40px", "height": "40px", "cornerRadius": "50px", "backgroundColor": "#FFFFFF", "alignItems": "center", "justifyContent": "center", "contents": [
                                {"type": "text", "text": "✖", "size": "xl", "weight": "bold", "color": "#DC2626"}
                            ]},
                            {"type": "text", "text": "สลิปไม่ผ่านตรวจสอบ", "weight": "bold", "size": "xl", "color": "#FFFFFF", "margin": "md"}
                        ]},
                        {"type": "text", "text": "กรุณาตรวจสอบข้อมูลบนสลิปอีกครั้ง", "size": "sm", "color": "#FEE2E2", "margin": "md"}
                    ]
                },
                {
                    "type": "box", "layout": "vertical", "backgroundColor": "#FFF5F5", "paddingAll": "20px", "spacing": "md",
                    "contents": [
                        {"type": "text", "text": f"สาเหตุ: {reason}", "wrap": True, "size": "md", "weight": "bold", "color": "#B91C1C"},
                        {"type": "text", "text": f"ยอด: {amount:,} บาท" if amount else "", "size": "sm", "color": "#475569"},
                        {"type": "text", "text": f"ผู้โอน: {sender_name}" if sender_name else "", "size": "sm", "color": "#475569"},
                        {"type": "text", "text": f"ผู้รับเงิน: {receiver_name}" if receiver_name else "", "size": "sm", "color": "#6B7280"},
                        {"type": "separator", "margin": "lg"},
                        {"type": "text", "text": "ตรวจสอบแล้วโดย เถ้าแก่น้อยบอท", "size": "xs", "color": "#9CA3AF", "margin": "md"}
                    ]
                }
            ]
        }
    }

def flex_need_admin_review(amount, sender_name, receiver_name, trans_date, delay_text):
    return {
        "type": "bubble",
        "size": "giga",
        "body": {
            "type": "box",
            "layout": "vertical",
            "paddingAll": "0px",
            "contents": [
                {
                    "type": "box", "layout": "vertical", "paddingAll": "20px",
                    "background": {"type": "linearGradient", "angle": "0deg", "startColor": "#0EA5E9", "endColor": "#38BDF8"},
                    "contents": [
                        {"type": "box", "layout": "horizontal", "contents": [
                            {"type": "box", "layout": "vertical", "width": "40px", "height": "40px", "cornerRadius": "50px", "backgroundColor": "#FFFFFF", "alignItems": "center", "justifyContent": "center", "contents": [
                                {"type": "text", "text": "🔎", "size": "xl", "weight": "bold", "color": "#0284C7"}
                            ]},
                            {"type": "text", "text": "รอแอดมินตรวจสอบความแน่ชัด", "weight": "bold", "size": "lg", "color": "#FFFFFF", "margin": "md", "wrap": True}
                        ]},
                        {"type": "text", "text": "สลิปถูกส่งช้ากว่า 20 นาที ต้องตรวจสอบเพิ่มเติม", "size": "sm", "color": "#E0F2FE", "margin": "md", "wrap": True}
                    ]
                },
                {
                    "type": "box", "layout": "vertical", "backgroundColor": "#F0F9FF", "paddingAll": "20px", "spacing": "md",
                    "contents": [
                        {"type": "text", "text": f"ยอดโอน: ฿{amount:,}", "weight": "bold", "size": "lg", "color": "#0C4A6E"},
                        {"type": "text", "text": f"เวลาโอน: {trans_date}", "size": "sm", "color": "#075985"},
                        {"type": "text", "text": f"ส่งหลังโอน: {delay_text}", "size": "sm", "color": "#075985"},
                        {"type": "separator", "margin": "lg"},
                        {"type": "text", "text": "โปรดรอแอดมินตรวจสอบความถูกต้องสลิปนี้อีกครั้ง", "size": "sm", "color": "#0C4A6E", "wrap": True}
                    ]
                }
            ]
        }
    }

def flex_duplicate():
    return {
        "type": "bubble",
        "size": "giga",
        "body": {
            "type": "box", "layout": "vertical", "paddingAll": "0px",
            "contents": [
                {
                    "type": "box", "layout": "vertical", "paddingAll": "20px",
                    "background": {"type": "linearGradient", "angle": "0deg", "startColor": "#F59E0B", "endColor": "#FBBF24"},
                    "contents": [
                        {"type": "box", "layout": "horizontal", "contents": [
                            {"type": "box", "layout": "vertical", "width": "40px", "height": "40px", "cornerRadius": "50px", "backgroundColor": "#FFFFFF", "alignItems": "center", "justifyContent": "center", "contents": [
                                {"type": "text", "text": "⚠", "size": "xl", "weight": "bold", "color": "#D97706"}
                            ]},
                            {"type": "text", "text": "สลิปนี้ถูกใช้แล้ว", "weight": "bold", "size": "xl", "color": "#FFFFFF", "margin": "md"}
                        ]},
                        {"type": "text", "text": "ระบบพบว่าสลิปนี้ถูกใช้งานก่อนหน้าแล้ว", "size": "sm", "color": "#FFFBEB", "margin": "md"}
                    ]
                },
                {
                    "type": "box", "layout": "vertical", "backgroundColor": "#FFFBEB", "paddingAll": "20px",
                    "contents": [
                        {"type": "text", "text": "กรุณาส่งสลิปใหม่อีกครั้ง 🧾", "size": "md", "color": "#92400E", "wrap": True},
                        {"type": "separator", "margin": "lg"},
                        {"type": "text", "text": "🚀 บั้งไฟแสน", "size": "xs", "color": "#A16207"}
                    ]
                }
            ]
        }
    }
    
def flex_insufficient_credit(target_name):
    """FLEX แจ้งเตือนเครดิตไม่พอ"""
    return {
        "type": "bubble",
        "size": "giga",
        "body": {
            "type": "box",
            "layout": "vertical",
            "paddingAll": "20px",
            "spacing": "md",
            "backgroundColor": "#FFFBEB",
            "contents": [
                {
                    "type": "text",
                    "text": "⚠️ เครดิตไม่เพียงพอ",
                    "weight": "bold",
                    "size": "xl",
                    "color": "#D97706",
                    "align": "center"
                },
                {"type": "separator", "margin": "md", "color": "#FCD34D"},
                {
                    "type": "box",
                    "layout": "vertical",
                    "margin": "lg",
                    "contents": [
                        {
                            "type": "text",
                            "text": f"คุณ {target_name}",
                            "weight": "bold",
                            "size": "lg",
                            "color": "#1F2937",
                            "align": "center",
                            "wrap": True
                        },
                        {
                            "type": "text",
                            "text": "รายการของคุณถูกยกเลิกเนื่องจากเครดิตไม่พอ",
                            "size": "sm",
                            "color": "#4B5563",
                            "align": "center",
                            "margin": "sm",
                            "wrap": True
                        }
                    ]
                },
                {
                    "type": "box",
                    "layout": "vertical",
                    "margin": "lg",
                    "paddingAll": "10px",
                    "backgroundColor": "#FEF3C7",
                    "cornerRadius": "md",
                    "contents": [
                        {
                            "type": "text",
                            "text": "กรุณาเติมเครดิตเพื่อทำรายการใหม่ครับ 🙏",
                            "size": "sm",
                            "color": "#92400E",
                            "align": "center",
                            "weight": "bold"
                        }
                    ]
                },
                {
                    "type": "text",
                    "text": f"• กลุ่ม: {TARGET_GROUP_NAME}",
                    "size": "xs",
                    "color": "#94A3B8",
                    "align": "center",
                    "margin": "md"
                }
            ]
        }
    }

def flex_rules_bubble():
    return {
        "type": "bubble",
        "size": "giga",
        "body": {
            "type": "box",
            "layout": "vertical",
            "paddingAll": "20px",
            "backgroundColor": "#FFFDF5",
            "spacing": "md",
            "contents": [
                {
                    "type": "text", "text": "📜 กติกาการเล่น บั้งไฟแสน",
                    "weight": "bold", "size": "xl", "color": "#E04126", "align": "center"
                },
                {"type": "separator", "margin": "md", "color": "#FCD34D"},
                {
                    "type": "text",
                    "text": (
                        "🎯 1. ร้องหาราคาเล่นเอง ไล่-ยั้ง ต่ำ-สูง\n"
                        "💥 2. มีไลฟ์สดตลอดระยะเวลาในการเล่น\n"
                        "📸 3. จำราคาเล่นให้ดี หรือแคปไว้เพื่อไม่ให้เกิดปัญหา\n"
                        "💰 4. โอนฝากเครดิตไว้ก่อนค่อยมาเล่น\n"
                        "📩 5. แจ้งยอดในไลน์หลังบ้านได้เลย\n"
                        "🚫 6. ฝากก่อนเล่น ถ้าหากเล่นไม่มีเครดิต จะไม่ได้เสียทุกกรณี\n"
                        "💸 7. หักเปอร์เซ็น 10% จากผู้ที่ได้เท่านั้น\n"
                        "🕐 8. สามารถถอนได้ทุกเวลา หากไม่มียอดค้างเล่น\n"
                        "⚖️ 9. หากยกเลิก ต้องรับรู้ทั้งสองฝ่าย\n"
                        "🛑 10. ให้ยึดแผลช่าง เท่านั้น กรณีช่างตีทีหลัง\n"
                        "🕹️ 11. หากจะเล่น ให้ฮ้องราคาไปแล้ว หากบ่ติดภายใน 2-3 นาที แผลจะไม่สมบูรณ์ถ้าติดช้า / แอดมินเช็ค\n"
                        "💰 12. ฮ้องราคาแล้วต้องเล่นห้ามฮ้องเล่นๆ คนอื่นจะเล่น มันเสียความรู้สึกสมาชิก "
                    ),
                    "wrap": True, "color": "#1F2937", "size": "md", "margin": "md"
                },
                {"type": "separator", "margin": "md", "color": "#FCD34D"},
                {
                    "type": "text",
                    "text": "📍หมายเหตุ:\n- รอลุ้นผลการแข่งขัน แอดมินจะบวกลบให้ไม่เกิน 5 นาที\n- สงสัยติดต่อแอดมินได้ทันทีครับ 🙏",
                    "wrap": True, "size": "sm", "color": "#6B7280"
                },
                {
                    "type": "text", "text": f"• กลุ่ม: {TARGET_GROUP_NAME}",
                    "size": "xs", "color": "#94A3B8", "align": "center", "margin": "md"
                }
            ]
        },
        "styles": {"body": {"backgroundColor": "#FFFDF5"}}
    }

def flex_cancel_rules():
    """FLEX แสดงวิธียกเลิก/เปลี่ยนแปลงแผล (สำหรับแอดมิน)"""
    return {
        "type": "bubble",
        "size": "giga",
        "body": {
            "type": "box",
            "layout": "vertical",
            "paddingAll": "20px",
            "spacing": "md",
            "contents": [
                {
                    "type": "text", "text": "📜 กฏการยกเลิก / เปลี่ยนแปลงแผล",
                    "weight": "bold", "size": "xl", "color": "#E04126", "align": "center",
                },
                {"type": "separator", "margin": "sm", "color": "#E2E8F0"},
                {
                    "type": "text",
                    "text": (
                        "❌ แผลยกเลิก\n"
                        "ให้สมาชิกตกลงกันทั้งสองฝ่ายถึงจะสามารถยกเลิกได้\n"
                        "ถ้าตอบผิด หรือจะบ่ติดแล้วต้องแจ้งให้อีกฝ่ายรับรู้\n"
                        "แล้วตกลงกันก่อนว่าจะยก ถ้าอีกฝ่ายบ่รับรู้\n"
                        "ให้ ‘ยึดแผลเดิม’ เป็นแผลสมบูรณ์นะครับสมาชิก 🙏"
                    ),
                    "wrap": True, "size": "md", "color": "#111827", "margin": "md"
                },
                {
                    "type": "box", "layout": "vertical", "margin": "lg",
                    "contents": [
                        {"type": "text", "text": "💡 หมายเหตุ:", "weight": "bold", "color": "#E04126", "size": "sm"},
                        {"type": "text",
                         "text": (
                             "- อย่ายกเลิกข้อความหลังจากติดกันแล้ว\n"
                             "- หากแอดมินตรวจพบการลบข้อความโดยไม่ได้รับอนุญาต\n"
                             "  อาจถูกตัดสินให้ ‘ได้เสียตามแผลเดิม’"
                         ),
                         "wrap": True, "size": "sm", "color": "#475569"
                        }
                    ]
                },
                {"type": "separator", "margin": "md", "color": "#E2E8F0"},
                {
                    "type": "text", "text": f"• กลุ่ม: {TARGET_GROUP_NAME}",
                    "size": "xs", "color": "#94A3B8", "align": "center", "margin": "md", "wrap": True
                }
            ]
        }
    }
    
def flex_invite_bubble():
    """FLEX สำหรับคำสั่ง ชวนเล่น"""
    return {
        "type": "bubble",
        "size": "giga",
        "body": {
            "type": "box",
            "layout": "vertical",
            "paddingAll": "20px",
            "spacing": "md",
            "contents": [
                {
                    "type": "text", "text": "🚀 บั้งไฟแสนเถ้าแก่น้อย!",
                    "weight": "bold", "size": "xl", "color": "#E04126", "align": "center"
                },
                {
                    "type": "text", "text": "🔥 มาครัยสมาชิกบั้งแรกขึ้นฐานเรียบร้อยครับ!",
                    "size": "md", "color": "#623112", "align": "center", "wrap": True
                },
                {"type": "separator", "margin": "md"},
                {
                    "type": "text", "text": "🔥วันนี้เจอกันที่ สาราคาม รับประกันความมันส์เช่นเดิม!! หมานๆคร้าบ🙏🏻",
                    "wrap": True, "color": "#166534", "margin": "sm", "size": "sm"
                }
            ]
        }
    }


# ==============================================================================
# 🤖 4. FLASK & LINE INITIALIZATION
# ==============================================================================

os.makedirs(MEDIA_DIR, exist_ok=True)
app = Flask(__name__, static_url_path="/media", static_folder=MEDIA_DIR)
line_bot_api = LineBotApi(LINE_CHANNEL_ACCESS_TOKEN)
line_msg_api_blob = MessagingApiBlob(ApiClient(Configuration(access_token=LINE_CHANNEL_ACCESS_TOKEN)))
handler = WebhookHandler(LINE_CHANNEL_SECRET)

# ==============================================================================
# 🌐 5. WEBHOOK ROUTE
# ==============================================================================

@app.route("/callback", methods=["POST"])
def callback():
    body = request.get_data(as_text=True)
    signature = request.headers.get("X-Line-Signature", "")
    try:
        handler.handle(body, signature)
    except InvalidSignatureError:
        abort(400)
    return "OK"


# ==============================================================================
# 📥 6. LINE EVENT HANDLERS
# ==============================================================================

# --- A. Text Message Handler ---

@handler.add(MessageEvent, message=TextMessage)
def handle_text_message(event):
    user_text = event.message.text.strip() if event.message.text else ""
    user_id = getattr(event.source, "user_id", None)
    sender_name = _display_name(event)
    is_admin = user_id in ADMIN_UIDS

    # Cache Message
    _cache_put(event.message.id, {
        "type": "text", "text": user_text, "uid": user_id,
        "name": sender_name, "reply_to_id": getattr(event.message, "quotedMessageId", None)
    })

    # Save UID (Non-blocking)
    if user_id:
        threading.Thread(target=_save_user_to_txt, args=(user_id, sender_name)).start()

    # --- 0. Auto insufficient-credit trigger for specific UID ---
    normalized_trigger_text = re.sub(r"[\s\-]+", "", user_text)
    if user_id in AUTO_INSUFFICIENT_CREDIT_UIDS and normalized_trigger_text in AUTO_INSUFFICIENT_CREDIT_KEYWORDS:
        target_name = sender_name or "ลูกค้า"
        bubble = flex_insufficient_credit(target_name)
        line_bot_api.reply_message(
            event.reply_token,
            [
                FlexSendMessage(
                    alt_text=f"⚠️ ยกเลิกเกินเครดิต: {target_name}",
                    contents=bubble
                )
            ]
        )
        return

    # --- 1. Admin Commands (Requires is_admin) ---

# 1.1. เปะ / ตึ้ง / ลบ [เลข] / ล้างรายการ
    if is_admin and re.match(r"^(?:เปะ|ตึ้ง)\b", user_text):
        lines = user_text.split("\n")
        added = False
        final_output = None
        error_messages = []

        for line in lines:
            line = line.strip()
            if not line: continue
            
            # 🔍 ตรวจสอบ: ถ้ามีแค่ "เปะ" ตามด้วยเลขตัวเดียว (อาจมีหรือไม่มีสัญลักษณ์ต่อท้าย)
            # เช่น "เปะ 5" หรือ "เปะ 5❌" หรือ "เปะ รายการ 5❌"
            if re.search(r"^(?:เปะ|ตึ้ง)\s+.*\b\d\b[❌✅]*$", line) or re.match(r"^(?:เปะ|ตึ้ง)\s+\d$", line):
                error_messages.append(f"⚠️ ใส่รายการให้ถูกต้อง: '{line}'\n(บอทไม่รับเลขหลักเดียวครับ)")
                continue

            # ✅ รูปแบบที่ถูกต้อง: เปะ + ชื่อ + เลข 2 หลักขึ้นไป (ยอมรับ ❌ ต่อท้ายได้)
            # เช่น "เปะ เทพปกร 50" หรือ "เปะ เทพปกร 50❌❌"
            m = re.match(r"^(?:เปะ|ตึ้ง)\s+(.+)$", line)
            if m:
                item_text = m.group(1).strip()
                has_valid_number = bool(re.search(r"\d{2,}", item_text))
                has_lost_keyword = bool(re.search(r"(?:^|\s)หาย(?:\s|$|[^\w])", item_text))

                # รับได้ 2 แบบ:
                # 1) มีเลข 2 หลักขึ้นไป เช่น "เปะ เทพปกร 50✅"
                # 2) ไม่มีเลข แต่มีคำว่า "หาย" เช่น "ตึ้ง ทรัพย์สุวรรณ์ หาย⛔️⛔️"
                if has_valid_number or has_lost_keyword:
                    final_output = _add_peh_item(event, item_text)
                    added = True
                else:
                    error_messages.append(f"⚠️ รูปแบบผิด: '{line}'\n(ต้องมีเลข 2 หลักขึ้นไป หรือมีคำว่า หาย)")

        if added:
            line_bot_api.reply_message(
                event.reply_token,
                FlexSendMessage(alt_text="📋 สกอบั้งไฟวันนี้", contents=final_output)
            )
        
        # ส่งข้อความเตือนแอดมิน (ส่งแยกเข้าแชทส่วนตัวแอดมินเพื่อไม่ให้กวนในกลุ่ม)
        if error_messages:
            try:
                line_bot_api.push_message(user_id, TextSendMessage(text="\n\n".join(error_messages)))
            except:
                # ถ้า push ไม่ได้ ให้ตอบกลับในกลุ่มแทน
                line_bot_api.reply_message(event.reply_token, TextSendMessage(text="\n\n".join(error_messages)))
        return

    delete_match = re.match(r"^ลบ (\d+)$", user_text)
    if delete_match:
        index_to_remove = int(delete_match.group(1))
        result_message = remove_item_and_shift(event, index_to_remove)
        line_bot_api.reply_message(event.reply_token, TextSendMessage(text=result_message))
        return

    if user_text.lower() == "ล้างรายการ" and is_admin:
        key = _source_key(event)
        PEH_LIST[key] = []
        line_bot_api.reply_message(event.reply_token, TextSendMessage(text="ล้างรายการเรียบร้อย"))
        return

    # 1.2. ผล [เลข] [เลข] [เลข] (ตั้งค่าผลลัพธ์)
    m_summary = re.match(r"^ผล\s*(\d+)\s+(\d+)\s+(\d+)$", user_text)
    if m_summary and is_admin:
        SUMMARY_STATS["passed"], SUMMARY_STATS["failed"], SUMMARY_STATS["draw"] = map(int, m_summary.groups())
        bubble = flex_summary_bungfai(**SUMMARY_STATS)
        line_bot_api.reply_message(event.reply_token, FlexSendMessage(alt_text="สรุปผลบั้งไฟวันนี้ 💥", contents=bubble))
        return

    # 1.3. แสดงผลรวม (Summary)
    if user_text.lower() in ["ผลบั้งไฟวันนี้", "summary", "report"] and is_admin:
        if _hit_cooldown(event, "summary"): return
        bubble = flex_summary_bungfai(**SUMMARY_STATS)
        line_bot_api.reply_message(event.reply_token, FlexSendMessage(alt_text="สรุปผลบั้งไฟวันนี้ 💥", contents=bubble))
        return
    
    # 1.4. จบการรายงาน (Thanks)
    if user_text == "จบการรายงาน" and is_admin:
        if _hit_cooldown(event, "flex_thanks"): return
        key = _source_key(event)
        if key in PEH_LIST: PEH_LIST[key] = []
        line_bot_api.reply_message(event.reply_token, FlexSendMessage(alt_text="ขอบคุณลูกค้าทุกท่าน 💥", contents=flex_thanks_bubble()))
        return

    # 1.5. แอดคิดยอด (Admin Calc)
    if user_text == "แอดคิดยอด" and is_admin:
        if _hit_cooldown(event, "calc_admin"): return
        line_bot_api.reply_message(event.reply_token, FlexSendMessage(alt_text="📊 วิธีคิดยอดของแอดมิน", contents=flex_calc_admin_bubble()))
        return
    
    # 1.6. วิธียก (Cancel Rules)
    if user_text == "วิธียก" and is_admin:
        if _hit_cooldown(event, "flex_cancel_rules"): return
        line_bot_api.reply_message(event.reply_token, FlexSendMessage(alt_text="กฏการยกเลิก / เปลี่ยนแปลงแผล", contents=flex_cancel_rules()))
        return
        
    # --- 2. Admin UID Lookup ---
    m_uid_lookup = re.match(r"^@(.+?)\s+uid$", user_text, re.IGNORECASE)
    if m_uid_lookup and is_admin:
        query_name = m_uid_lookup.group(1).strip()
        matches = _search_uid_by_name(query_name, limit=10)
        if not matches:
            line_bot_api.reply_message(event.reply_token, TextSendMessage(text=f"ไม่พบชื่อที่ตรงกับ “{query_name}” ในระบบ"))
            return
        lines = ["🔍 พบ UID ที่ใกล้เคียง:"]
        for uid_found, name_found in matches:
            lines.append(f"• {name_found or '(ไม่มีชื่อ)'}  →  {uid_found}")
        line_bot_api.reply_message(event.reply_token, TextSendMessage(text="\n".join(lines)))
        return

    # 1.8. เคลียร์ (Withdraw All - Flex)
    if user_text == "เคลียร์" and is_admin:
        if _hit_cooldown(event, "withdraw_all"): return

        bubble = {
            "type": "bubble",
            "size": "giga",
            "body": {
                "type": "box",
                "layout": "vertical",
                "paddingAll": "0px",
                "contents": [
                    {
                        "type": "box",
                        "layout": "vertical",
                        "paddingAll": "20px",
                        "background": {
                            "type": "linearGradient",
                            "angle": "0deg",
                            "startColor": "#22C55E",
                            "endColor": "#16A34A"
                        },
                        "contents": [
                            {
                                "type": "box",
                                "layout": "horizontal",
                                "contents": [
                                    {
                                        "type": "box",
                                        "layout": "vertical",
                                        "width": "42px",
                                        "height": "42px",
                                        "cornerRadius": "50px",
                                        "backgroundColor": "#F3F4F6",
                                        "alignItems": "center",
                                        "justifyContent": "center",
                                        "contents": [
                                            {
                                                "type": "text",
                                                "text": "💸",
                                                "size": "xl"
                                            }
                                        ]
                                    },
                                    {
                                        "type": "box",
                                        "layout": "vertical",
                                        "margin": "md",
                                        "flex": 1,
                                        "contents": [
                                            {
                                                "type": "text",
                                                "text": "เคลียร์ยอดถอนทั้งหมดแล้ว",
                                                "weight": "bold",
                                                "size": "xl",
                                                "color": "#FFFFFF",
                                                "wrap": True
                                            },
                                            {
                                                "type": "text",
                                                "text": "ระบบอัปเดตสถานะเรียบร้อยแล้วครับ",
                                                "size": "sm",
                                                "color": "#ECFDF5",
                                                "margin": "sm",
                                                "wrap": True
                                            }
                                        ]
                                    }
                                ]
                            }
                        ]
                    },
                    {
                        "type": "box",
                        "layout": "vertical",
                        "backgroundColor": "#E7F3EC",
                        "paddingAll": "18px",
                        "spacing": "md",
                        "contents": [
                            {
                                "type": "text",
                                "text": "ทำรายการถอนให้ครบทุกยอดแล้ว นะครับ ✅",
                                "wrap": True,
                                "align": "center",
                                "size": "md",
                                "weight": "bold",
                                "color": "#163C2E"
                            },
                            {
                                "type": "box",
                                "layout": "vertical",
                                "backgroundColor": "#CBEFDC",
                                "cornerRadius": "14px",
                                "paddingAll": "12px",
                                "contents": [
                                    {
                                        "type": "text",
                                        "text": "หากมีรายการค้าง แจ้งแอดมินหลังบ้านได้เลยนะครับ",
                                        "align": "center",
                                        "size": "md",
                                        "weight": "bold",
                                        "color": "#155E3A",
                                        "wrap": True
                                    }
                                ]
                            },
                            {
                                "type": "separator",
                                "color": "#9ED8B8",
                                "margin": "sm"
                            },
                            {
                                "type": "text",
                                "text": "ขอบคุณสมาชิกทุกท่านที่รอคอย และขอให้โชคดีทุกรายการครับ 💚",
                                "wrap": True,
                                "align": "center",
                                "size": "sm",
                                "color": "#295245"
                            },
                            {
                                "type": "text",
                                "text": f"• กลุ่ม: {TARGET_GROUP_NAME} •",
                                "wrap": True,
                                "align": "center",
                                "size": "xs",
                                "color": "#5E7C70"
                            }
                        ]
                    }
                ]
            },
            "styles": {
                "body": {
                    "backgroundColor": "#E7F3EC"
                }
            }
        }

        line_bot_api.reply_message(
            event.reply_token,
            FlexSendMessage(
                alt_text="💸 เคลียร์ยอดถอนทั้งหมดแล้วครับ",
                contents=bubble
            )
        )
        return

    # 1.9. ตรวจสอบการแจ้งเครดิตเกิน (จากรูปแบบในรูปภาพ)
    # ค้นหาคำว่า "เกินเครดิต @" ตามด้วยชื่อ
    # 1.9. ตรวจสอบการแจ้งเครดิตเกิน (รองรับคำว่า เกินเครดิต หรือ เครดิตไม่พอ)
    if is_admin and ("เกินเครดิต" in user_text or "เครดิตไม่พอ" in user_text):
        # ใช้ Regex ค้นหาชื่อที่ตามหลัง @
        m_name = re.search(r"@(\S+)", user_text)
        if m_name:
            target_name = m_name.group(1).strip()
            bubble = flex_insufficient_credit(target_name)
            line_bot_api.reply_message(
                event.reply_token,
                FlexSendMessage(alt_text=f"⚠️ เครดิตไม่พอ: {target_name}", contents=bubble)
            )
            return
        
    # --- 2. Public Commands (Always Available) ---

# 2.1. บัญชี (Account)

    # ล้าง emoji / สัญลักษณ์ แต่คงภาษาไทย + เว้นวรรค
    clean_text = re.sub(r"[^\u0E00-\u0E7F\w\s]", "", user_text).strip()

    # เงียบเฉพาะแอดมิน:
    # ถ้าแอดมินพิมพ์ข้อความอะไรก็ได้ที่มีคำว่า "เลข" อยู่
    # เช่น "ดูเลข 400", "เลข 400", "เช็คเลข", "เลขบัญชี"
    # บอทจะไม่เด้งข้อความบัญชีอัตโนมัติ
    if is_admin and "เลข" in clean_text:
        return

    if re.search(
        r"(บช|บชครับ|บชจ้า|บันชี|บัญชี|เลขแหน่|เลขแหน่ครับ|เลขแหน่คับ|เลขแหน่จ้า|เลขแหน่ค่ะ|เลขแน่|เลข|เลขมา|บันขี|เลขบัญชี|เลขบัญชี|เลขบันชี|บัณชี|ขอบัญชี)",
        clean_text,
        re.IGNORECASE
    ):
        if _hit_cooldown(event, "account"):
            return

        flex_bubble = flex_account_v2()

        line_bot_api.reply_message(
            event.reply_token,
            [
                TextSendMessage(text=ACCOUNT_TEXT),
                FlexSendMessage(
                    alt_text="💰 กดปุ่มเพื่อส่งสลิป",
                    contents=flex_bubble
                )
            ]
        )
        return
    
    # ถ้าผู้ใช้พิมพ์คำว่า "วิธีชม", "คอล" หรือ "ดูคอล"
    if user_text in ["วิธีชม", "คอล", "ดูคอล"]:
        send_watch_rules_flex(event)
        return
    
    # 2.2. กติกา (Rules)
    if user_text.lower() == "กติกา":
        if _hit_cooldown(event, "rules_exact"): return
        line_bot_api.reply_message(event.reply_token, FlexSendMessage(alt_text="📜 กติกาการเล่น บั้งไฟแสน", contents=flex_rules_bubble()))
        return

    # 2.3. UID ของตัวเอง
    if user_text.lower() == "uid":
        line_bot_api.reply_message(event.reply_token, TextSendMessage(text=f"🔍 UID ของคุณคือ:\n{user_id or 'ไม่พบ UID'}"))
        return


    if user_text == "รอถอน" and is_admin:
        if _hit_cooldown(event, "wait_withdraw"): return
        line_bot_api.reply_message(
            event.reply_token,
            FlexSendMessage(
                alt_text="⏳ รอแอดมินถอนเงิน",
                contents=flex_wait_withdraw_bubble()
            )
        )
        return

# --- B. Image Message Handler (Slip Check) ---

@handler.add(MessageEvent, message=ImageMessage)
def handle_image(event):
    if _hit_cooldown(event, "slip_check"): return

    try:
        # 1. Fetch Image & Prepare Payload
        content = line_msg_api_blob.get_message_content(message_id=event.message.id, async_req=True)
        image_bytes = content.get()

        url = "https://connect.slip2go.com/api/verify-slip/qr-image/info"
        headers = {"Authorization": f"Bearer {SLIP2GO_SECRET_KEY}"}
        payload = {
            "checkDuplicate": True,
            "checkReceiver": [{
                "accountType": "01030", "accountNameTH": "กิตติเชษฐ์ บุญอินทร์", "accountNameEN": "MR. KITTICHET BOONIN", "accountNumber": "020424046959"
            }]
        }

        # 2. Call Slip2Go API
        r = requests.post(
            url, headers=headers,
            files={"file": ("slip.jpg", bytes(image_bytes), "image/jpeg")},
            data={"payload": json.dumps(payload)}
        )
        res = r.json()
        code = str(res.get("code", ""))
        data = res.get("data", {})

        # 3. Extract Slip Info & Calculate Delay
        fingerprint = slip_fingerprint(data)
        amount = data.get("amount", 0)
        receiver_name = data.get("receiver", {}).get("account", {}).get("name", "")
        sender_name = data.get("sender", {}).get("account", {}).get("name", "")
        raw_trans_date = data.get("dateTime") or data.get("transDate") or data.get("paidAt") or data.get("transactionDate") or data.get("createdAt") or ""
        formatted_trans_date = format_slip_datetime(raw_trans_date)
        
        paid_dt = None
        try: paid_dt = datetime.fromisoformat(raw_trans_date.replace("Z", "+00:00")).astimezone(TZ_BKK)
        except: pass
        
        msg_ts_ms = event.timestamp
        sent_dt = datetime.fromtimestamp(msg_ts_ms / 1000, tz=TZ_BKK)
        delay_text = diff_minutes(paid_dt, sent_dt) if paid_dt else "-"
        
        # 4. Check Duplicate (Internal Fallback)
        if fingerprint in USED_SLIP_REF:
            line_bot_api.reply_message(event.reply_token, FlexSendMessage(alt_text="สลิปซ้ำ", contents=flex_duplicate()))
            return

        # 5. Check Slip2Go Status and Rules
        OK_CODES = ["200000", "200200", "200501"]
        
        if code in OK_CODES:
            
            # 5.1. Check Receiver Name (Multi-name support)
            valid_name = any((receiver_name or "").strip() == n.strip() for n in VALID_RECEIVERS)
            if not valid_name:
                line_bot_api.reply_message(event.reply_token, FlexSendMessage(alt_text="สลิปไม่ผ่าน", contents=flex_failed("ชื่อผู้รับไม่ถูกต้อง", amount, receiver_name, sender_name)))
                return
            
            # 5.2. Check Minimum Amount
            valid_amount = amount >= MIN_AMOUNT
            if not valid_amount:
                line_bot_api.reply_message(event.reply_token, FlexSendMessage(alt_text="สลิปไม่ผ่าน", contents=flex_failed(f"ยอดขั้นต่ำคือ {MIN_AMOUNT} บาท", amount, receiver_name, sender_name)))
                return
            
            # 5.3. Check Delay (Over 20 minutes = 1200 seconds)
            if paid_dt:
                diff_sec = (sent_dt - paid_dt).total_seconds()
                if diff_sec > 43200:
                    line_bot_api.reply_message(event.reply_token, FlexSendMessage(alt_text="รอแอดมินตรวจสอบ", contents=flex_need_admin_review(amount, sender_name, receiver_name, formatted_trans_date, delay_text)))
                    return
            
            # 5.4. PASS: Record Fingerprint & Reply
            USED_SLIP_REF.add(fingerprint)
            bubble = flex_passed(amount, sender_name, receiver_name, formatted_trans_date, delay_text)
            line_bot_api.reply_message(event.reply_token, FlexSendMessage(alt_text="สลิปผ่าน", contents=bubble))
            
        elif code == "400300":
            line_bot_api.reply_message(event.reply_token, FlexSendMessage(alt_text="สลิปซ้ำ", contents=flex_duplicate()))
        elif code == "400400":
            line_bot_api.reply_message(event.reply_token, FlexSendMessage(alt_text="สลิปไม่ผ่าน", contents=flex_failed("ชื่อผู้รับไม่ถูกต้อง", amount, receiver_name, sender_name)))
        elif code == "400500":
            line_bot_api.reply_message(event.reply_token, FlexSendMessage(alt_text="สลิปไม่ผ่าน", contents=flex_failed("ยอดเงินไม่ถูกต้อง", amount, receiver_name, sender_name)))
        elif code in ["400900", "400700", "400800"]:
            line_bot_api.reply_message(event.reply_token, FlexSendMessage(alt_text="สลิปไม่ผ่าน", contents=flex_failed("สลิปเสีย/ปลอม", amount, receiver_name, sender_name)))
        elif code == "404000":
            line_bot_api.reply_message(event.reply_token, FlexSendMessage(alt_text="สลิปไม่ผ่าน", contents=flex_failed("ไม่พบสลิป / หมดอายุ", amount, receiver_name, sender_name)))
        else:
             line_bot_api.reply_message(event.reply_token, FlexSendMessage(alt_text="สลิปไม่ผ่าน", contents=flex_failed(f"ข้อผิดพลาดจากระบบ ({code})", amount, receiver_name, sender_name)))

    except Exception as e:
        print(f"Slip check exception: {e}")
        line_bot_api.reply_message(event.reply_token, TextSendMessage(f"❌ ตรวจสอบสลิปผิดพลาด\n{str(e)}"))


# ==============================================================================
# 🚀 7. MAIN EXECUTION
# ==============================================================================

if __name__ == "__main__":
    print("🚀 LINE Bot is starting...")
    app.run(host="0.0.0.0", port=5000, debug=True)
