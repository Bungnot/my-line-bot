import os, re, sqlite3, traceback, json, threading, time
from contextlib import closing
from datetime import datetime, timezone, timedelta
from urllib.parse import parse_qs

from flask import Flask, request, abort, jsonify

# LINE SDK
from linebot import LineBotApi, WebhookHandler
from linebot.exceptions import InvalidSignatureError
from linebot.models import (
    MessageEvent, TextMessage, TextSendMessage,
    PostbackEvent, FollowEvent,
    FlexSendMessage,
    PostbackAction
)

app = Flask(__name__)

# -------------------- LINE Config --------------------
# ใช้ ENV ก่อนเสมอ เพื่อให้หมุน secret/token ได้โดยไม่ต้องแก้โค้ด
DEFAULT_LINE_CHANNEL_SECRET = "d3adf70433cfdcfb9d7ada1fadcce377"
DEFAULT_LINE_CHANNEL_ACCESS_TOKEN = "yJ9iETHEXV69HxwTs3FINt14/Oz/+pW5aio8Hf+MIMo6+lOPZ+qsD1NjbhS/u/qst9e2UqmYskLOixXKg2qaqMNAIastgvza7RfaTgiAa+KpGlrRbHe1DyB/BXQ/JX+jCL4DFKHz0gFyJ5zwj0vCfQdB04t89/1O/w1cDnyilFU="
LINE_CHANNEL_SECRET = (os.environ.get("LINE_CHANNEL_SECRET") or DEFAULT_LINE_CHANNEL_SECRET).strip()
LINE_CHANNEL_ACCESS_TOKEN = (os.environ.get("LINE_CHANNEL_ACCESS_TOKEN") or DEFAULT_LINE_CHANNEL_ACCESS_TOKEN).strip()
line_bot_api = LineBotApi(LINE_CHANNEL_ACCESS_TOKEN) if LINE_CHANNEL_ACCESS_TOKEN else None
handler = WebhookHandler(LINE_CHANNEL_SECRET) if LINE_CHANNEL_SECRET else None

# -------------------- Admin Seeds (Backend only) --------------------
DEFAULT_ADMIN_UIDS = [
    "U255dd67c1fef32fb0eae127149c7cadc",
]
ENV_ADMIN_UIDS = [u.strip() for u in os.environ.get("ADMIN_UIDS", "").split(",") if u.strip()]
ADMIN_UIDS = list(dict.fromkeys(DEFAULT_ADMIN_UIDS + ENV_ADMIN_UIDS))

# -------------------- Static Messages --------------------
RULES_TEXT = """📌 กติกาการเล่น (ติดได้หลายคน/หลายแผล)
1) เปิดให้เล่น: เปิด <ชื่อค่าย>  เช่น เปิด แอดเทวดา
2) โพสเล่น: ชล500 / ถ500 / +5ถ500 / -3ชล200 / 300-320ล1000
   - 'ชล/ล/ไล่' = ฝั่งสูง, 'ถ/ชถ/ยั้ง/ถอย' = ฝั่งต่ำ
   - +N/-N คือแผลเพิ่ม/ลดจากราคาช่าง (คำนวณหลังแอดมินตีราคา)
   - ถ้ามีตัวเลขในโพสเอง เช่น 300-320ล1000 จะถือเป็น "แผลตัวเลข" และใช้ช่วงนั้นคิดผลโดยตรง
3) การติด (ต้องตอบกลับ/Quote เท่านั้น)
   - คนรับ: ตอบกลับที่โพสเล่น แล้วพิมพ์ 'ติด' / 'ต'
   - ถ้าจะติดไม่เต็มจำนวน ให้พิมพ์ เช่น 'ต300' / 'ติด300'
   - คนโพส: ตอบกลับที่ข้อความ 'ติด' ของคนรับ แล้วพิมพ์ 'ติด' เพื่อยืนยัน
   - ถ้าคนโพสอยากลดไม้ เช่น โพส 500 แต่จะรับแค่ 300 ให้ตอบ 'ติด300' ที่ข้อความคนรับ แล้วให้คู่เล่นตอบ 'ติด' ซ้ำอีกครั้งเพื่อยืนยันยอดใหม่
   - 1 โพสเล่น ติดได้หลายคนได้เรื่อย ๆ และคนเดิมติดซ้ำได้ หากเครดิตพอ
4) ปิดให้เล่น: ปิด
5) แอดมินตีราคาช่าง
   - ราคาช่าง <ตัวเลข>  เช่น ราคาช่าง 210
   - ราคาช่าง <ช่วง>    เช่น ราคาช่าง 330-360
   - ราคาช่าง ไม่ตี     = รอบนี้จะไม่มีราคาช่างฐาน
6) สรุปผลรอบ
   - รอบปกติ: แจ้งผล <ค่า> เช่น แจ้งผล 370
   - รอบช่างไม่ตี: แจ้งผล ชตย <ค่า> หรือ แจ้งผล ช่างไม่ตี <ค่า> เช่น แจ้งผล ชตย 330
   - เมื่อพิมพ์แจ้งผล ระบบจะให้ส่งคำสั่งเดิมซ้ำอีกครั้งเพื่อยืนยันภายใน 5 นาที
   - หรือ แจ้งผล <ข้อความ> เพื่อ "ไม่คิดยอดทั้งรอบ" เช่น แจ้งผล บั้งไฟหาย
   - ถ้ารอบเป็น "ราคาช่าง ไม่ตี" จะคิดเฉพาะบิลที่มีช่วงตัวเลขในบิลเอง เช่น 300-320ล1000
   - บิลที่อิงราคาช่างฐาน เช่น ชล1000 / ชถ1000 / +5ล1000 / -3ถ1000 จะไม่คิดยอดในรอบช่างไม่ตี
   - (ยังรองรับคำสั่งเดิม: รส/รต/สรุปสูง/สรุปต่ำ/ออกสูง/ออกต่ำ)
7) เครดิตต้องพอทั้งสองฝั่งก่อนยืนยัน (ระบบกันเครดิตเมื่อยืนยัน)
8) เสมอ: คืนเครดิต / ชนะคิดค่าธรรมเนียม 10% จากกำไร
9) สรุปยอดรอบพิมพ์: สรุปยอด / สรุปรอบ / ยอด
🔒 บิลยืนยันสำเร็จจะส่งเข้าหลังบ้าน (DM) ไม่ประกาศหน้ากลุ่ม
"""

BANK_TEXT = (
    "🏦 ข้อมูลโอนชำระ\n"
    "ธนาคาร: ไทยพาณิชย์ (SCB)\n"
    "ชื่อบัญชี: สมหมาย จันทอง\n"
    "เลขที่บัญชี: 123-456-789\n\n"
    "📎 ฝากแล้วแนบสลิปแจ้งแอดมินหลังบ้านได้เลยครับ"
)

# -------------------- DB --------------------
DB = "bankhai_bot.db"
_db_local = threading.local()

class _DbProxy:
    def __init__(self, con):
        self._con = con
    def __getattr__(self, name):
        return getattr(self._con, name)
    def close(self):
        # ให้ with closing(db()) ใช้ซ้ำ connection เดิมได้ภายใน request
        return None

def _configure_db_connection(con):
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA busy_timeout=5000;")
    con.execute("PRAGMA synchronous=NORMAL;")
    con.execute("PRAGMA foreign_keys=ON;")
    con.execute("PRAGMA temp_store=MEMORY;")
    con.execute("PRAGMA cache_size=-32000;")
    return con

def _new_db_connection():
    return _configure_db_connection(
        sqlite3.connect(DB, timeout=30, isolation_level=None, check_same_thread=False)
    )

def db():
    con = getattr(_db_local, "con", None)
    if con is None:
        con = _new_db_connection()
        _db_local.con = con
    return _DbProxy(con)

def close_db_connection(*_args, **_kwargs):
    con = getattr(_db_local, "con", None)
    if con is not None:
        try:
            con.close()
        except Exception:
            pass
        _db_local.con = None

app.teardown_appcontext(close_db_connection)

def init_db():
    with closing(_new_db_connection()) as con, con:
        # users + index
        con.execute("""
        CREATE TABLE IF NOT EXISTS users(
            user_id TEXT PRIMARY KEY,
            user_name TEXT,
            credit INTEGER DEFAULT 0,
            reserved INTEGER DEFAULT 0
        )""")
        con.execute("""
        CREATE TABLE IF NOT EXISTS user_index(
            user_id TEXT PRIMARY KEY,
            idx INTEGER UNIQUE
        )""")
        # admins
        con.execute("""
        CREATE TABLE IF NOT EXISTS admins(
            admin_id TEXT PRIMARY KEY
        )""")
        # rounds/bets/messages
        con.execute("""
        CREATE TABLE IF NOT EXISTS rounds(
            thread_root_id TEXT PRIMARY KEY,
            context_id TEXT,
            admin_id TEXT, admin_name TEXT,
            label TEXT, base_low INTEGER, base_high INTEGER,
            base_mode TEXT DEFAULT 'pending',
            status TEXT, opened_at TEXT, closed_at TEXT,
            result_value INTEGER, result_side TEXT
        )""")
        con.execute("""
        CREATE TABLE IF NOT EXISTS bets(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            context_id TEXT,
            thread_root_id TEXT,
            poster_id TEXT, poster_name TEXT,
            opponent_id TEXT, opponent_name TEXT,
            amount INTEGER, want_side TEXT,
            price_low INTEGER, price_high INTEGER,
            play_flag TEXT,
            status TEXT, created_at TEXT, settled_at TEXT,
            result TEXT, winner_id TEXT, loser_id TEXT, fee INTEGER DEFAULT 0,
            open_message_id TEXT,
            accept_message_id TEXT,
            delta INTEGER DEFAULT 0,
            cancel_status TEXT,
            cancel_requester_id TEXT,
            cancel_requested_at TEXT,
            cancel_responder_id TEXT,
            cancel_resolved_at TEXT
        )""")
        con.execute("""
        CREATE TABLE IF NOT EXISTS messages(
            message_id TEXT PRIMARY KEY,
            context_id TEXT,
            thread_root_id TEXT,
            user_id TEXT, user_name TEXT,
            text TEXT, created_at TEXT
        )""")
        con.execute("""
        CREATE TABLE IF NOT EXISTS result_confirms(
            context_id TEXT NOT NULL,
            thread_root_id TEXT NOT NULL,
            admin_id TEXT NOT NULL,
            command_key TEXT NOT NULL,
            mode TEXT NOT NULL,
            result_value INTEGER,
            reason_text TEXT,
            created_at TEXT NOT NULL,
            expires_at TEXT NOT NULL,
            PRIMARY KEY(context_id, thread_root_id)
        )""")
        # safe migrations
        for stmt in [
            "ALTER TABLE rounds ADD COLUMN context_id TEXT",
            "ALTER TABLE rounds ADD COLUMN base_mode TEXT DEFAULT 'pending'",
            "ALTER TABLE bets ADD COLUMN context_id TEXT",
            "ALTER TABLE messages ADD COLUMN context_id TEXT",
            "ALTER TABLE bets ADD COLUMN open_message_id TEXT",
            "ALTER TABLE bets ADD COLUMN accept_message_id TEXT",
            "ALTER TABLE bets ADD COLUMN play_flag TEXT",
            "ALTER TABLE bets ADD COLUMN delta INTEGER DEFAULT 0",
            "ALTER TABLE bets ADD COLUMN cancel_status TEXT",
            "ALTER TABLE bets ADD COLUMN cancel_requester_id TEXT",
            "ALTER TABLE bets ADD COLUMN cancel_requested_at TEXT",
            "ALTER TABLE bets ADD COLUMN cancel_responder_id TEXT",
            "ALTER TABLE bets ADD COLUMN cancel_resolved_at TEXT",
            "ALTER TABLE users ADD COLUMN reserved INTEGER DEFAULT 0",
        ]:
            try: con.execute(stmt)
            except Exception: pass

        # indexes for hot paths
        for stmt in [
            "CREATE INDEX IF NOT EXISTS idx_rounds_context_status_opened ON rounds(context_id, status, opened_at DESC)",
            "CREATE INDEX IF NOT EXISTS idx_rounds_context_status_closed ON rounds(context_id, status, closed_at DESC)",
            "CREATE INDEX IF NOT EXISTS idx_bets_context_thread_status ON bets(context_id, thread_root_id, status)",
            "CREATE INDEX IF NOT EXISTS idx_bets_context_accept_msg ON bets(context_id, accept_message_id)",
            "CREATE INDEX IF NOT EXISTS idx_bets_context_open_msg ON bets(context_id, open_message_id)",
            "CREATE INDEX IF NOT EXISTS idx_bets_context_thread_created ON bets(context_id, thread_root_id, created_at)",
            "CREATE INDEX IF NOT EXISTS idx_bets_context_poster_status_created ON bets(context_id, poster_id, status, created_at DESC)",
            "CREATE INDEX IF NOT EXISTS idx_messages_context_thread_created ON messages(context_id, thread_root_id, created_at)",
            "CREATE INDEX IF NOT EXISTS idx_users_name ON users(user_name)",
        ]:
            try: con.execute(stmt)
            except Exception: pass

        # seed admins from backend only
        if ADMIN_UIDS:
            for uid in ADMIN_UIDS:
                try:
                    con.execute("INSERT OR IGNORE INTO admins(admin_id) VALUES(?)", (uid,))
                except Exception:
                    pass

# -------------------- Time helpers --------------------
TIME_FMT = "%Y-%m-%d %H:%M:%S"
RESULT_CONFIRM_TIMEOUT_MINUTES = 5

def now_utc_str() -> str:
    return datetime.now(timezone.utc).strftime(TIME_FMT)
def parse_utc(s: str) -> datetime:
    return datetime.strptime(s, TIME_FMT)

def normalize_command_key(text: str) -> str:
    t = (text or "").strip().translate(str.maketrans("๐๑๒๓๔๕๖๗๘๙", "0123456789"))
    return re.sub(r"\s+", " ", t)

def format_price_value(low, high) -> str:
    if low is None or high is None:
        return "รอช่างตีราคา"
    low = int(low)
    high = int(high)
    return str(low) if low == high else f"{low}/{high}"

def upsert_result_confirm(context_id: str, thread_root_id: str, admin_id: str,
                          command_key: str, mode: str, result_value=None, reason_text: str | None = None,
                          timeout_minutes: int = RESULT_CONFIRM_TIMEOUT_MINUTES):
    created_at = now_utc_str()
    expires_at_dt = datetime.now(timezone.utc) + timedelta(minutes=timeout_minutes)
    expires_at = expires_at_dt.strftime(TIME_FMT)
    with closing(db()) as con:
        con.execute("BEGIN IMMEDIATE")
        try:
            con.execute("""
                INSERT OR REPLACE INTO result_confirms(
                    context_id, thread_root_id, admin_id, command_key, mode,
                    result_value, reason_text, created_at, expires_at
                ) VALUES (?,?,?,?,?,?,?,?,?)
            """, (
                context_id, thread_root_id, admin_id, command_key, mode,
                result_value, reason_text, created_at, expires_at
            ))
            con.commit()
        except:
            con.rollback()
            raise

def get_result_confirm(context_id: str, thread_root_id: str):
    with closing(db()) as con:
        row = con.execute("""
            SELECT admin_id, command_key, mode, result_value, reason_text, created_at, expires_at
            FROM result_confirms
            WHERE context_id=? AND thread_root_id=?
        """, (context_id, thread_root_id)).fetchone()
        if not row:
            return None
        admin_id, command_key, mode, result_value, reason_text, created_at, expires_at = row
        try:
            expires_dt = parse_utc(expires_at).replace(tzinfo=timezone.utc)
            if expires_dt < datetime.now(timezone.utc):
                con.execute("DELETE FROM result_confirms WHERE context_id=? AND thread_root_id=?",
                            (context_id, thread_root_id))
                return None
        except Exception:
            con.execute("DELETE FROM result_confirms WHERE context_id=? AND thread_root_id=?",
                        (context_id, thread_root_id))
            return None
        return {
            "admin_id": admin_id,
            "command_key": command_key,
            "mode": mode,
            "result_value": result_value,
            "reason_text": reason_text,
            "created_at": created_at,
            "expires_at": expires_at,
        }

def clear_result_confirm(context_id: str, thread_root_id: str):
    with closing(db()) as con:
        con.execute("DELETE FROM result_confirms WHERE context_id=? AND thread_root_id=?",
                    (context_id, thread_root_id))

def build_result_confirm_message(command_key: str) -> str:
    tail = command_key
    if tail.startswith("แจ้งผล"):
        tail = tail[len("แจ้งผล"):].strip()
    tail = tail or command_key
    return (
        f"⚠️ ยืนยันผล: {tail}\n"
        f"ส่ง {command_key} อีกครั้งเพื่อยืนยัน (หมดเวลาใน {RESULT_CONFIRM_TIMEOUT_MINUTES} นาที)"
    )

# -------------------- Admin / Users helpers --------------------
def is_admin(uid: str) -> bool:
    with closing(db()) as con:
        row = con.execute("SELECT 1 FROM admins WHERE admin_id=?", (uid,)).fetchone()
        return bool(row)

def next_user_idx(con) -> int:
    r = con.execute("SELECT COALESCE(MAX(idx),0)+1 FROM user_index").fetchone()
    return int(r[0] or 1)

def upsert_user(uid, name):
    clean_name = (name or "").strip() or (uid[-6:] if uid else "")
    with closing(db()) as con:
        con.execute("BEGIN IMMEDIATE")
        try:
            if con.execute("SELECT 1 FROM users WHERE user_id=?", (uid,)).fetchone() is None:
                con.execute("INSERT INTO users(user_id,user_name,credit,reserved) VALUES(?,?,0,0)", (uid,clean_name))
            else:
                con.execute("UPDATE users SET user_name=? WHERE user_id=?", (clean_name,uid))
            # ensure idx
            r = con.execute("SELECT 1 FROM user_index WHERE user_id=?", (uid,)).fetchone()
            if not r:
                idx = next_user_idx(con)
                con.execute("INSERT INTO user_index(user_id, idx) VALUES(?,?)", (uid, idx))
            con.commit()
            _cache_display_name("", uid, clean_name)
        except:
            con.rollback()
            raise

def get_or_assign_idx(uid: str) -> int:
    with closing(db()) as con:
        con.execute("BEGIN IMMEDIATE")
        try:
            r = con.execute("SELECT idx FROM user_index WHERE user_id=?", (uid,)).fetchone()
            if r:
                idx = int(r[0])
            else:
                idx = next_user_idx(con)
                con.execute("INSERT INTO user_index(user_id, idx) VALUES(?,?)", (uid, idx))
            con.commit()
            return idx
        except:
            con.rollback()
            raise

def find_user_by_idx(idx: int):
    with closing(db()) as con:
        r = con.execute("""
            SELECT u.user_id, u.user_name, u.credit 
            FROM user_index ui 
            JOIN users u ON ui.user_id = u.user_id
            WHERE ui.idx=?""", (idx,)).fetchone()
        return r

def find_users_by_name(q: str, limit: int = 10):
    with closing(db()) as con:
        return con.execute(
            "SELECT user_id, user_name, credit FROM users WHERE user_name LIKE ? LIMIT ?",
            (f"%{q}%", limit)
        ).fetchall()

def get_credit(uid):
    with closing(db()) as con:
        r = con.execute("SELECT credit FROM users WHERE user_id=?", (uid,)).fetchone()
        return r[0] if r else 0

def get_balances(uid):
    with closing(db()) as con:
        r = con.execute("SELECT credit, reserved FROM users WHERE user_id=?", (uid,)).fetchone()
        if not r: return (0,0)
        return (int(r[0] or 0), int(r[1] or 0))

def get_available(uid):
    cr, rs = get_balances(uid)
    return cr - rs

def get_active_using_credit(uid):
    with closing(db()) as con:
        row = con.execute("""
            SELECT COALESCE(SUM(amount), 0)
            FROM bets
            WHERE status='active' AND (poster_id=? OR opponent_id=?)
        """, (uid, uid)).fetchone()
        return int((row[0] or 0)) if row else 0

def add_credit(uid, delta):
    with closing(db()) as con:
        con.execute("BEGIN IMMEDIATE")
        try:
            con.execute("UPDATE users SET credit=credit+? WHERE user_id=?", (delta, uid))
            con.commit()
        except:
            con.rollback()
            raise

def add_credit_tx(con, uid, delta):
    con.execute("UPDATE users SET credit=credit+? WHERE user_id=?", (delta, uid))

def add_reserved_tx(con, uid, delta):
    if delta < 0:
        con.execute("""
            UPDATE users 
            SET reserved = MAX(0, reserved + ?) 
            WHERE user_id=?""", (delta, uid))
    else:
        con.execute("UPDATE users SET reserved = reserved + ? WHERE user_id=?", (delta, uid))

# -------------------- Name resolver (Group/Room/1:1) --------------------
DISPLAY_NAME_CACHE_TTL_SECONDS = int(os.environ.get("DISPLAY_NAME_CACHE_TTL_SECONDS", "1800"))
_display_name_cache = {}
_display_name_cache_lock = threading.Lock()

def _display_cache_key(context_id: str, user_id: str):
    return (context_id or "", user_id or "")

def _cache_display_name(context_id: str, user_id: str, name: str):
    if not user_id or not name:
        return
    with _display_name_cache_lock:
        _display_name_cache[_display_cache_key(context_id, user_id)] = (name, time.time() + DISPLAY_NAME_CACHE_TTL_SECONDS)

def _get_cached_display_name(context_id: str, user_id: str):
    if not user_id:
        return None
    key = _display_cache_key(context_id, user_id)
    now_ts = time.time()
    with _display_name_cache_lock:
        item = _display_name_cache.get(key)
        if not item:
            return None
        name, expires_at = item
        if expires_at <= now_ts:
            _display_name_cache.pop(key, None)
            return None
        return name

_RAW_QUOTE_CACHE = {}
_RAW_QUOTE_CACHE_LOCK = threading.Lock()
_RAW_QUOTE_TTL_SECONDS = 900

def _cleanup_raw_quote_cache(now_ts: float | None = None):
    now_ts = now_ts or time.time()
    expired = [k for k, v in _RAW_QUOTE_CACHE.items() if (now_ts - float(v.get("ts") or 0)) > _RAW_QUOTE_TTL_SECONDS]
    for k in expired:
        _RAW_QUOTE_CACHE.pop(k, None)

def cache_raw_quote_ids_from_body(body: str):
    """
    บาง SDK/ขั้นตอน deserialize อาจทำให้ quotedMessageId ไม่โผล่ใน event object
    จึงอ่านจาก raw webhook body แล้ว cache ไว้ตาม message.id
    """
    try:
        data = json.loads(body or "{}")
    except Exception:
        return

    events = data.get("events") or []
    now_ts = time.time()
    with _RAW_QUOTE_CACHE_LOCK:
        _cleanup_raw_quote_cache(now_ts)
        for ev in events:
            msg = (ev or {}).get("message") or {}
            mid = msg.get("id")
            if not mid:
                continue
            qid = (
                msg.get("quotedMessageId")
                or msg.get("quoted_message_id")
                or ((msg.get("quote") or {}).get("messageId"))
                or ((msg.get("quote") or {}).get("quotedMessageId"))
            )
            _RAW_QUOTE_CACHE[str(mid)] = {
                "quoted_message_id": str(qid) if qid else None,
                "ts": now_ts,
            }

def get_cached_raw_quote_id(message_id: str):
    if not message_id:
        return None
    now_ts = time.time()
    with _RAW_QUOTE_CACHE_LOCK:
        _cleanup_raw_quote_cache(now_ts)
        row = _RAW_QUOTE_CACHE.get(str(message_id)) or {}
        qid = row.get("quoted_message_id")
        return str(qid) if qid else None

def get_stored_user_name(user_id: str):
    if not user_id:
        return None
    with closing(db()) as con:
        row = con.execute("SELECT user_name FROM users WHERE user_id=?", (user_id,)).fetchone()
        if not row:
            return None
        name = (row[0] or "").strip()
        return name or None

def resolve_display_name(context_id: str, user_id: str, fallback: str = "", force_remote: bool = False) -> str:
    default_name = user_id[-6:] if user_id else ""
    fallback_name = (fallback or "").strip() or get_stored_user_name(user_id) or default_name

    cached = _get_cached_display_name(context_id, user_id)
    if cached:
        return cached

    # ใช้ชื่อที่เคยมีอยู่แล้วก่อน ลด network call ไปยัง LINE API
    if not force_remote and fallback_name and fallback_name != default_name:
        _cache_display_name(context_id, user_id, fallback_name)
        return fallback_name

    if not line_bot_api:
        _cache_display_name(context_id, user_id, fallback_name)
        return fallback_name

    name = fallback_name
    try:
        if context_id and context_id.startswith("C"):
            prof = line_bot_api.get_group_member_profile(context_id, user_id)
        elif context_id and context_id.startswith("R"):
            prof = line_bot_api.get_room_member_profile(context_id, user_id)
        else:
            prof = line_bot_api.get_profile(user_id)
        if prof and getattr(prof, "display_name", None):
            name = prof.display_name.strip() or name
    except Exception:
        pass

    _cache_display_name(context_id, user_id, name)
    return name

# -------------------- Keywords / Parsers --------------------
HIGH_WORDS = {"ชล","ไล่","ช่างไล่","ช่างชนะ","ล"}
LOW_WORDS  = {"ชถ","ชร","ช่างถอย","ช่างรับ","ช่างมา","ถ","ยั้ง","ถอย"}
LOCK_WORDS = {"ต","ตอด","ตต","จ","ตอก","ตช","ติดครับ","ติด"}
re_price_any = re.compile(r"(\d{2,4})\s*[/\-]\s*(\d{2,4})")

def opposite(side: str) -> str: return "ต่ำ" if side == "สูง" else "สูง"

def parse_open_round(text: str):
    """
    รองรับ 2 แบบ:
    - แบบเดิม: O <ชื่อค่าย> 350/410  หรือ เปิดรอบ <ชื่อค่าย> 350/410
    - แบบใหม่: เปิด <ชื่อค่าย>   (ยังไม่ต้องมีราคา)
    """
    t = (text or "").strip()
    payload = None
    if t[:1].lower() == "o":
        payload = t[1:].strip()
    elif t.startswith("เปิดรอบ"):
        payload = t[len("เปิดรอบ"):].strip()
    elif t.startswith("เปิด"):
        payload = t[len("เปิด"):].strip()
    else:
        return None

    m = re_price_any.search(payload or "")
    if m:
        low, high = int(m.group(1)), int(m.group(2))
        if low >= high:
            return None
        label = (payload or "")[:m.start()].strip()
        if not label:
            label = "รอบ"
        return (label, low, high)

    # ไม่มีราคามาด้วย -> เปิดรอบแบบรอช่างตีราคา
    label = (payload or "").strip()
    if not label:
        label = "รอบ"
    return (label, None, None)

def parse_base_price(text: str):
    """
    รองรับ:
    - ราคาช่าง 210
    - ราคาช่าง 330-360 / ราคาช่าง 330/360
    - ช่างตีราคา 330-360 / ช่างตีราคา 210 (เพื่อ backward compatible)
    - ราคาช่าง ไม่ตี / ช่างตีราคา ไม่ตี
    """
    t = (text or "").strip().translate(str.maketrans("๐๑๒๓๔๕๖๗๘๙", "0123456789"))
    if re.match(r"^(?:ราคาช่าง|ช่างตีราคา)\s*(?:ไม่ตี|ช่างไม่ตี)\s*$", t):
        return {"kind": "no_price"}

    m = re.match(r"^(?:ราคาช่าง|ช่างตีราคา)\s*(\d{2,4})(?:\s*[/\-]\s*(\d{2,4}))?\s*$", t)
    if not m:
        return None
    low = int(m.group(1))
    high = int(m.group(2)) if m.group(2) else low
    if high < low:
        return None
    return {"kind": "price", "low": low, "high": high}

def parse_play_command(text):
    # ให้ตัว parse หลักรองรับรูปแบบที่ติดกัน/มีราคาในข้อความอยู่แล้วก่อน
    parsed = parse_play_post(text)
    if parsed:
        return parsed

    parts = (text or "").strip().translate(THAI_DIGIT_MAP).split()
    if len(parts) < 2:
        return None

    play_flag = None
    if parts and parts[-1].strip().lower() == "ชตย":
        play_flag = "ชตย"
        parts = parts[:-1]
    if len(parts) < 2:
        return None

    cmd = parts[0]
    m_cmd = re.match(r"^(?:(\d{2,4})\s*[/\-]\s*(\d{2,4}))?\s*([+\-]\d+)?\s*(.+)$", cmd)
    if not m_cmd:
        return None

    pl = ph = None
    delta = 0
    if m_cmd.group(1) and m_cmd.group(2):
        pl, ph = int(m_cmd.group(1)), int(m_cmd.group(2))
        if pl >= ph:
            return None
    if m_cmd.group(3):
        delta = int(m_cmd.group(3))

    side_token = (m_cmd.group(4) or "").strip()
    if side_token in HIGH_WORDS:
        side = "สูง"
    elif side_token in LOW_WORDS:
        side = "ต่ำ"
    else:
        return None

    m_amount = re.search(r"(\d[\d,]*)", " ".join(parts[1:]))
    if not m_amount:
        return None
    amount = int(m_amount.group(1).replace(",", ""))
    if amount <= 0:
        return None

    return {"side": side, "amount": amount, "pl": pl, "ph": ph, "delta": delta, "play_flag": play_flag}


def parse_play_post(text: str):
    """
    รองรับรูปแบบหลักทั้งหมด เช่น:
      ชล500
      ถ500
      +5ถ500
      -3ชล200
      ชล 500
      +5 ถ 500
      330-360ล500
      330/360ชถ500
      330-360 +5 ถ 500
      320-360ล500 ชตย
    """
    raw = (text or "").strip().translate(THAI_DIGIT_MAP)
    raw = raw.replace(",", "")
    t = raw.lower().replace(" ", "")
    if not t:
        return None

    play_flag = None
    if t.endswith("ชตย"):
        play_flag = "ชตย"
        t = t[:-4]
        if not t:
            return None

    high_tokens = sorted([w.lower().replace(" ", "") for w in HIGH_WORDS], key=len, reverse=True)
    low_tokens  = sorted([w.lower().replace(" ", "") for w in LOW_WORDS],  key=len, reverse=True)

    def _match(tokens, side_name):
        for tok in tokens:
            mm = re.match(rf"^(?:(\d{{2,4}})[/\-](\d{{2,4}}))?([+\-]\d+)?{re.escape(tok)}(\d+)$", t)
            if not mm:
                continue
            pl = ph = None
            if mm.group(1) and mm.group(2):
                pl, ph = int(mm.group(1)), int(mm.group(2))
                if pl >= ph:
                    return None
            delta = int(mm.group(3)) if mm.group(3) else 0
            amt = int(mm.group(4))
            if amt <= 0:
                return None
            return {"side": side_name, "amount": amt, "pl": pl, "ph": ph, "delta": delta, "play_flag": play_flag}
        return None

    hit = _match(high_tokens, "สูง")
    if hit:
        return hit
    hit = _match(low_tokens, "ต่ำ")
    if hit:
        return hit
    return None

THAI_DIGIT_MAP = str.maketrans("๐๑๒๓๔๕๖๗๘๙", "0123456789")
def normalize_text(t: str) -> str:
    t = (t or "").strip().translate(THAI_DIGIT_MAP)
    t = re.sub(r",", "", t)
    t = re.sub(r"^\d+\s+", "", t)
    return t.lower()

def parse_lock_request(text: str):
    """
    รองรับ:
      ต / ติด
      ต300 / ติด300
      ต 300 / ติด 300
    คืนค่า {"amount": None|int} ถ้าไม่ใช่คำสั่งติดจะคืน None
    """
    raw = (text or "").strip().translate(THAI_DIGIT_MAP)
    raw = raw.replace(",", "")
    t = raw.lower().replace(" ", "")
    if not t:
        return None

    tokens = sorted([w.lower().replace(" ", "") for w in LOCK_WORDS], key=len, reverse=True)
    for tok in tokens:
        if t == tok:
            return {"amount": None}
        m = re.match(rf"^{re.escape(tok)}(\d+)$", t)
        if m:
            amt = int(m.group(1))
            if amt <= 0:
                return None
            return {"amount": amt}
    return None

def parse_settle_command(text: str):
    """
    สรุปผลรอบ
    - แจ้งผล <ตัวเลข>           -> สรุปแบบคิดยอดจากราคาช่างปกติ
    - แจ้งผล ชตย <ตัวเลข> / แจ้งผล ช่างไม่ตี <ตัวเลข>
      -> สรุปรอบช่างไม่ตี โดยคิดเฉพาะบิลที่มีช่วงตัวเลขในบิล เช่น 300-340ถ / 300-320ล1000
    - แจ้งผล <ข้อความ...>        -> ไม่คิดยอด (void) เช่น แจ้งผล บั้งไฟหาย
    - ยังรองรับคำสั่งเดิม: รส/รต/สรุปสูง/สรุปต่ำ/ออกสูง/ออกต่ำ
    """
    raw = (text or "").strip().translate(THAI_DIGIT_MAP)
    if not raw:
        return None

    # ✅ แบบใหม่: "แจ้งผล ..."
    if raw.startswith("แจ้งผล"):
        rest = raw[len("แจ้งผล"):].strip()
        if not rest:
            return None

        # รอบช่างไม่ตี ต้องมีเลขผลจริงต่อท้ายเสมอ
        m_no_price = re.fullmatch(r"(?:ชตย|ช่างไม่ตี)\s*(\d+)", rest, flags=re.IGNORECASE)
        if m_no_price:
            return {"side": "ช่างไม่ตี", "value": int(m_no_price.group(1)), "mode": "auto_no_price"}

        # ตัวเลขล้วน -> คิดยอดรอบปกติ
        if re.fullmatch(r"\d+", rest):
            return {"side": None, "value": int(rest), "mode": "auto"}

        # พิมพ์ ชตย แต่ไม่มีตัวเลข -> ถือว่าฟอร์แมตผิด ไม่ใช่ void
        if re.fullmatch(r"(?:ชตย|ช่างไม่ตี)", rest, flags=re.IGNORECASE):
            return {"mode": "invalid_no_price_result"}

        # ข้อความ -> ไม่คิดยอด
        reason = rest
        # รองรับเคสผู้ใช้พิมพ์ ... "" ต่อท้าย
        reason = re.sub(r'\s*""\s*$', '', reason).strip()
        # ตัด quote ครอบ
        if (reason.startswith('"') and reason.endswith('"')) or (reason.startswith("'") and reason.endswith("'")):
            reason = reason[1:-1].strip()
        # ตัด quote ที่เหลือ
        reason = reason.replace('"', '').replace("'", "").strip()
        if not reason:
            return None
        return {"mode": "void_text", "text": reason}

    # ---- คำสั่งเดิม ----
    t = normalize_text(text)
    if not t:
        return None

    patterns = [
        r"^[s]\s*(ต|ส)\s*(\d+)$",
        r"^[ร]\s*(ต|ส)\s*(\d+)$",
        r"^สรุป\s*(ต่ำ|สูง)\s*(\d+)$",
        r"^ออก\s*(ต่ำ|สูง)\s*(\d+)$",
        r"^[ร]\s*(ต|ส)(\d+)$",
        r"^[s]\s*(ต|ส)(\d+)$",
        r"^สรุป(ต่ำ|สูง)(\d+)$",
    ]
    for p in patterns:
        m = re.match(p, t)
        if m:
            if m.group(1) in ("ต", "ส"):
                side = "ต่ำ" if m.group(1) == "ต" else "สูง"
                value = int(m.group(2))
            else:
                side = m.group(1)
                value = int(m.group(2))
            return {"side": side, "value": value, "mode": "manual"}

    return None

def is_lock_word(text): return (text or "").strip() in LOCK_WORDS

# -------------------- Round helpers --------------------
def must_round(context_id, root):
    with closing(db()) as con:
        return con.execute("""
            SELECT status, base_low, base_high FROM rounds
            WHERE context_id=? AND thread_root_id=?
        """, (context_id, root)).fetchone()

def latest_open_round(context_id, max_age_minutes=999999):
    with closing(db()) as con:
        row = con.execute("""
            SELECT thread_root_id, status, base_low, base_high, opened_at
            FROM rounds
            WHERE context_id=? AND status='open'
            ORDER BY opened_at DESC
            LIMIT 1
        """, (context_id,)).fetchone()
    if not row: return None
    root_id, status, bl, bh, opened_at = row
    try:
        dt = parse_utc(opened_at)
        age_min = (datetime.now(timezone.utc) - dt.replace(tzinfo=timezone.utc)).total_seconds()/60.0
        if age_min <= max_age_minutes:
            return (root_id, status, bl, bh)
        return None
    except Exception:
        return (root_id, status, bl, bh)

def latest_round(context_id, status_list=("closed","open")):
    with closing(db()) as con:
        placeholders = ",".join("?"*len(status_list))
        row = con.execute(f"""
            SELECT thread_root_id, status, base_low, base_high,
                   COALESCE(closed_at, opened_at) AS ts
            FROM rounds
            WHERE context_id=? AND status IN ({placeholders})
            ORDER BY ts DESC
            LIMIT 1
        """, (context_id, *status_list)).fetchone()
    if not row: return None
    root_id, status, bl, bh, _ = row
    return (root_id, status, bl, bh)


def pick_pending_confirm_accept_message_id(con, context_id: str, round_root_id: str, poster_id: str,
                                           prefer_recent_seconds: int = 180):
    """
    ใช้ตอน LINE ไม่ส่ง quotedMessageId มาให้ แต่ผู้โพสพิมพ์ 'ต' เพื่อยืนยัน
    เลือกบิลรอยืนยันของเจ้าของโพสแบบปลอดภัยที่สุด:
    1) ถ้ามีใบเดียว -> ใช้ใบนั้น
    2) ถ้ามีหลายใบ แต่มีใบที่เพิ่งถูกสร้างในช่วงสั้น ๆ เพียงใบเดียว -> ใช้ใบนั้น
    3) อย่างอื่นถือว่ากำกวม -> ให้ quote เอง
    """
    rows = con.execute("""
        SELECT id, accept_message_id, created_at
        FROM bets
        WHERE context_id=? AND thread_root_id=? AND poster_id=? AND status='waiting_confirm'
        ORDER BY id DESC
        LIMIT 20
    """, (context_id, round_root_id, poster_id)).fetchall()

    usable = [r for r in rows if r[1]]
    if len(usable) == 1:
        return str(usable[0][1])
    if not usable:
        return None

    now_dt = datetime.now(timezone.utc)
    recent = []
    for _bid, accept_mid, created_at in usable:
        try:
            age = (now_dt - parse_utc(created_at).replace(tzinfo=timezone.utc)).total_seconds()
        except Exception:
            continue
        if age <= prefer_recent_seconds:
            recent.append((accept_mid, age))

    if len(recent) == 1:
        return str(recent[0][0])

    return None


def pick_recent_open_play_message_id(con, context_id: str, current_user_id: str, current_message_id: str,
                                     prefer_recent_seconds: int = 120, scan_limit: int = 40):
    """
    ใช้ตอน LINE ไม่ส่ง quotedMessageId มาให้ และ 'คนรับ' พิมพ์ ต/ติด
    เพื่อลดการจับคู่ผิด จะเลือกได้เฉพาะกรณีที่มีโพสเล่นจาก "คนอื่น" ที่ชัดเจนเพียงโพสเดียว
    ในช่วงเวลาใกล้ ๆ เท่านั้น ถ้ากำกวมจะคืน None เพื่อบังคับให้ Quote เอง
    """
    rows = con.execute(
        """SELECT message_id, user_id, text, created_at
             FROM messages
             WHERE context_id=? AND message_id<>?
             ORDER BY created_at DESC
             LIMIT ?""",
        (context_id, current_message_id, scan_limit)
    ).fetchall()

    now_dt = datetime.now(timezone.utc)
    recent_candidates = []
    for mid, uid, text, created_at in rows:
        if uid == current_user_id:
            continue
        play = parse_play_post(text) or parse_play_command(text)
        if not play:
            continue
        try:
            age = (now_dt - parse_utc(created_at).replace(tzinfo=timezone.utc)).total_seconds()
        except Exception:
            continue
        if age <= prefer_recent_seconds:
            recent_candidates.append((str(mid), age))

    if len(recent_candidates) == 1:
        return recent_candidates[0][0]
    return None


def get_message_by_id(con, context_id: str, message_id: str):
    return con.execute("""
        SELECT message_id, user_id, user_name, text, created_at
        FROM messages
        WHERE context_id=? AND message_id=?
        LIMIT 1
    """, (context_id, message_id)).fetchone()


def get_bet_by_accept_message_id(con, context_id: str, accept_message_id: str):
    return con.execute("""
        SELECT id, thread_root_id, poster_id, poster_name, opponent_id, opponent_name, status,
               amount, want_side, price_low, price_high, play_flag, delta, open_message_id, accept_message_id
        FROM bets
        WHERE context_id=? AND accept_message_id=?
        ORDER BY id DESC
        LIMIT 1
    """, (context_id, accept_message_id)).fetchone()


def get_waiting_confirms_by_open_message_id(con, context_id: str, open_message_id: str):
    return con.execute("""
        SELECT id, poster_id, poster_name, opponent_id, opponent_name, status, amount, want_side,
               price_low, price_high, delta, accept_message_id
        FROM bets
        WHERE context_id=? AND open_message_id=? AND status='waiting_confirm'
        ORDER BY id DESC
    """, (context_id, open_message_id)).fetchall()


def compute_outcome(value, low, high):
    if value < low: return "ต่ำ"
    if value > high: return "สูง"
    return "เสมอ"

# -------------------- Settle / Clear --------------------
def settle_round(context_id, thread_root_id, result_side, result_value):
    now = now_utc_str()
    with closing(db()) as con:
        con.execute("BEGIN IMMEDIATE")
        try:
            r = con.execute("""
                SELECT status, base_low, base_high, base_mode, closed_at, label
                FROM rounds WHERE context_id=? AND thread_root_id=?
            """, (context_id, thread_root_id)).fetchone()
            if not r:
                con.rollback()
                return {"ok":False,"msg":"ยังไม่มีรอบในเธรดนี้"}

            status, base_low, base_high, base_mode, closed_at, label = r
            con.execute("""UPDATE rounds SET status='settled', result_side=?, result_value=?
                           WHERE context_id=? AND thread_root_id=?""",
                        (result_side, result_value, context_id, thread_root_id))
            con.execute("DELETE FROM result_confirms WHERE context_id=? AND thread_root_id=?",
                        (context_id, thread_root_id))

            if closed_at:
                cur = con.execute("""SELECT id, poster_id, opponent_id, amount, want_side, price_low, price_high, play_flag, delta
                                     FROM bets
                                     WHERE context_id=? AND thread_root_id=? AND status='active' AND created_at <= ?""",
                                  (context_id, thread_root_id, closed_at))
            else:
                cur = con.execute("""SELECT id, poster_id, opponent_id, amount, want_side, price_low, price_high, play_flag, delta
                                     FROM bets
                                     WHERE context_id=? AND thread_root_id=? AND status='active'""",
                                  (context_id, thread_root_id))
            rows = cur.fetchall()
            if not rows:
                con.commit()
                return {"ok":True,"msg":"สรุปผลแล้ว แต่ไม่มีบิลที่เข้าเงื่อนไข"}

            total_fee=0
            for bid, poster_id, opponent_id, amount, want_side, pl, ph, play_flag, delta in rows:
                delta = int(delta or 0)
                play_flag = (play_flag or '').strip() or None
                has_explicit_range = (pl is not None and ph is not None)

                # รอบช่างไม่ตี:
                # - คิดเฉพาะบิลที่มี "ช่วงตัวเลข" อยู่ในบิลเอง เช่น 300-340ถ, 320-360ล, 300-340 +5 ถ
                # - บิลที่อิงราคาช่างฐาน เช่น ชล1000 / ชถ1000 / +5ล1000 / -3ถ1000 (ไม่มีช่วงตัวเลขในบิล) ให้ไม่คิดยอด
                if base_mode == 'no_price' and not has_explicit_range:
                    add_reserved_tx(con, poster_id, -amount)
                    add_reserved_tx(con, opponent_id, -amount)
                    con.execute("UPDATE bets SET status='settled', settled_at=?, result='void', fee=0 WHERE id=?",
                                (now, bid))
                    continue

                calc_pl, calc_ph = resolve_bet_price_range(base_low, base_high, pl, ph, delta)
                if calc_pl is None or calc_ph is None:
                    if base_mode == 'no_price':
                        add_reserved_tx(con, poster_id, -amount)
                        add_reserved_tx(con, opponent_id, -amount)
                        con.execute("UPDATE bets SET status='settled', settled_at=?, result='void', fee=0 WHERE id=?",
                                    (now, bid))
                        continue
                    con.rollback()
                    return {"ok":False,"msg":"ยังไม่ได้ตั้งราคาช่างของรอบนี้ (ใช้คำสั่ง: ราคาช่าง 210 หรือ ราคาช่าง ไม่ตี)"}

                add_reserved_tx(con, poster_id, -amount)
                add_reserved_tx(con, opponent_id, -amount)

                oc = compute_outcome(result_value, calc_pl, calc_ph)
                if oc == "เสมอ":
                    con.execute("UPDATE bets SET status='settled', settled_at=?, result='draw' WHERE id=?",
                                (now, bid))
                    continue

                if oc == want_side:
                    winner_id, loser_id = poster_id, opponent_id
                else:
                    winner_id, loser_id = opponent_id, poster_id

                prize = amount
                fee = int(round(prize * 0.10))
                net = prize - fee
                add_credit_tx(con, winner_id, net)
                add_credit_tx(con, loser_id, -prize)
                total_fee += fee

                con.execute("""UPDATE bets SET status='settled', settled_at=?, result='win',
                               winner_id=?, loser_id=?, fee=? WHERE id=?""",
                            (now, winner_id, loser_id, fee, bid))

            con.commit()
            return {"ok":True,
                    "msg":f"สรุปรอบ {label}: ออก {result_side} ({result_value})"}
        except:
            con.rollback()
            raise

def clear_round(context_id, thread_root_id=None):
    now = now_utc_str()
    with closing(db()) as con:
        con.execute("BEGIN IMMEDIATE")
        try:
            target = None
            if thread_root_id:
                target = con.execute("""
                    SELECT thread_root_id, label, status, base_low, base_high
                    FROM rounds
                    WHERE context_id=? AND thread_root_id=? AND status IN ('open','closed')
                """, (context_id, thread_root_id)).fetchone()
            if not target:
                target = con.execute("""
                    SELECT thread_root_id, label, status, base_low, base_high
                    FROM rounds
                    WHERE context_id=? AND status IN ('open','closed')
                    ORDER BY COALESCE(closed_at, opened_at) DESC
                    LIMIT 1
                """, (context_id,)).fetchone()
            if not target:
                con.rollback()
                return {"ok": False, "msg": "ไม่มีรอบที่ต้องเคลียร์ในห้องนี้"}

            rid, label, st, bl, bh = target

            # คลายเครดิตของบิลที่ active ก่อนยกเลิก
            active_rows = con.execute("""
                SELECT poster_id, opponent_id, amount 
                FROM bets 
                WHERE context_id=? AND thread_root_id=? AND status='active'
            """, (context_id, rid)).fetchall()
            for poster_id, opponent_id, amount in active_rows:
                add_reserved_tx(con, poster_id, -amount)
                add_reserved_tx(con, opponent_id, -amount)

            con.execute("""
                UPDATE bets
                SET status='settled', settled_at=?, result='void', fee=0
                WHERE context_id=? AND thread_root_id=? 
                  AND status IN ('waiting_accept','waiting_confirm','active')
            """, (now, context_id, rid))

            con.execute("""
                UPDATE rounds
                SET status='settled', result_side='ยกเลิก', result_value=NULL,
                    closed_at=COALESCE(closed_at, ?)
                WHERE context_id=? AND thread_root_id=?
            """, (now, context_id, rid))
            con.execute("DELETE FROM result_confirms WHERE context_id=? AND thread_root_id=?",
                        (context_id, rid))
            con.commit()
            return {"ok": True,
                    "msg": f"เคลียร์รอบแล้ว: {label} (ราคาช่าง {format_price_value(bl, bh)}) — เปิดรอบใหม่ได้เลย"}
        except:
            con.rollback()
            raise
        

def void_round_by_reason(context_id: str, thread_root_id: str, reason: str):
    """
    ไม่คิดยอด/ยกเลิกรอบแบบ "แจ้งผล <เหตุผล>"
    - คลาย reserved ของบิลที่ active
    - ทำบิลทั้งหมดในรอบนั้นเป็น void
    - ปิดรอบเป็น settled และบันทึก result_side = reason, result_value = NULL
    - แจ้งหลังบ้าน (DM) เฉพาะคนที่ "ติดสำเร็จ" (active) ว่าไม่คิดยอด
    """
    now = now_utc_str()
    reason = (reason or "").strip()
    if not reason:
        return {"ok": False, "msg": "เหตุผลว่าง"}

    participants = set()
    label = "รอบ"
    bl = bh = None

    with closing(db()) as con:
        con.execute("BEGIN IMMEDIATE")
        try:
            rd = con.execute("""
                SELECT label, base_low, base_high
                FROM rounds
                WHERE context_id=? AND thread_root_id=?
            """, (context_id, thread_root_id)).fetchone()
            if not rd:
                con.rollback()
                return {"ok": False, "msg": "ยังไม่มีรอบในเธรดนี้"}
            label, bl, bh = rd

            # คลาย reserved เฉพาะบิลที่ active (ติดสำเร็จแล้ว)
            active_rows = con.execute("""
                SELECT poster_id, opponent_id, amount
                FROM bets
                WHERE context_id=? AND thread_root_id=? AND status='active'
            """, (context_id, thread_root_id)).fetchall()

            for poster_id, opponent_id, amount in active_rows:
                amt = int(amount or 0)
                if poster_id:
                    participants.add(poster_id)
                    add_reserved_tx(con, poster_id, -amt)
                if opponent_id:
                    participants.add(opponent_id)
                    add_reserved_tx(con, opponent_id, -amt)

            # void บิลทั้งหมดที่ยังไม่ settled
            con.execute("""
                UPDATE bets
                SET status='settled', settled_at=?, result='void', fee=0
                WHERE context_id=? AND thread_root_id=?
                  AND status IN ('waiting_accept','waiting_confirm','active')
            """, (now, context_id, thread_root_id))

            # ปิดรอบเป็น settled + ตั้งผลเป็นข้อความ
            con.execute("""
                UPDATE rounds
                SET status='settled',
                    result_side=?,
                    result_value=NULL,
                    closed_at=COALESCE(closed_at, ?)
                WHERE context_id=? AND thread_root_id=?
            """, (reason, now, context_id, thread_root_id))
            con.execute("DELETE FROM result_confirms WHERE context_id=? AND thread_root_id=?",
                        (context_id, thread_root_id))

            con.commit()
        except:
            con.rollback()
            raise

    # 🔒 แจ้งหลังบ้าน (DM) ให้คนที่ active ในรอบนี้
    dm_text = f"⚠️ แจ้งผลรอบ {label}: {reason} — ไม่คิดยอด (คืนเครดิต)"
    if bl is not None and bh is not None:
        dm_text += f"\nราคาช่าง: {bl}/{bh}"

    try:
        if line_bot_api and participants:
            for uid in participants:
                try:
                    line_bot_api.push_message(uid, TextSendMessage(text=dm_text))
                except Exception:
                    pass
    except Exception:
        pass

    return {"ok": True, "msg": f"✅ยืนยันผล: {reason} — ไม่คิดยอด"}


def cancel_bet_by_id(context_id: str, bet_id: int):
    """
    ยกเลิกบิลตาม ID:
    - ยอมรับเฉพาะสถานะ waiting_accept / waiting_confirm / active
    - ถ้า active จะคลาย reserved ของทั้งสองฝั่ง
    - เซ็ตเป็น settled + result='void' + fee=0 + settled_at=now
    """
    now = now_utc_str()
    with closing(db()) as con:
        con.execute("BEGIN IMMEDIATE")
        try:
            row = con.execute("""
                SELECT id, context_id, thread_root_id, poster_id, poster_name,
                       opponent_id, opponent_name, status, amount
                FROM bets
                WHERE id=?""", (bet_id,)).fetchone()

            if not row:
                con.rollback()
                return {"ok": False, "msg": f"ไม่พบบิล ID {bet_id}"}

            (bid, bet_ctx, thread_root_id, poster_id, poster_name,
             opponent_id, opponent_name, status, amount) = row

            if bet_ctx != context_id:
                # กันเผลอยกเลิกข้ามห้อง
                con.rollback()
                return {"ok": False, "msg": f"บิล #{bet_id} ไม่ได้อยู่ในห้องนี้"}

            if status not in ("waiting_accept", "waiting_confirm", "active"):
                con.rollback()
                return {"ok": False, "msg": f"บิล #{bet_id} ไม่อยู่สถานะที่ยกเลิกได้ (ปัจจุบัน: {status})"}

            # ถ้าเป็น active ต้องคลาย reserved ให้ทั้งสองฝั่ง
            if status == "active":
                if poster_id:
                    add_reserved_tx(con, poster_id, -amount)
                if opponent_id:
                    add_reserved_tx(con, opponent_id, -amount)

            # ปิดบิลเป็น void
            con.execute("""
                UPDATE bets
                SET status='settled', settled_at=?, result='void', fee=0
                WHERE id=?""", (now, bid))

            con.commit()

            # ทำข้อความแจ้งผล
            a = poster_name or (poster_id[-6:] if poster_id else "-")
            b = opponent_name or (opponent_id[-6:] if opponent_id else "-")
            who = f"{a} vs {b}" if opponent_id else a
            return {"ok": True, "msg": f"ยกเลิกบิล #{bet_id} สำเร็จ ({who})"}
        except:
            con.rollback()
            raise


# -------------------- Flex Cards --------------------
def _play_side_code(side: str) -> str:
    return "ชล" if side == "สูง" else "ชถ"


def _play_side_suffix(side: str) -> str:
    return "ล" if side == "สูง" else "ถ"


def resolve_bet_price_range(base_low, base_high, pl, ph, delta=0):
    d = int(delta or 0)
    pl2 = (int(pl) + d) if pl is not None else ((int(base_low) + d) if base_low is not None else None)
    ph2 = (int(ph) + d) if ph is not None else ((int(base_high) + d) if base_high is not None else None)
    return pl2, ph2


def build_play_label(side: str, orig_pl=None, orig_ph=None, delta=0, play_flag: str | None = None) -> str:
    """
    แสดงราคาเล่นตามที่ผู้เล่นส่งมา/เล่นจริง
    - เล่นแบบตัวเลข: 320/360ล, 325/365ถ, 360ล
    - เล่นแบบบวก/ลบ: +5ชล, -3ถ
    - เล่นแบบไม่ระบุราคา: ชล, ชถ
    - ปิดท้ายด้วย flag พิเศษได้ เช่น ชตย
    """
    delta = int(delta or 0)
    if orig_pl is not None and orig_ph is not None:
        pl = int(orig_pl) + delta
        ph = int(orig_ph) + delta
        base = f"{format_price_value(pl, ph)}{_play_side_suffix(side)}"
    elif delta:
        delta_side = "ชล" if side == "สูง" else "ถ"
        base = f"{delta:+d}{delta_side}"
    else:
        base = _play_side_code(side)

    if play_flag:
        base = f"{base} {play_flag}"
    return base


def flex_bill_confirmed(poster_name, poster_side, opponent_name, opponent_side, amount, pl, ph, bet_id,
                       delta=0, note: str | None = None, played_text: str | None = None):
    """
    ส่งเข้าหลังบ้าน (DM) หลังจากบิล Active แล้ว
    - ใส่ปุ่ม "ขอยกเลิก" (Postback) เพื่อให้ผู้เล่นกดขอยกเลิกได้
    """
    delta = int(delta or 0)
    played_text = (played_text or "").strip() or build_play_label(poster_side, None, None, delta)
    if pl is None or ph is None:
        price_text = "รอช่างตีราคา" + (f" ({delta:+d})" if delta else "")
    else:
        price_text = format_price_value(pl, ph)

    bubble = {
      "type":"bubble",
      "header":{
        "type":"box","layout":"vertical","backgroundColor":"#1ABC9C","paddingAll":"16px",
        "contents":[{"type":"text","text":"จับคู่สำเร็จ ✅","weight":"bold","size":"lg","color":"#FFFFFF"}]
      },
      "body":{
        "type":"box","layout":"vertical","spacing":"md",
        "contents":[
          {"type":"text","text":f"Order #{bet_id}","size":"xs","color":"#666666"},
          {"type":"text","text":f"{amount:.2f}" if isinstance(amount,(int,float)) else str(amount),
           "size":"xxl","weight":"bold"},
          {"type":"separator"},
          {"type":"box","layout":"vertical","contents":[
            {"type":"box","layout":"baseline","contents":[
              {"type":"text","text":"ผู้เล่น","size":"sm","color":"#666666","flex":2},
              {"type":"text","text":poster_name,"size":"sm","weight":"bold","flex":4},
              {"type":"text","text":f"({poster_side})","size":"sm","align":"end","flex":2}
            ]},
            {"type":"box","layout":"baseline","contents":[
              {"type":"text","text":"ผู้เล่น","size":"sm","color":"#666666","flex":2},
              {"type":"text","text":opponent_name,"size":"sm","weight":"bold","flex":4},
              {"type":"text","text":f"({opponent_side})","size":"sm","align":"end","flex":2}
            ]},
          ]},
          {"type":"separator"},
          {"type":"box","layout":"baseline","contents":[
            {"type":"text","text":"ราคาเล่น","size":"sm","color":"#666666","flex":2},
            {"type":"text","text":played_text,"size":"sm","weight":"bold","flex":6,"wrap":True}
          ]},
          {"type":"box","layout":"baseline","contents":[
            {"type":"text","text":"ราคาคิด","size":"sm","color":"#666666","flex":2},
            {"type":"text","text":price_text,"size":"sm","weight":"bold","flex":6,"wrap":True}
          ]},
          {"type":"box","layout":"baseline","contents":[
            {"type":"text","text":"สถานะ","size":"sm","color":"#666666","flex":2},
            {"type":"text","text":"ยืนยันแล้ว ✅","size":"sm","weight":"bold","flex":6,"color":"#16A34A"}
          ]}
        ]
      },
      "footer":{
        "type":"box","layout":"vertical","spacing":"sm","contents":[
          {
            "type":"button",
            "style":"primary",
            "action":{
              "type":"postback",
              "label":"แตะเพื่อขอยกเลิก",
              "data":f"act=req_cancel&bet_id={bet_id}"
            }
          },
          {"type":"text","text":"* การยกเลิกต้องให้อีกฝ่ายยืนยัน","size":"xs","color":"#999999","wrap":True}
        ]
      }
    }
    if note:
        bubble["footer"]["contents"].append({"type":"text","text":note,"size":"xs","color":"#C0392B","wrap":True})

    return FlexSendMessage(alt_text=f"จับคู่สำเร็จ #{bet_id}", contents=bubble)


def flex_cancel_request(bet_id: int, amount: int, price_text: str, requester_name: str):
    bubble = {
      "type":"bubble",
      "header":{
        "type":"box","layout":"vertical","backgroundColor":"#F59E0B","paddingAll":"16px",
        "contents":[{"type":"text","text":"⚠️ คำขอยกเลิก","weight":"bold","size":"lg","color":"#FFFFFF"}]
      },
      "body":{
        "type":"box","layout":"vertical","spacing":"md",
        "contents":[
          {"type":"text","text":f"Order #{bet_id}","size":"xs","color":"#666666"},
          {"type":"text","text":f"{amount:.2f}" if isinstance(amount,(int,float)) else str(amount),
           "size":"xxl","weight":"bold"},
          {"type":"box","layout":"baseline","contents":[
            {"type":"text","text":"ผู้ขอยกเลิก","size":"sm","color":"#666666","flex":3},
            {"type":"text","text":requester_name or "-","size":"sm","weight":"bold","flex":7,"wrap":True}
          ]},
          {"type":"box","layout":"baseline","contents":[
            {"type":"text","text":"ราคา","size":"sm","color":"#666666","flex":3},
            {"type":"text","text":price_text,"size":"sm","weight":"bold","flex":7,"wrap":True}
          ]},
          {"type":"text","text":"ยืนยันยกเลิกไหม?","size":"sm","wrap":True}
        ]
      },
      "footer":{
        "type":"box","layout":"horizontal","spacing":"sm",
        "contents":[
          {
            "type":"button","style":"primary",
            "action":{"type":"postback","label":"ยืนยันยกเลิก","data":f"act=cancel_confirm&bet_id={bet_id}"}
          },
          {
            "type":"button","style":"secondary",
            "action":{"type":"postback","label":"ปฏิเสธ","data":f"act=cancel_reject&bet_id={bet_id}"}
          }
        ]
      }
    }
    return FlexSendMessage(alt_text=f"คำขอยกเลิก #{bet_id}", contents=bubble)


def flex_cancel_done(bet_id: int, amount: int):
    bubble = {
      "type":"bubble",
      "header":{
        "type":"box","layout":"vertical","backgroundColor":"#22C55E","paddingAll":"16px",
        "contents":[{"type":"text","text":"✅ ยกเลิกสำเร็จ","weight":"bold","size":"lg","color":"#FFFFFF"}]
      },
      "body":{
        "type":"box","layout":"vertical","spacing":"md",
        "contents":[
          {"type":"text","text":f"Order #{bet_id}","size":"xs","color":"#666666"},
          {"type":"text","text":f"{amount:.2f}" if isinstance(amount,(int,float)) else str(amount),
           "size":"xxl","weight":"bold"},
          {"type":"text","text":"ยอดที่กันไว้ (hold) ถูกคืนเรียบร้อยแล้ว","size":"sm","color":"#666666","wrap":True}
        ]
      }
    }
    return FlexSendMessage(alt_text=f"ยกเลิกสำเร็จ #{bet_id}", contents=bubble)


def flex_round_summary(context_id, thread_root_id):
    # สรุปผลรวม: ชื่อค่าย + ราคา + ผู้เล่นได้/เสีย (±)
    with closing(db()) as con:
        rd = con.execute("""
            SELECT label, base_low, base_high, result_side, result_value
            FROM rounds WHERE context_id=? AND thread_root_id=?""",
            (context_id, thread_root_id)).fetchone()
        if not rd:
            return TextSendMessage(text="ยังไม่มีรอบในเธรดนี้")
        label, bl, bh, rside, rvalue = rd

        rows = con.execute("""
            SELECT poster_id, poster_name, opponent_id, opponent_name, amount, fee, result, winner_id, loser_id
            FROM bets WHERE context_id=? AND thread_root_id=? AND status='settled'""",
            (context_id, thread_root_id)).fetchall()

        net = {}
        name_hint = {}
        for poster_id, poster_name, opp_id, opp_name, amount, fee, result, winner_id, loser_id in rows:
            if poster_id: name_hint[poster_id] = poster_name
            if opp_id:    name_hint[opp_id]    = opp_name
            if result == 'win':
                net[winner_id] = net.get(winner_id, 0) + (amount - (fee or 0))
                net[loser_id]  = net.get(loser_id, 0)  - amount

    # build player list with real display names
    items = []
    if net:
        for uid, val in sorted(net.items(), key=lambda x: (-x[1], x[0])):
            disp = resolve_display_name(context_id, uid, name_hint.get(uid, uid[-6:]))
            sign = "+" if val >= 0 else ""
            items.append({
                "type":"box","layout":"baseline","spacing":"sm","contents":[
                    {"type":"text","text":disp,"size":"sm","flex":3,"wrap":True},
                    {"type":"text","text":f"{sign}{val}","size":"sm","align":"end","flex":1}
                ]
            })
    else:
        items.append({"type":"text","text":"ยังไม่มีได้/เสีย","size":"sm","color":"#777777"})

    title = f"สรุปผล {label}"
    price = f"ราคา {format_price_value(bl, bh)}"
        # แสดงผลแบบภาษาบ้าน: สูง=ผ่าน, ต่ำ=ยั้ง
    def _show_side(s: str | None) -> str:
        if s == "สูง": return "ผ่าน"
        if s == "ต่ำ": return "แพ้"
        if s == "เสมอ": return "เสมอ"
        return s or "-"

    result_text = f"ผล: {_show_side(rside)} ({rvalue})" if (rside and rvalue is not None) else "ผล: -"

    bubble = {
      "type":"bubble",
      "size":"mega",
      "header":{"type":"box","layout":"vertical","paddingAll":"16px","backgroundColor":"#2ECC71","contents":[
        {"type":"text","text":title,"weight":"bold","size":"lg","color":"#FFFFFF"},
        {"type":"text","text":price,"size":"sm","color":"#E8F8F5"}
      ]},
      "body":{"type":"box","layout":"vertical","spacing":"md","contents":[
        {"type":"text","text":result_text,"size":"sm"},
        {"type":"separator"},
        {"type":"text","text":"รายชื่อผู้เล่น ได้/เสีย","weight":"bold","size":"sm"},
        {"type":"box","layout":"vertical","spacing":"xs","contents": items}
      ]}
    }
    return bubble



def push_private_safe(to_uid: str, message) -> bool:
    if not line_bot_api or not to_uid:
        return False
    try:
        line_bot_api.push_message(to_uid, message)
        return True
    except Exception:
        return False


def _fmt_points(n) -> str:
    try:
        v = int(round(float(n)))
    except Exception:
        return str(n)
    sign = "+" if v > 0 else ""
    return f"{sign}{v:,}"


def _show_round_outcome(side, value) -> str:
    if value is None:
        return side or "-"
    if side == "สูง":
        label = "ผ่าน"
    elif side == "ต่ำ":
        label = "แพ้"
    elif side == "เสมอ":
        label = "เสมอ"
    else:
        label = side or "-"
    return f"{label} ({value})"


def make_round_result_flex(label: str, display_name: str, net: int, balance: int,
                           result_side, result_value, base_low, base_high, detail_lines):
    positive = net > 0
    negative = net < 0
    header_color = "#22C55E" if positive else ("#E74C3C" if negative else "#6B7280")
    accent_color = "#22C55E" if positive else ("#E74C3C" if negative else "#6B7280")
    emoji = "🎉" if positive else ("😵" if negative else "📌")
    title = f'{emoji} ผลค่าย "{label}"'
    main_text = _fmt_points(net) if net else "0"
    if positive or negative:
        main_text = f"{main_text}.00 บาท"
    else:
        main_text = "0.00 บาท"

    outcome_text = _show_round_outcome(result_side, result_value)
    price_text = f"ราคา {format_price_value(base_low, base_high)}" if base_low is not None and base_high is not None else "ราคา รอช่างตีราคา"
    balance_text = f"{int(balance):,.2f} บาท"

    detail_boxes = []
    for item in (detail_lines or [])[:8]:
        change = int(item.get("change") or 0)
        chg_text = f"{change:+,.2f}" if change else "0.00"
        status = item.get("status") or "-"
        icon = item.get("icon") or "•"
        opp = item.get("opponent_name") or "-"
        played = item.get("played_text") or "-"
        detail_boxes.extend([
            {
                "type": "box", "layout": "vertical", "spacing": "xs",
                "contents": [
                    {"type": "text", "text": f'#{item.get("bet_id")} {icon} {status} vs {opp}', "size": "sm", "wrap": True},
                    {"type": "text", "text": f'{chg_text}', "size": "sm", "weight": "bold", "color": accent_color, "align": "end"},
                    {"type": "text", "text": f'คุณเล่น: {played} | {price_text}', "size": "xs", "color": "#8E8E93", "wrap": True},
                ]
            },
            {"type": "separator", "margin": "md"}
        ])
    if detail_boxes and detail_boxes[-1].get("type") == "separator":
        detail_boxes.pop()
    if not detail_boxes:
        detail_boxes = [{"type": "text", "text": "ไม่มีรายการในรอบนี้", "size": "sm", "color": "#777777"}]

    bubble = {
        "type": "bubble",
        "size": "mega",
        "header": {
            "type": "box", "layout": "vertical", "backgroundColor": header_color, "paddingAll": "16px",
            "contents": [
                {"type": "text", "text": title, "weight": "bold", "size": "lg", "color": "#FFFFFF", "wrap": True}
            ]
        },
        "body": {
            "type": "box", "layout": "vertical", "spacing": "md",
            "contents": [
                {"type": "text", "text": f'สวัสดี คุณ {display_name}', "size": "sm", "color": "#666666", "wrap": True},
                {"type": "text", "text": main_text, "size": "xxl", "weight": "bold", "color": accent_color, "align": "center", "wrap": True},
                {"type": "separator"},
                {"type": "text", "text": f'ผลออก {result_value}' if result_value is not None else f'ผลรอบ: {result_side or "-"}', "size": "sm", "color": "#666666"},
                {"type": "text", "text": outcome_text, "size": "sm", "weight": "bold", "color": accent_color},
                {"type": "separator"},
                *detail_boxes,
                {"type": "separator", "margin": "md"},
                {
                    "type": "box", "layout": "baseline",
                    "contents": [
                        {"type": "text", "text": "คงเหลือ", "size": "lg", "weight": "bold", "flex": 3},
                        {"type": "text", "text": balance_text, "size": "lg", "weight": "bold", "align": "end", "flex": 5}
                    ]
                }
            ]
        }
    }
    return FlexSendMessage(alt_text=f'ผลรอบ {label}', contents=bubble)


def make_welcome_flex(display_name: str, uid_code):
    bubble = {
        "type": "bubble",
        "size": "mega",
        "header": {
            "type": "box", "layout": "vertical", "backgroundColor": "#16A34A", "paddingAll": "18px",
            "contents": [
                {"type": "text", "text": "ยินดีต้อนรับ 💚", "weight": "bold", "size": "xl", "color": "#FFFFFF"}
            ]
        },
        "body": {
            "type": "box", "layout": "vertical", "spacing": "md",
            "contents": [
                {"type": "text", "text": f'สวัสดี คุณ {display_name}', "size": "md", "weight": "bold", "wrap": True},
                {"type": "text", "text": 'ส่งสลิป+สรุปยอด แชทนี้เลยจ้า ✅', "size": "md", "wrap": True},
                {"type": "separator"},
                {"type": "text", "text": '(!!!)ย้ำ !! บัญชีฝากและถอนต้องเป็นบัญชีเดียวกันเท่านั้น(!!!)', "size": "sm", "weight": "bold", "color": "#E11D48", "wrap": True},
                {"type": "text", "text": 'เล่นตามทุนนะคะ ❌', "size": "sm", "wrap": True},
                {"type": "text", "text": 'ฝากทุนแล้วส่งสลิปแจ้งยอดได้เลยจ้า หมานๆจ้าพี่ๆ♥️💸', "size": "sm", "wrap": True},
                {"type": "separator"},
                {"type": "text", "text": f'🪪 UID: {uid_code}', "size": "md", "weight": "bold", "wrap": True}
            ]
        }
    }
    return FlexSendMessage(alt_text='ยินดีต้อนรับ', contents=bubble)


def notify_round_result_private(context_id: str, thread_root_id: str):
    """
    ส่งผลรอบเข้าหลังบ้าน (DM) เป็น FLEX ให้ผู้เล่นแต่ละคนเท่านั้น
    - คนเสียสีแดง
    - คนได้สีเขียว
    - คนได้เห็นยอดสุทธิหลังหัก 10%
    - แจ้งยอดคงเหลือปัจจุบันด้วย
    """
    with closing(db()) as con:
        rd = con.execute("""
            SELECT label, base_low, base_high, result_side, result_value
            FROM rounds
            WHERE context_id=? AND thread_root_id=?
        """, (context_id, thread_root_id)).fetchone()
        if not rd:
            return

        label, base_low, base_high, result_side, result_value = rd

        rows = con.execute("""
            SELECT id, poster_id, poster_name, opponent_id, opponent_name,
                   amount, want_side, price_low, price_high, delta,
                   result, winner_id, loser_id, fee
            FROM bets
            WHERE context_id=? AND thread_root_id=? AND status='settled'
            ORDER BY id ASC
        """, (context_id, thread_root_id)).fetchall()

    if not rows:
        return

    user_summaries = {}
    name_hint = {}

    def _ensure(uid, fallback_name=""):
        if uid not in user_summaries:
            user_summaries[uid] = {"net": 0, "lines": []}
        if fallback_name:
            name_hint[uid] = fallback_name
        return user_summaries[uid]

    for bid, poster_id, poster_name, opponent_id, opponent_name, amount, want_side, pl, ph, delta, result, winner_id, loser_id, fee in rows:
        amount = int(amount or 0)
        fee = int(fee or 0)
        delta = int(delta or 0)
        played_text = build_play_label(want_side, pl, ph, delta, None)
        calc_pl, calc_ph = resolve_bet_price_range(base_low, base_high, pl, ph, delta)
        line_price_text = f"ราคา {format_price_value(calc_pl, calc_ph)}" if calc_pl is not None and calc_ph is not None else "รอช่างตีราคา"

        _ensure(poster_id, poster_name or "")
        _ensure(opponent_id, opponent_name or "")

        if result == "win":
            winner_gain = amount - fee
            loser_loss = -amount

            poster_change = winner_gain if poster_id == winner_id else loser_loss
            opponent_change = winner_gain if opponent_id == winner_id else loser_loss

            user_summaries[poster_id]["net"] += poster_change
            user_summaries[opponent_id]["net"] += opponent_change
            user_summaries[poster_id]["lines"].append({
                "bet_id": bid,
                "status": "ชนะ" if poster_change > 0 else "แพ้",
                "icon": "✅" if poster_change > 0 else "❌",
                "change": poster_change,
                "opponent_name": opponent_name or "-",
                "played_text": played_text,
                "price_text": line_price_text,
            })
            user_summaries[opponent_id]["lines"].append({
                "bet_id": bid,
                "status": "ชนะ" if opponent_change > 0 else "แพ้",
                "icon": "✅" if opponent_change > 0 else "❌",
                "change": opponent_change,
                "opponent_name": poster_name or "-",
                "played_text": build_play_label(opposite(want_side), pl, ph, delta, None),
                "price_text": line_price_text,
            })

        elif result == "draw":
            user_summaries[poster_id]["lines"].append({
                "bet_id": bid, "status": "เสมอ", "icon": "➖", "change": 0,
                "opponent_name": opponent_name or "-", "played_text": played_text, "price_text": line_price_text,
            })
            user_summaries[opponent_id]["lines"].append({
                "bet_id": bid, "status": "เสมอ", "icon": "➖", "change": 0,
                "opponent_name": poster_name or "-", "played_text": build_play_label(opposite(want_side), pl, ph, delta, None), "price_text": line_price_text,
            })

        elif result == "void":
            user_summaries[poster_id]["lines"].append({
                "bet_id": bid, "status": "ไม่คิดยอด (แผลยกเลิก)", "icon": "⛔", "change": 0,
                "opponent_name": opponent_name or "-", "played_text": played_text, "price_text": line_price_text,
            })
            user_summaries[opponent_id]["lines"].append({
                "bet_id": bid, "status": "ไม่คิดยอด (แผลยกเลิก)", "icon": "⛔", "change": 0,
                "opponent_name": poster_name or "-", "played_text": build_play_label(opposite(want_side), pl, ph, delta, None), "price_text": line_price_text,
            })

    for uid, info in user_summaries.items():
        display_name = resolve_display_name(context_id, uid, name_hint.get(uid, uid[-6:] if uid else ""))
        balance = get_credit(uid)
        net = int(info.get("net") or 0)
        flex = make_round_result_flex(
            label=label,
            display_name=display_name,
            net=net,
            balance=balance,
            result_side=result_side,
            result_value=result_value,
            base_low=base_low,
            base_high=base_high,
            detail_lines=info.get("lines") or [],
        )
        push_private_safe(uid, flex)

def flex_round_pairlist(context_id, thread_root_id):
    with closing(db()) as con:
        rd = con.execute("""
            SELECT label, base_low, base_high
            FROM rounds WHERE context_id=? AND thread_root_id=?
        """, (context_id, thread_root_id)).fetchone()
        if not rd:
            return None
        label, bl, bh = rd

        rows = con.execute("""
            SELECT poster_name, price_low, price_high, opponent_name, amount, want_side, delta
            FROM bets
            WHERE context_id=? AND thread_root_id=? AND status='active'
            ORDER BY id ASC
        """, (context_id, thread_root_id)).fetchall()

    # helper: แปลงชื่อ + แท็กสีเป็น spans
    def name_with_side_spans(name: str, side: str | None, is_opponent: bool = False):
        # ฝั่ง B ต้องเป็นฝั่งตรงข้ามถ้ามี side
        if side:
            side = ("ต่ำ" if side == "สูง" else "สูง") if is_opponent else side
        color = "#16A34A" if side == "สูง" else ("#EF4444" if side == "ต่ำ" else None)
        arrow = " ✅" if side == "สูง" else ("  ❌" if side == "ต่ำ" else "")
        tag  = f"{arrow} {side}" if side else ""
        spans = [{"type": "span", "text": name or "-"}]
        if side:
            spans.append({"type": "span", "text": " "})
            spans.append({"type": "span", "text": tag, "weight": "bold", "color": color})
        return spans

    # หัวตาราง
    header_row = {
        "type": "box",
        "layout": "horizontal",
        "spacing": "sm",
        "contents": [
            {"type": "text", "text": "ผู้เล่น", "weight": "bold", "size": "sm", "flex": 4},
            {"type": "text", "text": "ราคา",   "weight": "bold", "size": "sm", "flex": 3, "align": "center"},
            {"type": "text", "text": "ผู้เล่น","weight": "bold", "size": "sm", "flex": 4},
            {"type": "text", "text": "ยอด",    "weight": "bold", "size": "sm", "flex": 2, "align": "end"},
        ]
    }

    # แถวข้อมูล
    row_boxes = []
    for poster_name, pl, ph, opponent_name, amount, want_side, delta in rows:
        d = int(delta or 0)
        if pl is None or ph is None:
            if bl is not None and bh is not None:
                pl2, ph2 = int(bl) + d, int(bh) + d
                price_cell = format_price_value(pl2, ph2) + (f" ({d:+d})" if d else "")
            else:
                price_cell = "รอช่างตีราคา" + (f" ({d:+d})" if d else "")
        else:
            price_cell = format_price_value(pl, ph) + (f" ({d:+d})" if d else "")
        row_boxes.append({
            "type": "box",
            "layout": "horizontal",
            "spacing": "sm",
            "contents": [
                {
                    "type": "text",
                    "size": "sm",
                    "flex": 4,
                    "wrap": True,
                    "contents": name_with_side_spans(poster_name or "-", want_side, is_opponent=False)
                },
                {
                    "type": "text",
                    "text": price_cell,
                    "size": "sm",
                    "flex": 3,
                    "align": "center",
                    "maxLines": 1
                },
                {
                    "type": "text",
                    "size": "sm",
                    "flex": 4,
                    "wrap": True,
                    "contents": name_with_side_spans(opponent_name or "-", want_side, is_opponent=True)
                },
                {"type": "text", "text": str(amount), "size": "sm", "flex": 2, "align": "end", "weight": "bold"}
            ]
        })

    body_contents = [
        {"type": "text", "text": f"ชื่อค่าย: {label} | ราคา {bl}/{bh}", "size": "xs", "color": "#7F8C8D"},
        {"type": "separator"},
        header_row
    ] + (row_boxes if row_boxes else [
        {"type": "text", "text": "ยังไม่มีรายการเล่นในรอบนี้", "size": "sm", "color": "#777777"}
    ])

    bubble = {
        "type": "bubble",
        "size": "mega",
        "header": {
            "type": "box",
            "layout": "vertical",
            "backgroundColor": "#2C3E50",
            "paddingAll": "16px",
            "contents": [
                {"type": "text", "text": "เช็ครายการเดิมพันถูกต้องไหม?", "weight": "bold", "size": "lg", "color": "#FFFFFF"}
            ]
        },
        "body": {"type": "box", "layout": "vertical", "spacing": "md", "contents": body_contents}
    }
    return bubble




# -------------------- Core handler --------------------
def core_handle(context_id, user_id, user_name, text, message_id, thread_root_id, reply_to_message_id):
    now = now_utc_str()
    upsert_user(user_id, user_name)

    # log message + กัน webhook/message ซ้ำไม่ให้ประมวลผลซ้ำ
    with closing(db()) as con:
        con.execute("BEGIN IMMEDIATE")
        try:
            cur = con.execute("""INSERT OR IGNORE INTO messages(message_id,context_id,thread_root_id,user_id,user_name,text,created_at)
                           VALUES(?,?,?,?,?,?,?)""",
                        (message_id,context_id,thread_root_id,user_id,user_name,text,now))
            inserted = int(cur.rowcount or 0)
            con.commit()
        except:
            con.rollback()
            raise

    if inserted == 0:
        # LINE อาจส่ง event/message เดิมซ้ำได้ ให้ no-op ทันทีเพื่อกันบิล/เครดิตซ้ำ
        return None

    t = (text or "").strip()

    # -------- Quick Info Commands --------
    if t in ("กต","กติกา"):
        return TextSendMessage(text=RULES_TEXT)

    if t in ("บช","บัญชี"):
        return TextSendMessage(text=BANK_TEXT)
    
    # ยกเลิกบิลรายใบ: "ยกเลิก <IDบิล>" (admin only)
    if t.startswith("ยกเลิก"):
        if not is_admin(user_id):
            return "คำสั่งนี้แอดมินเท่านั้น"
        m = re.match(r"^ยกเลิก\s+(\d+)$", t)
        if not m:
            return "รูปแบบ: ยกเลิก <IDบิล>  เช่น ยกเลิก 568"
        bid = int(m.group(1))
        res = cancel_bet_by_id(context_id, bid)
        return res["msg"]


    # -------- Utility Commands --------
    if t.lower() == "uid":
        idx = get_or_assign_idx(user_id)
        return f"UID ของคุณ: {user_id}\nID ภายใน: {idx}"

    # เติม/ลบเครดิต: $+ <ID> <amount>  /  $- <ID> <amount>  (admin only)
    if t.startswith("$+") or t.startswith("$-"):
        if not is_admin(user_id):
            return "คำสั่งนี้แอดมินเท่านั้น"
        m = re.match(r"^\$(\+|\-)\s+(\d+)\s+(\d+)$", t)
        if not m: return "รูปแบบ: $+ <ID> <จำนวน>  หรือ  $- <ID> <จำนวน>"
        sign, idx_str, amt_str = m.group(1), m.group(2), m.group(3)
        uid_rec = find_user_by_idx(int(idx_str))
        if not uid_rec: return f"ไม่พบผู้ใช้ ID {idx_str}"
        target_uid, target_name, _ = uid_rec
        delta = int(amt_str) * (1 if sign == "+" else -1)
        add_credit(target_uid, delta)
        new_cr = get_credit(target_uid)
        return f"ปรับเครดิตของ {target_name} (ID {idx_str}) แล้ว: {('+' if delta>=0 else '')}{delta}\nเครดิตใหม่: {new_cr}"

    
    # -------- Credits Inquiry --------
    if t.upper() == "C" or t == "เครดิต":
        idx = get_or_assign_idx(user_id)
        total_credit, reserved_credit = get_balances(user_id)
        remaining_credit = max(0, int(total_credit or 0) - int(reserved_credit or 0))

        return "\n".join([
            f"ID: {idx} | {user_name}",
            f"💰 เครดิตคงเหลือ: {remaining_credit} บาท",
        ])


    if t.startswith("เครดิต "):
        qname = t.split(" ", 1)[1].strip()
        matches = find_users_by_name(qname, limit=10)
        if not matches: return f"ไม่พบผู้ใช้ที่ชื่อมีคำว่า “{qname}”"
        if len(matches) == 1:
            uid, nm, cr = matches[0]
            idx = get_or_assign_idx(uid)
            sign = "+" if (cr or 0) >= 0 else ""
            return f"ID: {idx} | เครดิตของ {nm}: {sign}{cr or 0}"
        lines = [f"พบหลายคนที่ชื่อมี “{qname}”: (แสดง ID เพื่อเติมเครดิต)"]
        for uid, nm, cr in matches:
            idx = get_or_assign_idx(uid)
            sign = "+" if (cr or 0) >= 0 else ""
            lines.append(f"• ID {idx} | {nm}: {sign}{cr or 0}")
        return "\n".join(lines)

    # -------- Admin-only Round Controls --------
    def admin_guard():
        return is_admin(user_id)

    # เคลียร์รอบ
    if t.upper() == "CR" or t == "เคลียร์รอบ":
        if not admin_guard(): return "คำสั่งนี้แอดมินเท่านั้น"
        res = clear_round(context_id, thread_root_id)
        return res["msg"]

    # สรุปยอด/สรุปรอบ
    if t in ("สรุปยอด","สรุปรอบ","ยอด"):
        root = thread_root_id
        with closing(db()) as con:
            has = con.execute("SELECT 1 FROM rounds WHERE context_id=? AND thread_root_id=?",
                            (context_id, root)).fetchone()
        if not has:
            lr = latest_round(context_id, ("closed","open"))
            if not lr: 
                return "ยังไม่มีรอบในระบบ"
            root = lr[0]

        bubble = flex_round_pairlist(context_id, root)
        if not bubble:
            return "ยังไม่มีรายการเล่นในรอบนี้"
        return FlexSendMessage(alt_text="รายการเล่น", contents=bubble)

    # เปิดรอบ (admin only)
    op = parse_open_round(t)
    if op:
        if not admin_guard():
            return "คำสั่งนี้แอดมินเท่านั้น"

        # กันเคสมีรอบค้างอยู่ก่อน
        with closing(db()) as con:
            pending = con.execute("""
                SELECT thread_root_id, label, status, base_low, base_high
                FROM rounds
                WHERE context_id=? AND status IN ('open','closed')
                ORDER BY COALESCE(closed_at, opened_at) DESC
                LIMIT 1
            """, (context_id,)).fetchone()

        if pending:
            pend_root, pend_label, pend_status, bl, bh = pending
            return (
                f"❌ยังมีรอบค้างอยู่❌: {pend_label} (สถานะ: {pend_status}, ราคาช่าง {format_price_value(bl, bh)})\n"
                f"โปรดสรุปผลรอบนั้นก่อน: รต/รส <ค่า> หรือ สรุปต่ำ/สรุปสูง <ค่า>\n"
                f"หรือพิมพ์ CR เพื่อเคลียร์รอบ (ยกเลิก)"
            )

        label, base_low, base_high = op
        base_text = format_price_value(base_low, base_high) if (base_low is not None and base_high is not None) else "รอช่างตีราคา"
        round_root_id = message_id
        with closing(db()) as con:
            con.execute("BEGIN IMMEDIATE")
            try:
                con.execute("""
                    INSERT OR REPLACE INTO rounds(
                        thread_root_id, context_id, admin_id, admin_name,
                        label, base_low, base_high, base_mode, status, opened_at
                    ) VALUES (?,?,?,?,?,?,?,?,?,?)
                """, (
                    round_root_id, context_id, user_id, user_name,
                    label, base_low, base_high, ("normal" if (base_low is not None and base_high is not None) else "pending"), "open", now
                ))
                con.commit()
            except:
                con.rollback()
                raise

        return (
            f"{label}\n\n"
            f"ช่าง ⛔️\n\n"
            f"🚀🚀🚀🚀🚀"
        )

    # ปิดรอบ (admin only)
    if t in ("ปิด","ปิดรอบ"):
        if not admin_guard(): return "คำสั่งนี้แอดมินเท่านั้น"
        closed_label = None
        with closing(db()) as con:
            con.execute("BEGIN IMMEDIATE")
            try:
                r = con.execute("SELECT status, label FROM rounds WHERE context_id=? AND thread_root_id=?",
                                (context_id, thread_root_id)).fetchone()
                target_root = thread_root_id
                if r and r[0] == "open":
                    closed_label = r[1]
                else:
                    lr = latest_open_round(context_id, 999999)
                    if not lr:
                        con.rollback()
                        return "ไม่มีรอบที่เปิดอยู่"
                    target_root = lr[0]
                    rr = con.execute("SELECT label FROM rounds WHERE context_id=? AND thread_root_id=?",
                                     (context_id, target_root)).fetchone()
                    closed_label = rr[0] if rr else "-"
                con.execute("UPDATE rounds SET status='closed', closed_at=? WHERE context_id=? AND thread_root_id=?",
                            (now, context_id, target_root))
                con.commit()
            except:
                con.rollback()
                raise
        closed_label = closed_label or "-"
        return (
            "❌❌❌❌ ปิด ❌❌❌❌\n\n"
            "3 2 1 ไป๊!! 🚀🚀🚀\n\n"
            f"{closed_label}\n\n"
            "⛔หลังปิดไม่ติดทุกกรณี"
        )

    
    # ช่างตีราคา (admin only)
    bp = parse_base_price(t)
    if bp:
        if not admin_guard(): return "คำสั่งนี้แอดมินเท่านั้น"
        target_root = thread_root_id
        with closing(db()) as con:
            has = con.execute("SELECT 1 FROM rounds WHERE context_id=? AND thread_root_id=?",
                              (context_id, target_root)).fetchone()
        if not has:
            lr = latest_round(context_id, ("closed","open"))
            if not lr: 
                return "ยังไม่มีรอบในห้องนี้"
            target_root = lr[0]

        with closing(db()) as con:
            con.execute("BEGIN IMMEDIATE")
            try:
                if bp.get("kind") == "no_price":
                    con.execute("UPDATE rounds SET base_low=NULL, base_high=NULL, base_mode='no_price' WHERE context_id=? AND thread_root_id=?",
                                (context_id, target_root))
                else:
                    low, high = int(bp["low"]), int(bp["high"])
                    con.execute("UPDATE rounds SET base_low=?, base_high=?, base_mode='normal' WHERE context_id=? AND thread_root_id=?",
                                (low, high, context_id, target_root))
                con.commit()
            except:
                con.rollback()
                raise
        if bp.get("kind") == "no_price":
            return "✅ ตั้งรอบนี้เป็น: ราคาช่าง ไม่ตี"
        return f"✅ ตั้งราคาช่างแล้ว: {format_price_value(low, high)}"
# สรุปผล (admin only)
    settle = parse_settle_command(t)
    if settle:
        if not admin_guard(): return "คำสั่งนี้แอดมินเท่านั้น"

        mode = settle.get("mode")
        if mode == "invalid_no_price_result":
            return "ฟอร์แมตแจ้งผลช่างไม่ตีไม่ถูก ต้องพิมพ์แบบ: แจ้งผล ชตย 370"

        # หา target รอบ (ถ้าไม่ได้อยู่ในเธรดเดียวกันให้ใช้รอบล่าสุด)
        target_root = thread_root_id
        with closing(db()) as con:
            has = con.execute("SELECT 1 FROM rounds WHERE context_id=? AND thread_root_id=?",
                              (context_id, target_root)).fetchone()
        if not has:
            lr = latest_round(context_id, ("closed","open"))
            if not lr: return "ยังไม่มีรอบในห้องนี้"
            target_root = lr[0]

        round_base_mode = None
        if mode != "void_text":
            with closing(db()) as con:
                rr = con.execute("SELECT base_low, base_high, COALESCE(base_mode, 'pending') FROM rounds WHERE context_id=? AND thread_root_id=?",
                                 (context_id, target_root)).fetchone()
            if not rr:
                return "ยังไม่มีรอบในห้องนี้"
            bl, bh, round_base_mode = rr
            if round_base_mode == 'pending':
                return "ยังไม่ได้ตั้งราคาช่าง: พิมพ์ ราคาช่าง 210 หรือ ราคาช่าง ไม่ตี ก่อน"
            if mode == "auto_no_price":
                if round_base_mode != 'no_price':
                    return "รอบนี้ไม่ได้ตั้งเป็นช่างไม่ตี ใช้คำสั่งแบบปกติ: แจ้งผล 370"
            elif settle.get("side") is None:
                if round_base_mode == 'no_price':
                    return "รอบนี้เป็นช่างไม่ตี ต้องพิมพ์แบบ: แจ้งผล ชตย 370"
                if bl is None or bh is None:
                    return "ยังไม่ได้ตั้งราคาช่าง: พิมพ์ ราคาช่าง 210 หรือ ราคาช่าง ไม่ตี ก่อน"

        needs_confirm = normalize_command_key(t).startswith("แจ้งผล")
        command_key = normalize_command_key(t)
        pending = get_result_confirm(context_id, target_root) if needs_confirm else None
        is_same_confirm = bool(
            pending and
            pending.get("admin_id") == user_id and
            pending.get("command_key") == command_key
        )

        if needs_confirm and not is_same_confirm:
            confirm_value = settle.get("value") if mode != "void_text" else None
            confirm_reason = settle.get("text") if mode == "void_text" else None
            upsert_result_confirm(
                context_id=context_id,
                thread_root_id=target_root,
                admin_id=user_id,
                command_key=command_key,
                mode=mode,
                result_value=confirm_value,
                reason_text=confirm_reason,
            )
            return build_result_confirm_message(command_key)

        if needs_confirm:
            clear_result_confirm(context_id, target_root)

        # ✅ แบบใหม่: แจ้งผล <ข้อความ> -> ไม่คิดยอด
        if mode == "void_text":
            reason = settle.get("text") or ""
            res = void_round_by_reason(context_id, target_root, reason)
            if res.get("ok"):
                push_private_safe(user_id, TextSendMessage(text=res.get("msg") or "ยืนยันผลแล้ว"))
                return None
            return res.get("msg") or "ยืนยันผลแล้ว"

        # ✅ แบบคิดยอด (แจ้งผล <ตัวเลข> หรือคำสั่งเดิม)
        value = int(settle["value"])

        # แจ้งผลตัวเลข -> คำนวณผลจากราคาช่างอัตโนมัติ
        side = settle.get("side")
        if side is None:
            with closing(db()) as con:
                rr = con.execute("SELECT base_low, base_high, COALESCE(base_mode, 'pending') FROM rounds WHERE context_id=? AND thread_root_id=?",
                                 (context_id, target_root)).fetchone()
            if not rr:
                return "ยังไม่มีรอบในห้องนี้"
            bl, bh, round_base_mode = rr
            if round_base_mode == 'pending':
                return "ยังไม่ได้ตั้งราคาช่าง: พิมพ์ ราคาช่าง 210 หรือ ราคาช่าง ไม่ตี ก่อน"
            if round_base_mode == 'no_price':
                side = 'ช่างไม่ตี'
            else:
                if bl is None or bh is None:
                    return "ยังไม่ได้ตั้งราคาช่าง: พิมพ์ ราคาช่าง 210 หรือ ราคาช่าง ไม่ตี ก่อน"
                side = compute_outcome(value, int(bl), int(bh))  # สูง/ต่ำ/เสมอ

        res = settle_round(context_id, target_root, side, value)
        if res.get("ok"):
            notify_round_result_private(context_id, target_root)
            push_private_safe(user_id, TextSendMessage(text=res.get("msg") or "สรุปผลสำเร็จ")) 
            return None
        else:
            return res.get("msg") or "สรุปผลไม่สำเร็จ"

# แจ้งฟอร์แมตผิด (กันเงียบ)
    if re.match(r"^\s*[srร]\s*[ตส]\s*\d+\s*$", normalize_text(t)) or t.startswith(("สรุป","ออก","แจ้งผล")):
        return "พิมพ์สรุปไม่ถูกฟอร์แมต ลองแบบ: แจ้งผล 390 / แจ้งผล ชตย 370 / แจ้งผล ช่างไม่ตี 370 / แจ้งผล บั้งไฟหาย / รส 390 / รต 255"

    # ---------- ต้องมีรอบก่อน ----------
    r = must_round(context_id, thread_root_id)
    round_root_id = thread_root_id
    if not r:
        lr = latest_open_round(context_id, 999999)
        if lr:
            round_root_id, status, base_low, base_high = lr[0], lr[1], lr[2], lr[3]
            r = (status, base_low, base_high)
        else:
            return None

    # ---------- โพสเล่น (ไม่สร้างบิลทันที) ----------
    # ผู้เล่นโพส เช่น: ชล500 / ถ500 / +5ถ500  -> ระบบจะ "สร้างบิล" ตอนมีคนมาติด (ตอบกลับ/Quote)
    play_post = parse_play_post(t) or parse_play_command(t)
    if play_post and (reply_to_message_id is None or reply_to_message_id == message_id):
        status, _, _ = r
        if status != "open":
            return "รอบนี้ไม่ได้อยู่สถานะเปิด"
        # ไม่ต้องตอบในกลุ่ม
        return None

# ---------- ติด/ยืนยัน (รองรับโพสเดียวติดได้หลายคน) ----------
    lock_req = parse_lock_request(text)
    if lock_req:
        status, base_low, base_high = r
        if status != "open":
            return "รอบปิดแล้ว บิลใหม่/การยืนยันหลังปิดจะไม่นับ"

        accept_override_amount = lock_req.get("amount")

        if not reply_to_message_id:
            return "ต้องตอบกลับ (Quote) ข้อความที่ต้องการเท่านั้น — คนรับให้ Quote ที่โพสเล่น, คนโพสให้ Quote ที่ข้อความ 'ต/ติด' ของคู่เล่น"

        with closing(db()) as con:
            con.execute("BEGIN IMMEDIATE")
            try:
                # (A) กรณี 'คนโพส' ยืนยัน:
                #   ✅ ต้อง Quote ที่ข้อความ 'ต/ติด' ของคนรับ (accept_message_id) เท่านั้น
                row = None
                if reply_to_message_id:
                    row = get_bet_by_accept_message_id(con, context_id, reply_to_message_id)

                def _confirm_bet_row(row_):
                    (bet_id, bet_thread_root_id, poster_id, poster_name, opponent_id, opponent_name, bstatus,
                     amount, want_side, pl, ph, play_flag, delta, open_message_id, accept_message_id) = row_
                    opp_side = opposite(want_side)

                    def _activate_current_bet():
                        # เช็คเครดิตทั้งสองฝั่ง
                        if get_available(poster_id) < amount:
                            con.rollback()
                            return f"เจ้าของโพส ({poster_name}) เครดิตไม่พอสำหรับการยืนยันบิลนี้"
                        if get_available(opponent_id) < amount:
                            con.rollback()
                            return f"คู่เล่น ({opponent_name}) เครดิตไม่พอสำหรับบิลนี้"

                        # กันเครดิต + ทำ active
                        con.execute("UPDATE bets SET status='active' WHERE id=?", (bet_id,))
                        add_reserved_tx(con, poster_id, amount)
                        add_reserved_tx(con, opponent_id, amount)
                        con.commit()

                        # คำนวณราคา (ถ้ายังไม่ได้ตีราคาช่าง -> ส่งแบบรอช่างตีราคา)
                        d = int(delta or 0)
                        orig_pl, orig_ph = pl, ph
                        pl2, ph2 = resolve_bet_price_range(base_low, base_high, pl, ph, d)
                        played_text = build_play_label(want_side, orig_pl, orig_ph, d, play_flag)

                        poster_name_disp   = resolve_display_name(context_id, poster_id, poster_name or "")
                        opponent_name_disp = resolve_display_name(context_id, opponent_id, opponent_name or "")

                        flex_dm = flex_bill_confirmed(
                            poster_name_disp, want_side,
                            opponent_name_disp, opp_side,
                            amount, pl2, ph2, bet_id, delta=d, note=None, played_text=played_text
                        )

                        # 🔒 ส่งเข้าหลังบ้าน (DM) ให้ทั้งสองคน (best-effort)
                        if line_bot_api:
                            try:
                                line_bot_api.push_message(poster_id, flex_dm)
                            except Exception:
                                pass
                            try:
                                line_bot_api.push_message(opponent_id, flex_dm)
                            except Exception:
                                pass

                        # แจ้งเฉพาะหลังบ้าน (DM) เท่านั้น — ไม่ประกาศหน้ากลุ่ม
                        return None

                    if bstatus == "waiting_confirm":
                        if user_id == poster_id:
                            # คนโพสเปลี่ยนยอดจากไม้เดิม -> ส่งเป็น counter แล้วรอคู่เล่นตอบ "ติด" ซ้ำ
                            if accept_override_amount is not None:
                                new_amount = int(accept_override_amount)
                                if new_amount <= 0:
                                    con.rollback()
                                    return "จำนวนที่ยืนยันต้องมากกว่า 0"
                                if new_amount > amount:
                                    con.rollback()
                                    return f"ยืนยันเกินยอดเดิมไม่ได้ — โพสนี้รับได้สูงสุด {amount}"
                                if new_amount < amount:
                                    con.execute(
                                        "UPDATE bets SET amount=?, status='waiting_accept', accept_message_id=? WHERE id=?",
                                        (new_amount, message_id, bet_id)
                                    )
                                    con.commit()
                                    return None
                            return _activate_current_bet()

                        con.rollback()
                        return f"รอยืนยันโดย {poster_name} เท่านั้น"

                    if bstatus == "waiting_accept":
                        if user_id == opponent_id:
                            if accept_override_amount is not None and int(accept_override_amount) != amount:
                                con.rollback()
                                return f"โพสนี้รอยืนยันยอด {amount} เท่านั้น — ให้ตอบ 'ติด' เพื่อยืนยัน"
                            return _activate_current_bet()
                        if user_id == poster_id:
                            con.rollback()
                            return f"รอ {opponent_name} ตอบ 'ติด' เพื่อยืนยันยอด {amount}"
                        con.rollback()
                        return f"รอ {opponent_name} ยืนยันยอดนี้เท่านั้น"

                    if bstatus == "active":
                        con.rollback()
                        return "บิลนี้ยืนยันแล้ว"

                    con.rollback()
                    return "สถานะบิลไม่พร้อมสำหรับการยืนยัน"

                # 1) ยืนยันแบบปกติ (quote ที่ข้อความ 'ต' ของคนรับ)
                if row:
                    out = _confirm_bet_row(row)
                    return out

                # ถ้าเป็นคนโพส แต่ไม่ได้ Quote ที่ข้อความ 'ต/ติด' ของคู่เล่นโดยตรง -> ไม่ให้เดา
                q = get_message_by_id(con, context_id, reply_to_message_id)
                if q and user_id == q[1]:
                    pending_rows = get_waiting_confirms_by_open_message_id(con, context_id, reply_to_message_id)
                    if pending_rows:
                        con.rollback()
                        return "การยืนยันต้อง Quote ที่ข้อความ 'ต/ติด' ของคู่เล่นเท่านั้น — ห้าม Quote ที่โพสต้นทาง"

                # (B) กรณี 'คนรับ' ติด: Quote ที่โพสเล่นของคนโพส (open_message_id)
                if not q:
                    con.rollback()
                    return "ไม่พบโพสที่อ้างอิง (อาจเป็นข้อความเก่ามาก หรือยังไม่ถูกบันทึก)"

                _, poster_id0, poster_name0, open_text, _open_created_at = q
                play0 = parse_play_post(open_text) or parse_play_command(open_text)
                if not play0:
                    con.rollback()
                    return "โพสที่อ้างอิงไม่ใช่คำสั่งเล่น (ตัวอย่าง: ชล500, ถ500, +5ถ500)"

                if user_id == poster_id0:
                    con.rollback()
                    return "ถ้าจะยืนยัน ให้ตอบ 'ต' หลังจากมีคนมาติดโพสนี้แล้ว"

                amount = int(accept_override_amount or play0["amount"])
                want_side = play0["side"]
                pl = play0.get("pl")
                ph = play0.get("ph")
                delta = int(play0.get("delta") or 0)
                play_flag = play0.get("play_flag")
                opp_side = opposite(want_side)

                if get_available(user_id) < amount:
                    con.rollback()
                    return "เครดิตไม่พอสำหรับการติดบิลนี้"

                # สร้างบิลใหม่ (1 โพสเล่น ติดได้หลายคน และคนเดิมติดซ้ำได้)
                opp_name_resolved = resolve_display_name(context_id, user_id, user_name)
                poster_name_resolved = resolve_display_name(context_id, poster_id0, poster_name0)

                con.execute("""
                    INSERT INTO bets(
                        context_id, thread_root_id,
                        poster_id, poster_name,
                        opponent_id, opponent_name,
                        amount, want_side,
                        price_low, price_high, play_flag,
                        status, created_at,
                        open_message_id, accept_message_id, delta
                    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """, (
                    context_id, round_root_id,
                    poster_id0, poster_name_resolved,
                    user_id, opp_name_resolved,
                    amount, want_side,
                    pl, ph, play_flag,
                    "waiting_confirm", now,
                    reply_to_message_id, message_id, delta
                ))

                con.commit()
                # ไม่ตอบในกลุ่ม (ปล่อยให้เห็นข้อความ 'ติด' ของคนรับตามธรรมชาติ)
                return None

            except:
                con.rollback()
                raise

    return None



# -------------------- Cancel (peer confirm) --------------------
def _bet_price_text(base_low, base_high, pl, ph, delta):
    d = int(delta or 0)
    pl2, ph2 = resolve_bet_price_range(base_low, base_high, pl, ph, d)
    if pl2 is None or ph2 is None:
        return "รอช่างตีราคา" + (f" ({d:+d})" if d else "")
    return format_price_value(pl2, ph2) + (f" ({d:+d})" if d else "")

def request_cancel_bet(bet_id: int, requester_id: str):
    """
    ผู้เล่นกดปุ่ม 'ขอยกเลิก' -> ตั้งสถานะ pending และส่งคำขอไปอีกฝ่าย
    """
    now = now_utc_str()
    with closing(db()) as con:
        con.execute("BEGIN IMMEDIATE")
        try:
            row = con.execute("""
                SELECT id, context_id, thread_root_id, poster_id, poster_name,
                       opponent_id, opponent_name, amount, want_side,
                       price_low, price_high, delta, status, cancel_status,
                       base_low, base_high
                FROM (
                    SELECT b.*, r.base_low, r.base_high
                    FROM bets b
                    LEFT JOIN rounds r
                      ON r.context_id = b.context_id AND r.thread_root_id = b.thread_root_id
                )
                WHERE id=?
            """, (bet_id,)).fetchone()

            if not row:
                con.rollback()
                return {"ok": False, "msg": "ไม่พบ Order นี้"}

            (bid, ctx, root, poster_id, poster_name,
             opp_id, opp_name, amount, want_side,
             pl, ph, delta, st, cst, bl, bh) = row

            if st != "active":
                con.rollback()
                return {"ok": False, "msg": "Order นี้ไม่ได้อยู่สถานะที่ยกเลิกได้แล้ว"}

            if requester_id not in (poster_id, opp_id):
                con.rollback()
                return {"ok": False, "msg": "คุณไม่ใช่ผู้เล่นของ Order นี้"}

            if cst == "pending":
                con.rollback()
                return {"ok": False, "msg": "มีคำขอยกเลิกค้างอยู่แล้ว"}

            other_id = opp_id if requester_id == poster_id else poster_id
            if not other_id:
                con.rollback()
                return {"ok": False, "msg": "ไม่พบคู่เล่นของ Order นี้"}

            con.execute("""
                UPDATE bets
                SET cancel_status='pending', cancel_requester_id=?, cancel_requested_at=?
                WHERE id=?
            """, (requester_id, now, bet_id))

            con.commit()

            requester_name = poster_name if requester_id == poster_id else opp_name
            price_text = _bet_price_text(bl, bh, pl, ph, delta)
            return {
                "ok": True,
                "other_id": other_id,
                "requester_name": requester_name or (requester_id[-6:] if requester_id else "-"),
                "amount": int(amount or 0),
                "price_text": price_text,
            }
        except:
            con.rollback()
            raise

def respond_cancel_bet(bet_id: int, responder_id: str, accept: bool):
    """
    อีกฝ่ายกด 'ยืนยันยกเลิก' หรือ 'ปฏิเสธ'
    """
    now = now_utc_str()
    with closing(db()) as con:
        con.execute("BEGIN IMMEDIATE")
        try:
            row = con.execute("""
                SELECT id, poster_id, opponent_id, amount, status,
                       cancel_status, cancel_requester_id
                FROM bets WHERE id=?
            """, (bet_id,)).fetchone()

            if not row:
                con.rollback()
                return {"ok": False, "msg": "ไม่พบ Order นี้"}

            bid, poster_id, opp_id, amount, st, cst, req_id = row

            if st != "active" or cst != "pending":
                con.rollback()
                return {"ok": False, "msg": "Order นี้ไม่มีคำขอยกเลิกค้างอยู่"}

            if responder_id not in (poster_id, opp_id):
                con.rollback()
                return {"ok": False, "msg": "คุณไม่ใช่ผู้เล่นของ Order นี้"}

            if responder_id == req_id:
                con.rollback()
                return {"ok": False, "msg": "ผู้ขอยกเลิกไม่สามารถกดยืนยันฝั่งตัวเองได้"}

            other_id = req_id
            amount = int(amount or 0)

            if accept:
                # คืนเครดิตกันไว้ (reserved)
                add_reserved_tx(con, poster_id, -amount)
                add_reserved_tx(con, opp_id, -amount)

                con.execute("""
                    UPDATE bets
                    SET status='settled', settled_at=?, result='void', fee=0,
                        cancel_status='approved', cancel_responder_id=?, cancel_resolved_at=?
                    WHERE id=?
                """, (now, responder_id, now, bet_id))
                con.commit()
                return {"ok": True, "accepted": True, "poster_id": poster_id, "opp_id": opp_id, "amount": amount}
            else:
                con.execute("""
                    UPDATE bets
                    SET cancel_status='rejected', cancel_responder_id=?, cancel_resolved_at=?
                    WHERE id=?
                """, (responder_id, now, bet_id))
                con.commit()
                return {"ok": True, "accepted": False, "requester_id": other_id, "responder_id": responder_id, "amount": amount}
        except:
            con.rollback()
            raise

# -------------------- LINE Webhook --------------------
@app.route("/callback", methods=['POST'])
def callback():
    if not handler: abort(500, "LINE handler is not configured")
    signature = request.headers.get('X-Line-Signature', '')
    body = request.get_data(as_text=True)
    cache_raw_quote_ids_from_body(body)
    try:
        handler.handle(body, signature)
    except InvalidSignatureError:
        abort(400)
    return "OK"


def extract_quoted_message_id(event) -> str | None:
    """ดึง quotedMessageId ให้ได้แม้ SDK รุ่นเก่าจะไม่ map field นี้"""
    msg_id = str(getattr(event.message, "id", "") or "")

    # 1) attribute ตรง ๆ (กรณี SDK รองรับ)
    q = getattr(event.message, "quotedMessageId", None) or getattr(event.message, "quoted_message_id", None)
    if q:
        return str(q)

    # 2) จาก raw webhook body ที่ cache ไว้ก่อนเข้า SDK
    q = get_cached_raw_quote_id(msg_id)
    if q:
        return str(q)

    # 3) จาก raw event JSON (บางเวอร์ชันเก็บ field ไว้ใน as_json_dict)
    try:
        d = event.as_json_dict()
        q = (d.get("message") or {}).get("quotedMessageId") or (d.get("message") or {}).get("quoted_message_id")
        if q:
            return str(q)
    except Exception:
        pass

    # 4) จาก raw message JSON ถ้ามี
    try:
        d = event.message.as_json_dict()
        q = d.get("quotedMessageId") or d.get("quoted_message_id")
        if q:
            return str(q)
    except Exception:
        pass
    return None

@handler.add(MessageEvent, message=TextMessage)
def handle_message(event: MessageEvent):
    group_id = getattr(event.source, "group_id", None)
    room_id = getattr(event.source, "room_id", None)
    user_id = getattr(event.source, "user_id", None)

    context_id = group_id or room_id or user_id or "unknown"
    uid = user_id or "unknown"
    uname = resolve_display_name(context_id, uid, get_stored_user_name(uid) or uid[-6:])

    text = (event.message.text or "").strip()
    message_id = str(event.message.id)
    quoted_id = extract_quoted_message_id(event)
    thread_root_id = quoted_id if quoted_id else message_id
    reply_to_message_id = quoted_id

    try:
        reply = core_handle(context_id, uid, uname, text, message_id, thread_root_id, reply_to_message_id)
    except Exception as e:
        print("ERROR in core_handle:", e)
        traceback.print_exc()
        reply = "❌เกิดข้อผิดพลาดภายในระบบ❌"

    if reply:
        try:
            if isinstance(reply, (TextSendMessage, FlexSendMessage)):
                line_bot_api.reply_message(event.reply_token, reply)
            elif isinstance(reply, list):
                line_bot_api.reply_message(event.reply_token, reply)
            elif isinstance(reply, str):
                line_bot_api.reply_message(event.reply_token, TextSendMessage(text=reply))
            else:
                line_bot_api.reply_message(event.reply_token, TextSendMessage(text=str(reply)))
        except Exception:
            to = getattr(event.source, "user_id", None) or getattr(event.source, "group_id", None) or getattr(event.source, "room_id", None)
            if to:
                if isinstance(reply, (TextSendMessage, FlexSendMessage)):
                    line_bot_api.push_message(to, reply)
                elif isinstance(reply, str):
                    line_bot_api.push_message(to, TextSendMessage(text=reply))
                elif isinstance(reply, list):
                    line_bot_api.push_message(to, reply)
                else:
                    line_bot_api.push_message(to, TextSendMessage(text=str(reply)))


@handler.add(FollowEvent)
def handle_follow(event: FollowEvent):
    user_id = getattr(event.source, "user_id", None)
    if not user_id or not line_bot_api:
        return

    context_id = user_id
    display_name = resolve_display_name(context_id, user_id, "ลูกค้า", force_remote=True)
    upsert_user(user_id, display_name)
    uid_code = get_or_assign_idx(user_id)
    flex = make_welcome_flex(display_name, uid_code)

    try:
        line_bot_api.reply_message(event.reply_token, flex)
    except Exception:
        try:
            line_bot_api.push_message(user_id, flex)
        except Exception:
            pass


@handler.add(PostbackEvent)
def handle_postback(event: PostbackEvent):
    """
    รับปุ่มจาก Flex:
    - act=req_cancel&bet_id=...
    - act=cancel_confirm&bet_id=...
    - act=cancel_reject&bet_id=...
    """
    if not line_bot_api:
        return

    uid = getattr(event.source, "user_id", None) or "unknown"
    data = getattr(event.postback, "data", "") or ""
    qs = parse_qs(data)
    act = (qs.get("act", [""])[0] or "").strip()
    bet_id_s = (qs.get("bet_id", ["0"])[0] or "0").strip()
    try:
        bet_id = int(bet_id_s)
    except Exception:
        bet_id = 0

    def push_private(to_uid: str, message):
        try:
            line_bot_api.push_message(to_uid, message)
        except Exception:
            pass

    def reply_text(msg: str):
        # แจ้งเฉพาะแชทส่วนตัวของผู้กดปุ่ม ไม่ตอบกลับหน้ากลุ่ม/ห้อง
        push_private(uid, TextSendMessage(text=msg))

    if not act or bet_id <= 0:
        reply_text("คำสั่งไม่ถูกต้อง")
        return

    # 1) ขอ ยกเลิก -> ส่งคำขอไปอีกฝ่าย
    if act == "req_cancel":
        try:
            res = request_cancel_bet(bet_id, uid)
        except Exception:
            reply_text("❌เกิดข้อผิดพลาดภายในระบบ❌")
            return

        if not res.get("ok"):
            reply_text(res.get("msg") or "ทำรายการไม่สำเร็จ")
            return

        other_id = res["other_id"]
        flex = flex_cancel_request(bet_id, res["amount"], res["price_text"], res["requester_name"])

        # ตอบคนกดปุ่มก่อน
        reply_text(f"ส่งคำขอยกเลิก Order #{bet_id} แล้ว รออีกฝ่ายยืนยัน")

        # ส่งคำขอไปอีกฝ่าย (DM)
        try:
            line_bot_api.push_message(other_id, flex)
        except Exception:
            pass
        return

    # 2) อีกฝ่ายยืนยัน/ปฏิเสธ
    if act in ("cancel_confirm", "cancel_reject"):
        accept = (act == "cancel_confirm")
        try:
            res = respond_cancel_bet(bet_id, uid, accept=accept)
        except Exception:
            reply_text("❌เกิดข้อผิดพลาดภายในระบบ❌")
            return

        if not res.get("ok"):
            reply_text(res.get("msg") or "ทำรายการไม่สำเร็จ")
            return

        if res.get("accepted"):
            # ยกเลิกสำเร็จ -> แจ้งทั้งคู่
            done = flex_cancel_done(bet_id, res["amount"])
            push_private(res["poster_id"], done)
            push_private(res["opp_id"], done)
            return
        else:
            # ปฏิเสธ -> แจ้งผู้ขอ
            reply_text("คุณปฏิเสธคำขอยกเลิกแล้ว")
            try:
                line_bot_api.push_message(res["requester_id"], TextSendMessage(text=f"คำขอยกเลิก Order #{bet_id} ถูกปฏิเสธ"))
            except Exception:
                pass
            return

# -------------------- Legacy webhook for curl test --------------------
@app.route("/webhook", methods=["POST"])
def legacy_webhook():
    data = request.get_json(force=True)
    for k in ["context_id","user_id","user_name","text","message_id","thread_root_id"]:
        if k not in data: return jsonify({"ok":False,"msg":"bad payload"}), 400
    reply = core_handle(
        data["context_id"],
        data["user_id"], data.get("user_name") or data["user_id"],
        (data.get("text") or "").strip(),
        str(data["message_id"]), str(data["thread_root_id"]),
        data.get("reply_to_message_id")
    )

    if isinstance(reply, (TextSendMessage, FlexSendMessage)):
        return jsonify({"ok":True,"reply":"(Flex message)"}), 200
    elif isinstance(reply, list):
        return jsonify({"ok":True,"reply":"(Multiple messages)"}), 200
    else:
        return jsonify({"ok":True,"reply":reply}), 200

@app.route("/healthz")
def healthz(): return "ok"

if __name__ == "__main__":
    init_db()
    app.run(host="0.0.0.0", port=5000, debug=os.environ.get("FLASK_DEBUG") == "1", threaded=True, use_reloader=False)
