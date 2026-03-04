
import os, json, time, asyncio, logging, re
from typing import Any, Dict, Optional, List, Tuple

import requests
from flask import Flask
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.error import BadRequest, Forbidden, Conflict
from telegram.constants import ParseMode
from telegram.ext import (
    Application, ApplicationBuilder,
    CommandHandler, CallbackQueryHandler,
    MessageHandler, ChatMemberHandler, ContextTypes, filters
)

# ============================================================
#  PumpTools/Solana/Pump.fun BuyBot (public, polling-based)
#  - Posts buys in groups AND your trending channel
#  - Group setup wizard via buttons (like reference)
#  - /trending + /ads booking menus (manual confirm supported)
#  - Leaderboard message (paid slots + organic rolling buys)
# ============================================================

# -------------------- LOGGING --------------------
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
log = logging.getLogger("pumptools_buybot")

# -------------------- ENV --------------------
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
if not BOT_TOKEN:
    raise SystemExit("BOT_TOKEN is required")

DATA_DIR = os.getenv("DATA_DIR", ".").strip()

OWNER_IDS = {int(x) for x in re.split(r"[,\s]+", os.getenv("OWNER_IDS", "").strip()) if x.strip().isdigit()}
TRENDING_CHANNEL_ID = int(os.getenv("TRENDING_CHANNEL_ID", "0") or "0")  # optional
TRENDING_CHANNEL_HANDLE = os.getenv("TRENDING_CHANNEL_HANDLE", "").strip()  # e.g. @PumpToolsTrending (optional)

THOR_BUY_URL_PREFIX = os.getenv("THOR_BUY_URL_PREFIX", "https://t.me/ThorSolana_bot?start=r-TBw15MO-buy-").strip()

PAY_WALLET = os.getenv("PAY_WALLET", "").strip()  # SOL address for bookings (string shown to users)
POLL_INTERVAL = max(2.0, float(os.getenv("POLL_INTERVAL", "3.0")))  # seconds

# Pump.fun frontend API (v3 is commonly used; fallback to classic)
PUMPFUN_API_BASES = [x.strip().rstrip("/") for x in re.split(r"[,\s]+", os.getenv(
    "PUMPFUN_API_BASES",
    "https://frontend-api-v3.pump.fun https://frontend-api.pump.fun"
)) if x.strip()]

# Booking defaults (SOL)
TOP3_PRICE_SOL = float(os.getenv("TOP3_PRICE_SOL", "0.05"))
TOP10_PRICE_SOL = float(os.getenv("TOP10_PRICE_SOL", "0.03"))
ADS_PRICE_SOL = float(os.getenv("ADS_PRICE_SOL", "0.02"))

# durations in hours
TOP3_HOURS = int(float(os.getenv("TOP3_HOURS", "24")))
TOP10_HOURS = int(float(os.getenv("TOP10_HOURS", "24")))
ADS_HOURS = int(float(os.getenv("ADS_HOURS", "24")))

# Leaderboard (organic rolling window in hours)
LEADERBOARD_WINDOW_HOURS = int(float(os.getenv("LEADERBOARD_WINDOW_HOURS", "6")))
LEADERBOARD_TOP_N = int(float(os.getenv("LEADERBOARD_TOP_N", "10")))

LEADERBOARD_HEADER_HANDLE = os.getenv("LEADERBOARD_HEADER_HANDLE", "@Spytontrending").strip()  # keep your preferred header
LEADERBOARD_TITLE = os.getenv("LEADERBOARD_TITLE", "🔥 Trending Leaderboard").strip()

# -------------------- FILES --------------------
def _data_path(name: str) -> str:
    os.makedirs(DATA_DIR, exist_ok=True)
    return os.path.join(DATA_DIR, name)

TOKENS_FILE = _data_path(os.getenv("TOKENS_FILE", "tokens_public.json"))
GROUPS_FILE = _data_path(os.getenv("GROUPS_FILE", "groups_public.json"))
SEEN_FILE = _data_path(os.getenv("SEEN_FILE", "seen_public.json"))
BOOKINGS_FILE = _data_path(os.getenv("BOOKINGS_FILE", "bookings_public.json"))
LEADERBOARD_FILE = _data_path(os.getenv("LEADERBOARD_FILE", "leaderboard_public.json"))
LEADERBOARD_MSG_FILE = _data_path(os.getenv("LEADERBOARD_MSG_FILE", "leaderboard_msg.json"))
AD_SLOTS_FILE = _data_path(os.getenv("AD_SLOTS_FILE", "ads_public.json"))

# -------------------- SMALL UTIL --------------------
def is_owner(user_id: int) -> bool:
    return (user_id in OWNER_IDS) if OWNER_IDS else False

def now_ts() -> int:
    return int(time.time())

def load_json(path: str, default):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default

def save_json(path: str, obj) -> None:
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)

def short_addr(addr: str, n: int = 4) -> str:
    if not addr:
        return ""
    if len(addr) <= (n*2 + 3):
        return addr
    return f"{addr[:n]}...{addr[-n:]}"

def fmt_usd(v: Optional[float], decimals: int = 0) -> Optional[str]:
    if v is None:
        return None
    try:
        if decimals <= 0:
            return f"${v:,.0f}"
        return f"${v:,.{decimals}f}"
    except Exception:
        return None

def fmt_num(v: Optional[float], decimals: int = 2) -> str:
    if v is None:
        return "—"
    try:
        return f"{v:,.{decimals}f}"
    except Exception:
        return "—"

def fmt_token_amount(x: float) -> str:
    # similar to many buybots: abbreviate
    absx = abs(x)
    if absx >= 1_000_000_000:
        return f"{x/1_000_000_000:.2f}B"
    if absx >= 1_000_000:
        return f"{x/1_000_000:.2f}M"
    if absx >= 1_000:
        return f"{x/1_000:.2f}K"
    if absx >= 1:
        return f"{x:,.2f}"
    return f"{x:.6f}".rstrip("0").rstrip(".")

def solscan_tx(sig: str) -> str:
    return f"https://solscan.io/tx/{sig}"

def solscan_account(addr: str) -> str:
    return f"https://solscan.io/account/{addr}"

def pumpfun_coin_url(mint: str) -> str:
    return f"https://pump.fun/coin/{mint}"

# -------------------- STATE --------------------
TOKENS: Dict[str, Dict[str, Any]] = {}         # mint -> token meta
GROUPS: Dict[str, Dict[str, Any]] = {}         # chat_id str -> settings
SEEN: Dict[str, Dict[str, Any]] = {}           # scope key -> last seen per mint
BOOKINGS: Dict[str, Any] = {}                  # bookings + ads bookings
LEADERBOARD: Dict[str, Any] = {}               # organic stats
AD_SLOTS: Dict[str, Any] = {}                  # current ad slots (text + link + expires)

# -------------------- PUMPFUN API --------------------
SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "pumptools-buybot/1.0"})

def pumpfun_get(path: str, params: Optional[dict]=None, timeout: float=10.0) -> Optional[Any]:
    for base in PUMPFUN_API_BASES:
        url = f"{base}{path}"
        try:
            r = SESSION.get(url, params=params or {}, timeout=timeout)
            if r.status_code == 200:
                return r.json()
        except Exception:
            continue
    return None

def get_coin_meta(mint: str) -> Optional[Dict[str, Any]]:
    # /coins/{mint}?sync=true|false
    data = pumpfun_get(f"/coins/{mint}", params={"sync": "true"})
    if isinstance(data, dict) and data.get("mint"):
        return data
    return None

def get_trades(mint: str, limit: int = 50) -> List[Dict[str, Any]]:
    data = pumpfun_get(f"/trades/all/{mint}", params={"limit": str(limit), "offset": "0", "minimumSize": "0"})
    if isinstance(data, list):
        return data
    return []

def get_sol_price() -> Optional[float]:
    data = pumpfun_get("/sol-price")
    # try common shapes
    if isinstance(data, dict):
        for k in ("solPrice", "price", "sol_price", "usd"):
            if k in data:
                try:
                    return float(data[k])
                except Exception:
                    pass
    return None

def parse_trade(t: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Flexible parser for pump.fun trades.
    We only need: signature, timestamp, is_buy, buyer, sol_amount, token_amount
    """
    if not isinstance(t, dict):
        return None

    # signature / tx id
    sig = (t.get("signature") or t.get("txSignature") or t.get("tx_hash") or t.get("txHash") or t.get("hash") or "")
    sig = str(sig)
    if not sig:
        return None

    ts = t.get("timestamp") or t.get("ts") or t.get("blockTime") or t.get("time")
    try:
        ts = int(float(ts))
    except Exception:
        ts = now_ts()

    # buyer / maker / trader
    buyer = (t.get("trader") or t.get("user") or t.get("buyer") or t.get("owner") or t.get("maker") or "")
    buyer = str(buyer)

    # is buy?
    is_buy = None
    for k in ("isBuy", "is_buy", "buy", "is_buy_trade"):
        if k in t:
            try:
                is_buy = bool(t[k])
            except Exception:
                pass
    if is_buy is None:
        side = (t.get("side") or t.get("type") or t.get("direction") or "").lower()
        if side in ("buy", "b"):
            is_buy = True
        elif side in ("sell", "s"):
            is_buy = False
    if is_buy is None:
        # heuristic: sol amount positive often means spent SOL
        pass

    # amounts (SOL spent, token got)
    sol_amt = None
    for k in ("solAmount", "sol_amount", "sol", "solSpent", "sol_spent", "amountSol", "amount_sol"):
        if k in t:
            try:
                sol_amt = float(t[k])
                break
            except Exception:
                continue

    tok_amt = None
    for k in ("tokenAmount", "token_amount", "tokens", "amountToken", "amount_token", "tokenOut", "token_out"):
        if k in t:
            try:
                tok_amt = float(t[k])
                break
            except Exception:
                continue

    # If direction unknown and sol_amt exists:
    if is_buy is None and sol_amt is not None:
        is_buy = sol_amt > 0

    return {
        "sig": sig,
        "ts": ts,
        "buyer": buyer,
        "is_buy": bool(is_buy),
        "sol": float(sol_amt or 0.0),
        "tok": float(tok_amt or 0.0),
    }

# -------------------- LEADERBOARD (organic) --------------------
def lb_record_buy(mint: str, sol_amt: float):
    if sol_amt <= 0:
        return
    window_sec = max(60, LEADERBOARD_WINDOW_HOURS * 3600)
    lb = LEADERBOARD.setdefault("buys", {})
    arr = lb.setdefault(mint, [])
    arr.append([now_ts(), float(sol_amt)])
    # prune
    cutoff = now_ts() - window_sec
    while arr and int(arr[0][0]) < cutoff:
        arr.pop(0)

def lb_get_top() -> List[Tuple[str, float]]:
    window_sec = max(60, LEADERBOARD_WINDOW_HOURS * 3600)
    cutoff = now_ts() - window_sec
    out = []
    for mint, arr in (LEADERBOARD.get("buys") or {}).items():
        s = 0.0
        for ts, amt in arr:
            if int(ts) >= cutoff:
                s += float(amt)
        if s > 0:
            out.append((mint, s))
    out.sort(key=lambda x: x[1], reverse=True)
    return out[:LEADERBOARD_TOP_N]

# -------------------- ADS (under buys) --------------------
def get_active_ad_text() -> Optional[str]:
    # prefer "global" ad slot if valid
    slots = AD_SLOTS.get("slots") or []
    now = now_ts()
    for s in slots:
        if int(s.get("expires", 0)) > now and (s.get("text") or "").strip():
            text = (s.get("text") or "").strip()
            url = (s.get("url") or "").strip()
            if url:
                return f'\n\n📢 <a href="{url}">{text}</a>'
            return f"\n\n📢 {text}"
    return None

# -------------------- MESSAGE BUILDERS --------------------
def build_buy_message(token: Dict[str, Any], trade: Dict[str, Any], for_channel: bool) -> Tuple[str, InlineKeyboardMarkup]:
    mint = token["mint"]
    name = (token.get("name") or token.get("symbol") or "TOKEN").strip()
    sym = (token.get("symbol") or "").strip()
    title = sym or name

    # clickable header to TG if exists, else pump.fun coin page
    tg = (token.get("telegram") or "").strip()
    header_url = tg if tg else pumpfun_coin_url(mint)

    sol_spent = float(trade.get("sol") or 0.0)
    tok_got = float(trade.get("tok") or 0.0)

    buyer = trade.get("buyer") or ""
    buyer_short = short_addr(str(buyer))

    sig = trade.get("sig") or ""
    tx_url = solscan_tx(sig)

    # Market stats (best-effort)
    price = token.get("price_usd")
    liq = token.get("liq_usd")
    mc = token.get("mc_usd")
    holders = None  # holders intentionally omitted

    lines: List[str] = []
    lines.append(f'<b><a href="{header_url}">{title}</a> Buy!</b>')
    lines.append("")
    lines.append(f"Spent: <b>{sol_spent:,.3f} SOL</b>")
    if tok_got > 0:
        lines.append(f"Got: <b>{fmt_token_amount(tok_got)} {sym or ''}</b>".rstrip())
    lines.append("")
    buyer_line = buyer_short
    if buyer:
        buyer_line = f'<a href="{solscan_account(buyer)}">{buyer_short}</a>'
    buyer_line = f"{buyer_line} | <a href=\"{tx_url}\">Txn</a>"
    lines.append(buyer_line)
    lines.append("")

    price_disp = fmt_usd(float(price), 6) if price is not None else "—"
    liq_disp = fmt_usd(float(liq), 0) if liq is not None else "—"
    mc_disp = fmt_usd(float(mc), 0) if mc is not None else "—"
    lines.append(f"Price: {price_disp}")
    lines.append(f"Liquidity: {liq_disp}")
    lines.append(f"MCap: {mc_disp}")
    # Holders intentionally omitted

    # Links row
    link_parts = [
        f'<a href="{tx_url}">TX</a>',
        f'<a href="{pumpfun_coin_url(mint)}">Pump</a>',
    ]
    if tg:
        link_parts.append(f'<a href="{tg}">Telegram</a>')
    if TRENDING_CHANNEL_HANDLE:
        link_parts.append(f'<a href="https://t.me/{TRENDING_CHANNEL_HANDLE.lstrip("@")}">Trending</a>')
    lines.append(" | ".join(link_parts))

    ad = get_active_ad_text()
    if ad:
        lines.append(ad.strip("\n"))

    text = "\n".join(lines)

    # Buttons: single "Buy <TOKEN>" button (ThorSolana)
    buy_url = f"{THOR_BUY_URL_PREFIX}{mint}"
    kb = InlineKeyboardMarkup([[InlineKeyboardButton(f"Buy {title}", url=buy_url)]])
    return text, kb

# -------------------- GROUP SETUP WIZARD --------------------
def group_settings(chat_id: int) -> Dict[str, Any]:
    cid = str(chat_id)
    g = GROUPS.get(cid) or {}
    GROUPS[cid] = g
    g.setdefault("enabled", True)
    g.setdefault("mint", "")
    g.setdefault("min_buy_sol", 0.0)
    g.setdefault("post_to_trending", True)
    g.setdefault("trending_channel_id", TRENDING_CHANNEL_ID if TRENDING_CHANNEL_ID else 0)
    g.setdefault("last_setup_ts", 0)
    return g

def setup_keyboard(g: Dict[str, Any]) -> InlineKeyboardMarkup:
    enabled = "✅ ON" if g.get("enabled") else "❌ OFF"
    post_t = "✅ Yes" if g.get("post_to_trending") else "❌ No"
    mint = g.get("mint") or "Not set"
    minb = g.get("min_buy_sol", 0.0)
    kb = [
        [InlineKeyboardButton(f"Buy Alerts: {enabled}", callback_data="G_TOGGLE")],
        [InlineKeyboardButton(f"Token Mint: {mint}", callback_data="G_SET_MINT")],
        [InlineKeyboardButton(f"Min Buy: {minb} SOL", callback_data="G_SET_MIN")],
        [InlineKeyboardButton(f"Post to Trending: {post_t}", callback_data="G_TOGGLE_TREND")],
        [InlineKeyboardButton("Done ✅", callback_data="G_DONE")],
        [InlineKeyboardButton("Book Trending", callback_data="G_BOOK_TRENDING"),
         InlineKeyboardButton("Book Ads", callback_data="G_BOOK_ADS")],
    ]
    return InlineKeyboardMarkup(kb)

async def show_setup(update: Update, context: ContextTypes.DEFAULT_TYPE, note: str=""):
    chat_id = update.effective_chat.id
    g = group_settings(chat_id)
    text = (
        "🎩 <b>PumpTools Buy Bot Setup</b>\n\n"
        f"Group: <code>{chat_id}</code>\n"
        f"Token Mint: <code>{g.get('mint') or 'Not set'}</code>\n"
        f"Min Buy: <b>{g.get('min_buy_sol', 0.0)} SOL</b>\n"
        f"Post to Trending: <b>{'Yes' if g.get('post_to_trending') else 'No'}</b>\n\n"
        "Use the buttons below to configure."
    )
    if note:
        text = f"{note}\n\n{text}"
    await update.effective_chat.send_message(text, parse_mode=ParseMode.HTML, reply_markup=setup_keyboard(g), disable_web_page_preview=True)

# -------------------- BOOKINGS (manual confirm) --------------------
def add_booking(kind: str, mint: str, chat_id: int, buyer_id: int, hours: int, price: float) -> str:
    bid = f"{kind}_{now_ts()}_{buyer_id}_{mint[:6]}"
    b = {
        "id": bid,
        "kind": kind,              # top3 | top10 | ads
        "mint": mint,
        "chat_id": int(chat_id),
        "buyer_id": int(buyer_id),
        "created": now_ts(),
        "expires": now_ts() + int(hours*3600),
        "price_sol": float(price),
        "status": "PENDING",       # PENDING | ACTIVE | EXPIRED | CANCELLED
    }
    BOOKING_DATA = BOOKINGS.setdefault("bookings", [])
    BOOKING_DATA.append(b)
    return bid

def activate_booking(bid: str) -> bool:
    now = now_ts()
    for b in BOOKINGS.get("bookings") or []:
        if b.get("id") == bid:
            b["status"] = "ACTIVE"
            # extend expires from now (so if paid later it still gets full time)
            duration = int(float(b.get("expires", now)) - int(b.get("created", now)))
            if duration <= 0:
                duration = 24*3600
            b["expires"] = now + duration
            return True
    return False

def cleanup_bookings():
    now = now_ts()
    for b in BOOKINGS.get("bookings") or []:
        if b.get("status") == "ACTIVE" and int(b.get("expires", 0)) <= now:
            b["status"] = "EXPIRED"

def get_active_paid_slots() -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    cleanup_bookings()
    top3 = []
    top10 = []
    for b in BOOKINGS.get("bookings") or []:
        if b.get("status") == "ACTIVE":
            if b.get("kind") == "top3":
                top3.append(b)
            elif b.get("kind") == "top10":
                top10.append(b)
    # keep stable order by expires
    top3.sort(key=lambda x: int(x.get("expires", 0)))
    top10.sort(key=lambda x: int(x.get("expires", 0)))
    return top3, top10

# -------------------- LEADERBOARD MESSAGE --------------------
def build_leaderboard_text() -> str:
    top3, top10 = get_active_paid_slots()
    organic = lb_get_top()

    def mint_to_display(mint: str) -> str:
        t = TOKENS.get(mint) or {}
        sym = (t.get("symbol") or "").strip()
        name = (t.get("name") or "").strip()
        title = sym or name or short_addr(mint)
        tg = (t.get("telegram") or "").strip()
        url = tg if tg else pumpfun_coin_url(mint)
        return f'<a href="{url}">{title}</a>'

    lines = []
    lines.append(f"<b>{LEADERBOARD_TITLE}</b>")
    if LEADERBOARD_HEADER_HANDLE:
        lines.append(f"{LEADERBOARD_HEADER_HANDLE}")
    lines.append("")
    # paid slots
    lines.append("🏆 <b>Paid Top 3</b>")
    if top3:
        for i, b in enumerate(top3[:3], 1):
            lines.append(f"{i}. {mint_to_display(b['mint'])}  ⏳")
    else:
        lines.append("—")
    lines.append("")
    lines.append("📌 <b>Paid Top 10</b>")
    if top10:
        for i, b in enumerate(top10[:10], 1):
            lines.append(f"{i}. {mint_to_display(b['mint'])}  ⏳")
    else:
        lines.append("—")
    lines.append("")
    lines.append(f"🌱 <b>Organic (last {LEADERBOARD_WINDOW_HOURS}h)</b>")
    if organic:
        for i, (mint, vol) in enumerate(organic, 1):
            lines.append(f"{i}. {mint_to_display(mint)}  —  <b>{vol:.2f} SOL</b>")
    else:
        lines.append("—")
    return "\n".join(lines)

async def ensure_leaderboard_message(app: Application) -> None:
    if not TRENDING_CHANNEL_ID:
        return
    meta = load_json(LEADERBOARD_MSG_FILE, {})
    msg_id = meta.get("message_id")
    text = build_leaderboard_text()
    try:
        if msg_id:
            await app.bot.edit_message_text(
                chat_id=TRENDING_CHANNEL_ID, message_id=int(msg_id),
                text=text, parse_mode=ParseMode.HTML,
                disable_web_page_preview=True
            )
        else:
            m = await app.bot.send_message(
                chat_id=TRENDING_CHANNEL_ID,
                text=text, parse_mode=ParseMode.HTML,
                disable_web_page_preview=True
            )
            save_json(LEADERBOARD_MSG_FILE, {"message_id": m.message_id})
    except (BadRequest, Forbidden):
        # if can't edit, try send fresh
        m = await app.bot.send_message(
            chat_id=TRENDING_CHANNEL_ID,
            text=text, parse_mode=ParseMode.HTML,
            disable_web_page_preview=True
        )
        save_json(LEADERBOARD_MSG_FILE, {"message_id": m.message_id})

# -------------------- COMMANDS --------------------
async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.effective_chat:
        return
    if update.effective_chat.type in ("group", "supergroup"):
        await show_setup(update, context)
        return
    txt = (
        "🎩 <b>PumpTools Buy Bot</b>\n\n"
        "Add me to your group as Admin, then I’ll show a setup wizard.\n\n"
        "Commands:\n"
        "/trending - book a trending slot\n"
        "/ads - book buy-alert ads\n\n"
        "Owner:\n"
        "/adset <hours> | <text> | <url>\n"
        "/adclear\n"
        "/confirm <booking_id>\n"
        "/tokens\n"
    )
    await update.effective_chat.send_message(txt, parse_mode=ParseMode.HTML, disable_web_page_preview=True)

async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await start_cmd(update, context)

async def tokens_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # list known tokens
    items = list(TOKENS.values())
    items.sort(key=lambda x: int(x.get("added_ts", 0)), reverse=True)
    lines = ["<b>Tokens</b>"]
    for t in items[:30]:
        mint = t["mint"]
        name = t.get("symbol") or t.get("name") or short_addr(mint)
        lines.append(f"• <code>{mint}</code> — {name}")
    await update.effective_chat.send_message("\n".join(lines), parse_mode=ParseMode.HTML, disable_web_page_preview=True)

async def trending_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # booking menu
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton(f"Top 3 — {TOP3_PRICE_SOL} SOL / {TOP3_HOURS}h", callback_data="BOOK_TOP3")],
        [InlineKeyboardButton(f"Top 10 — {TOP10_PRICE_SOL} SOL / {TOP10_HOURS}h", callback_data="BOOK_TOP10")],
    ])
    txt = (
        "📈 <b>Book Trending</b>\n\n"
        "Choose a package. Then send your token mint.\n"
        f"Pay to: <code>{PAY_WALLET or 'SET PAY_WALLET env'}</code>\n\n"
        "After payment, owner can activate with /confirm <booking_id>."
    )
    await update.effective_chat.send_message(txt, parse_mode=ParseMode.HTML, reply_markup=kb, disable_web_page_preview=True)

async def ads_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton(f"Buy-Alert Ads — {ADS_PRICE_SOL} SOL / {ADS_HOURS}h", callback_data="BOOK_ADS")],
    ])
    txt = (
        "📢 <b>Book Buy-Alert Ads</b>\n\n"
        "Choose package. Then send your ad text.\n"
        f"Pay to: <code>{PAY_WALLET or 'SET PAY_WALLET env'}</code>\n\n"
        "After payment, owner can activate with /confirm <booking_id>.\n"
        "Owner can also set a global ad with /adset."
    )
    await update.effective_chat.send_message(txt, parse_mode=ParseMode.HTML, reply_markup=kb, disable_web_page_preview=True)

async def confirm_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.effective_user or not is_owner(update.effective_user.id):
        return
    if not context.args:
        await update.effective_chat.send_message("Usage: /confirm <booking_id>")
        return
    bid = context.args[0].strip()
    if activate_booking(bid):
        save_all()
        await update.effective_chat.send_message(f"✅ Activated: {bid}")
    else:
        await update.effective_chat.send_message("❌ Booking not found")

async def adset_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Owner-only: /adset 24h | Your ad text | https://yourlink"""
    if not update.effective_user or not is_owner(update.effective_user.id):
        return
    raw = (update.message.text or "")
    raw = raw[len("/adset"):].strip()
    if "|" not in raw:
        await update.effective_chat.send_message("Usage: /adset <hours> | <text> | <url(optional)>")
        return
    parts = [p.strip() for p in raw.split("|")]
    try:
        hours = int(float(parts[0]))
    except Exception:
        hours = ADS_HOURS
    text = parts[1] if len(parts) > 1 else ""
    url = parts[2] if len(parts) > 2 else ""
    if not text:
        await update.effective_chat.send_message("Ad text required.")
        return
    expires = now_ts() + hours*3600
    AD_SLOTS.setdefault("slots", []).insert(0, {"text": text, "url": url, "expires": expires})
    # cap slots
    AD_SLOTS["slots"] = (AD_SLOTS["slots"] or [])[:10]
    save_all()
    await update.effective_chat.send_message("✅ Ad set.")

async def adclear_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.effective_user or not is_owner(update.effective_user.id):
        return
    AD_SLOTS["slots"] = []
    save_all()
    await update.effective_chat.send_message("✅ Ads cleared.")

# -------------------- CALLBACKS --------------------
async def on_button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q:
        return
    await q.answer()
    chat = q.message.chat if q.message else None
    if not chat:
        return
    chat_id = chat.id

    data = q.data or ""

    # booking flows (private or group)
    if data in ("BOOK_TOP3", "BOOK_TOP10", "BOOK_ADS"):
        context.user_data["awaiting"] = data
        if data == "BOOK_ADS":
            await q.message.reply_text("Send your ad text (and optional URL on next line).", disable_web_page_preview=True)
        else:
            await q.message.reply_text("Send your token mint address (Solana mint).", disable_web_page_preview=True)
        return

    # group setup
    g = group_settings(chat_id)

    if data == "G_TOGGLE":
        g["enabled"] = not bool(g.get("enabled"))
        save_all()
        await q.edit_message_reply_markup(reply_markup=setup_keyboard(g))
        return

    if data == "G_TOGGLE_TREND":
        g["post_to_trending"] = not bool(g.get("post_to_trending"))
        save_all()
        await q.edit_message_reply_markup(reply_markup=setup_keyboard(g))
        return

    if data == "G_SET_MINT":
        context.chat_data["awaiting_group_mint"] = True
        await q.message.reply_text("Send the token mint address.", disable_web_page_preview=True)
        return

    if data == "G_SET_MIN":
        context.chat_data["awaiting_group_min"] = True
        await q.message.reply_text("Send min buy in SOL (example: 0.05).", disable_web_page_preview=True)
        return

    if data == "G_DONE":
        g["last_setup_ts"] = now_ts()
        save_all()
        await q.message.reply_text("✅ Setup saved. I will start posting buys.", disable_web_page_preview=True)
        return

    if data == "G_BOOK_TRENDING":
        await trending_cmd(update, context)
        return

    if data == "G_BOOK_ADS":
        await ads_cmd(update, context)
        return

# -------------------- TEXT INPUT --------------------
_SOL_MINT_RE = re.compile(r"^[1-9A-HJ-NP-Za-km-z]{32,44}$")

async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.effective_chat or not update.effective_user:
        return
    text = (update.message.text or "").strip()

    # group awaiting mint/min
    if context.chat_data.get("awaiting_group_mint"):
        context.chat_data["awaiting_group_mint"] = False
        if not _SOL_MINT_RE.match(text):
            await update.message.reply_text("Invalid mint. Try again.")
            return
        # fetch meta
        meta = get_coin_meta(text)
        if not meta:
            await update.message.reply_text("I couldn't find that mint on Pump.fun API. Still saved, but buy tracking may not work.")
            meta = {"mint": text}
        token = normalize_coin_meta(meta)
        TOKENS[token["mint"]] = token
        g = group_settings(update.effective_chat.id)
        g["mint"] = token["mint"]
        save_all()
        await update.message.reply_text("✅ Mint saved.")
        return

    if context.chat_data.get("awaiting_group_min"):
        context.chat_data["awaiting_group_min"] = False
        try:
            v = float(text)
        except Exception:
            await update.message.reply_text("Invalid number.")
            return
        g = group_settings(update.effective_chat.id)
        g["min_buy_sol"] = max(0.0, v)
        save_all()
        await update.message.reply_text("✅ Min buy saved.")
        return

    # user booking flow
    awaiting = context.user_data.get("awaiting")
    if awaiting in ("BOOK_TOP3", "BOOK_TOP10"):
        if not _SOL_MINT_RE.match(text):
            await update.message.reply_text("Invalid mint. Send a Solana mint address.")
            return
        meta = get_coin_meta(text)
        if not meta:
            await update.message.reply_text("Mint not found on Pump.fun API. Booking saved anyway.")
            meta = {"mint": text}
        token = normalize_coin_meta(meta)
        TOKENS[token["mint"]] = token

        if awaiting == "BOOK_TOP3":
            bid = add_booking("top3", token["mint"], update.effective_chat.id, update.effective_user.id, TOP3_HOURS, TOP3_PRICE_SOL)
            price = TOP3_PRICE_SOL
        else:
            bid = add_booking("top10", token["mint"], update.effective_chat.id, update.effective_user.id, TOP10_HOURS, TOP10_PRICE_SOL)
            price = TOP10_PRICE_SOL
        context.user_data["awaiting"] = None
        save_all()
        await update.message.reply_text(
            f"✅ Booking created.\n\n"
            f"ID: <code>{bid}</code>\n"
            f"Amount: <b>{price} SOL</b>\n"
            f"Pay to: <code>{PAY_WALLET or 'SET PAY_WALLET env'}</code>\n\n"
            f"After payment, owner activates with /confirm <code>{bid}</code>.",
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True
        )
        return

    if awaiting == "BOOK_ADS":
        # accept first line text, optional second line url
        lines = text.splitlines()
        ad_text = lines[0].strip()
        ad_url = (lines[1].strip() if len(lines) > 1 else "")
        bid = add_booking("ads", "N/A", update.effective_chat.id, update.effective_user.id, ADS_HOURS, ADS_PRICE_SOL)
        # store ad payload in booking
        for b in BOOKINGS.get("bookings") or []:
            if b.get("id") == bid:
                b["ad_text"] = ad_text
                b["ad_url"] = ad_url
        context.user_data["awaiting"] = None
        save_all()
        await update.message.reply_text(
            f"✅ Ads booking created.\n\n"
            f"ID: <code>{bid}</code>\n"
            f"Amount: <b>{ADS_PRICE_SOL} SOL</b>\n"
            f"Pay to: <code>{PAY_WALLET or 'SET PAY_WALLET env'}</code>\n\n"
            f"After payment, owner activates with /confirm <code>{bid}</code>.",
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True
        )
        return

# -------------------- TOKEN META NORMALIZE --------------------
def normalize_coin_meta(meta: Dict[str, Any]) -> Dict[str, Any]:
    mint = str(meta.get("mint") or meta.get("address") or "").strip()
    name = str(meta.get("name") or "").strip()
    sym = str(meta.get("symbol") or meta.get("ticker") or "").strip()
    tg = str(meta.get("telegram") or meta.get("telegram_link") or meta.get("telegramUrl") or "").strip()
    # price / liquidity / mcap fields vary; best effort
    price_usd = None
    liq_usd = None
    mc_usd = None
    holders = None
    for k in ("priceUsd", "price_usd", "usdPrice", "price"):
        if k in meta:
            try:
                price_usd = float(meta[k])
                break
            except Exception:
                pass
    for k in ("liquidityUsd", "liq_usd", "liquidity", "liquidity_usd"):
        if k in meta:
            try:
                liq_usd = float(meta[k])
                break
            except Exception:
                pass
    for k in ("marketCapUsd", "mc_usd", "market_cap_usd", "usd_market_cap", "marketCap"):
        if k in meta:
            try:
                mc_usd = float(meta[k])
                break
            except Exception:
                pass
    for k in ("holders", "holderCount", "numHolders"):
        if k in meta:
            try:
                holders = int(float(meta[k]))
                break
            except Exception:
                pass

    return {
        "mint": mint,
        "name": name,
        "symbol": sym,
        "telegram": tg,
        "price_usd": price_usd,
        "liq_usd": liq_usd,
        "mc_usd": mc_usd,
        "holders": holders,
        "added_ts": now_ts(),
    }

# -------------------- POLLER --------------------
async def poll_loop(app: Application):
    await asyncio.sleep(2)
    log.info("Polling loop started (interval=%ss)", POLL_INTERVAL)
    while True:
        try:
            await poll_once(app)
        except Exception as e:
            log.exception("poll_once error: %s", e)
        await asyncio.sleep(POLL_INTERVAL)

def get_scope_key(chat_id: int) -> str:
    return str(chat_id)

def get_seen(scope: str) -> Dict[str, Any]:
    s = SEEN.get(scope) or {}
    SEEN[scope] = s
    return s

async def poll_once(app: Application):
    # refresh leaderboard message occasionally
    await ensure_leaderboard_message(app)

    # determine what to track: each enabled group has 1 mint
    targets: List[Tuple[int, str, Dict[str, Any]]] = []  # (chat_id, mint, group_settings)
    for cid, g in list(GROUPS.items()):
        try:
            chat_id = int(cid)
        except Exception:
            continue
        if not g.get("enabled"):
            continue
        mint = (g.get("mint") or "").strip()
        if not mint:
            continue
        targets.append((chat_id, mint, g))

    if not targets and not TRENDING_CHANNEL_ID:
        return

    # prefetch token meta when missing / stale
    for _, mint, _ in targets:
        if mint and mint not in TOKENS:
            meta = get_coin_meta(mint) or {"mint": mint}
            TOKENS[mint] = normalize_coin_meta(meta)

    # poll trades for each mint
    for chat_id, mint, g in targets:
        trades = get_trades(mint, limit=40)
        parsed = []
        for t in trades:
            pt = parse_trade(t)
            if pt and pt["is_buy"]:
                parsed.append(pt)
        # sort oldest->newest by ts
        parsed.sort(key=lambda x: (x["ts"], x["sig"]))
        scope = get_scope_key(chat_id)
        seen = get_seen(scope)
        last_sig = str(seen.get(mint, ""))

        for pt in parsed:
            if last_sig and pt["sig"] <= last_sig:
                # signatures are not sortable reliably; so also track set
                pass

        # Instead: keep a set of last N sigs in seen
        sigset = set(seen.get(mint + ":set", []) or [])
        new_pts = [pt for pt in parsed if pt["sig"] not in sigset]
        if not new_pts:
            continue
        # update sigset with newest
        for pt in new_pts[-10:]:
            sigset.add(pt["sig"])
        # keep only last 200
        siglist = list(sigset)[-200:]
        seen[mint + ":set"] = siglist

        # post new buys
        token = TOKENS.get(mint) or {"mint": mint, "symbol": "", "name": mint}
        for pt in new_pts:
            # min buy filter
            min_buy = float(g.get("min_buy_sol") or 0.0)
            if pt["sol"] < min_buy:
                continue
            # update organic leaderboard
            lb_record_buy(mint, pt["sol"])
            # group post
            await send_buy(app, chat_id, token, pt, for_channel=False)
            # trending channel post
            if g.get("post_to_trending") and int(g.get("trending_channel_id") or 0) != 0:
                await send_buy(app, int(g.get("trending_channel_id")), token, pt, for_channel=True)

    save_all()

async def send_buy(app: Application, chat_id: int, token: Dict[str, Any], pt: Dict[str, Any], for_channel: bool):
    try:
        text, kb = build_buy_message(token, pt, for_channel=for_channel)
        await app.bot.send_message(
            chat_id=chat_id,
            text=text,
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True,
            reply_markup=kb
        )
    except Forbidden:
        pass
    except BadRequest as e:
        # fallback without kb if markup issues
        try:
            await app.bot.send_message(
                chat_id=chat_id,
                text=re.sub(r"<[^>]+>", "", text),
                disable_web_page_preview=True,
            )
        except Exception:
            log.warning("BadRequest send_buy: %s", e)

# -------------------- TELEGRAM JOIN EVENTS --------------------
async def on_chat_member(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # When added to group and made admin, show setup wizard
    try:
        chat = update.effective_chat
        if not chat:
            return
        member = update.my_chat_member
        if not member:
            return
        new_status = member.new_chat_member.status
        if chat.type in ("group", "supergroup") and new_status in ("member", "administrator"):
            # show setup if admin or member (needs admin to post)
            await show_setup(update, context, note="👋 Thanks for adding me! Configure me below.")
    except Exception:
        return

# -------------------- PERSISTENCE --------------------
def load_all():
    global TOKENS, GROUPS, SEEN, BOOKINGS, LEADERBOARD, AD_SLOTS
    TOKENS = {t["mint"]: t for t in (load_json(TOKENS_FILE, []) or []) if isinstance(t, dict) and t.get("mint")}
    GROUPS = load_json(GROUPS_FILE, {}) or {}
    SEEN = load_json(SEEN_FILE, {}) or {}
    BOOKINGS = load_json(BOOKINGS_FILE, {}) or {}
    LEADERBOARD = load_json(LEADERBOARD_FILE, {}) or {}
    AD_SLOTS = load_json(AD_SLOTS_FILE, {}) or {}

def save_all():
    save_json(TOKENS_FILE, list(TOKENS.values()))
    save_json(GROUPS_FILE, GROUPS)
    save_json(SEEN_FILE, SEEN)
    save_json(BOOKINGS_FILE, BOOKINGS)
    save_json(LEADERBOARD_FILE, LEADERBOARD)
    save_json(AD_SLOTS_FILE, AD_SLOTS)

# -------------------- FLASK HEALTH --------------------
flask_app = Flask(__name__)

@flask_app.get("/")
def home():
    return "OK", 200

def run_flask():
    port = int(os.getenv("PORT", "8080"))
    flask_app.run(host="0.0.0.0", port=port)

# -------------------- MAIN --------------------
def main():
    load_all()
    app = ApplicationBuilder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", start_cmd))
    app.add_handler(CommandHandler("help", help_cmd))
    app.add_handler(CommandHandler("tokens", tokens_cmd))
    app.add_handler(CommandHandler("mytokens", tokens_cmd))
    app.add_handler(CommandHandler("trending", trending_cmd))
    app.add_handler(CommandHandler("ads", ads_cmd))
    app.add_handler(CommandHandler("confirm", confirm_cmd))
    app.add_handler(CommandHandler("adset", adset_cmd))
    app.add_handler(CommandHandler("adclear", adclear_cmd))

    app.add_handler(CallbackQueryHandler(on_button))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))
    app.add_handler(ChatMemberHandler(on_chat_member, ChatMemberHandler.MY_CHAT_MEMBER))

    # background poller
    app.job_queue.run_once(lambda *_: asyncio.create_task(poll_loop(app)), when=1)

    # flask in thread for Railway health
    import threading
    threading.Thread(target=run_flask, daemon=True).start()

    log.info("PumpTools Solana BuyBot starting...")
    while True:
        try:
            app.run_polling(close_loop=False, allowed_updates=Update.ALL_TYPES)
            break
        except Conflict:
            log.warning("Conflict: another instance is running. Retrying in 5s...")
            time.sleep(5)
        except Exception as e:
            log.exception("run_polling error: %s", e)
            time.sleep(5)

if __name__ == "__main__":
    main()
