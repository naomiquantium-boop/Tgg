
import os, json, time, asyncio, logging, re, html, base64
from typing import Any, Dict, Optional, List, Tuple
from urllib.parse import urlparse, quote
import requests

from flask import Flask
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, InputFile
from telegram.error import Conflict
from telegram.ext import (
    Application, ApplicationBuilder,
    CommandHandler, CallbackQueryHandler,
    MessageHandler, ChatMemberHandler, ContextTypes, filters
)

# -------------------- LOGGING --------------------
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
log = logging.getLogger("spyton_public")

# -------------------- ENV --------------------
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
TONAPI_KEY = os.getenv("TONAPI_KEY", "").strip()
TONAPI_BASE = os.getenv("TONAPI_BASE", "https://tonapi.io").strip().rstrip("/")
POLL_INTERVAL = max(2.0, float(os.getenv("POLL_INTERVAL", "2.0")))
BURST_WINDOW_SEC = int(os.getenv("BURST_WINDOW_SEC", "30"))
DTRADE_REF = os.getenv("DTRADE_REF", "https://t.me/dtrade?start=11TYq7LInG").strip()
TRENDING_URL = os.getenv("TRENDING_URL", "https://t.me/SpyTonTrending").strip()
DEFAULT_TOKEN_TG = os.getenv("DEFAULT_TOKEN_TG", "https://t.me/SpyTonEco").strip()
LISTING_URL = os.getenv("LISTING_URL", "https://t.me/TonProjectListing").strip()

DATA_DIR = os.getenv("DATA_DIR", "").strip()
def _data_path(p: str) -> str:
    if not p:
        return p
    if DATA_DIR and (not os.path.isabs(p)):
        return os.path.join(DATA_DIR, p)
    return p
if DATA_DIR:
    try:
        os.makedirs(DATA_DIR, exist_ok=True)
    except Exception:
        pass

# Trending leaderboard (Top-10) message in the trending channel.
LEADERBOARD_ON = str(os.getenv("LEADERBOARD_ON", "1")).strip().lower() in ("1","true","yes","on")
LEADERBOARD_INTERVAL = max(30, int(float(os.getenv("LEADERBOARD_INTERVAL", "60"))))
LEADERBOARD_WINDOW_HOURS = max(1, int(float(os.getenv("LEADERBOARD_WINDOW_HOURS", "6"))))
# Used for the % column in the organic leaderboard: compare current window vs previous window.
LEADERBOARD_COMPARE_WINDOW_HOURS = max(1, int(float(os.getenv("LEADERBOARD_COMPARE_WINDOW_HOURS", str(LEADERBOARD_WINDOW_HOURS)))))
LEADERBOARD_STATS_FILE = _data_path(os.getenv("LEADERBOARD_STATS_FILE", "leaderboard_stats.json"))
LEADERBOARD_MSG_FILE = _data_path(os.getenv("LEADERBOARD_MSG_FILE", "leaderboard_msg.json"))
BOOK_TRENDING_URL = os.getenv("BOOK_TRENDING_URL", "https://t.me/SpyTONTrndBot").strip()
LEADERBOARD_HEADER_HANDLE = os.getenv("LEADERBOARD_HEADER_HANDLE", "@Spytontrending").strip()
LEADERBOARD_MESSAGE_ID_STR = os.getenv("LEADERBOARD_MESSAGE_ID", "").strip()  # e.g. 25145
LEADERBOARD_CHAT_ID_STR = os.getenv("LEADERBOARD_CHAT_ID", "").strip()  # optional; defaults to TRENDING_POST_CHAT_ID

# Optional: mirror *all* buy posts into an official trending/listing channel.
# Set TRENDING_POST_CHAT_ID to your channel's numeric id (e.g. -100123...).
# If MIRROR_TO_TRENDING is truthy, every buy posted in any configured group will also be posted there.
TRENDING_POST_CHAT_ID = os.getenv("TRENDING_POST_CHAT_ID", "").strip()
MIRROR_TO_TRENDING = str(os.getenv("MIRROR_TO_TRENDING", "0")).strip().lower() in ("1","true","yes","on")

# Owner-only Ads system
OWNER_IDS = [int(x) for x in re.split(r"[ ,;]+", os.getenv("OWNER_IDS", "").strip()) if x.strip().isdigit()]
ADS_FILE = _data_path(os.getenv("ADS_FILE", "ads_public.json"))
DEFAULT_AD_TEXT = os.getenv("DEFAULT_AD_TEXT", "Advertise here").strip()
DEFAULT_AD_LINK = os.getenv("DEFAULT_AD_LINK", "https://t.me/vseeton").strip()
GECKO_BASE = os.getenv("GECKO_BASE", "https://api.geckoterminal.com/api/v2").strip().rstrip("/")

DATA_FILE = _data_path(os.getenv("GROUPS_FILE", "groups_public.json"))
SEEN_FILE = _data_path(os.getenv("SEEN_FILE", "seen_public.json"))
# -------------------- AMOUNT HELPERS --------------------
NANO = 10**9

def ensure_ton_amount(v: float) -> float:
    """Heuristic: convert nanoton to TON if value looks like nanoton."""
    try:
        x = float(v)
    except Exception:
        return 0.0
    # Anything absurdly large is likely nanoton.
    if x > 1e6:
        return x / NANO
    return x


USER_PREFS_FILE = _data_path(os.getenv("USER_PREFS_FILE", "user_prefs_public.json"))

# Owner-added tokens that are tracked globally (posted in the trending channel even if no group added the bot).
# Stored by jetton master address.
GLOBAL_TOKENS_FILE = _data_path(os.getenv("GLOBAL_TOKENS_FILE", "tokens_public.json"))

# Dexscreener endpoints (used to resolve pool<->token)
DEX_TOKEN_URL = os.getenv("DEX_TOKEN_URL", "https://api.dexscreener.com/latest/dex/tokens").rstrip("/")
DEX_PAIR_URL = os.getenv("DEX_PAIR_URL", "https://api.dexscreener.com/latest/dex/pairs").rstrip("/")


# -------------------- STON API (exported events) --------------------
STON_BASE = os.getenv("STON_BASE", "https://api.ston.fi").rstrip("/")
STON_LATEST_BLOCK_URL = f"{STON_BASE}/export/dexscreener/v1/latest-block"
STON_EVENTS_URL = f"{STON_BASE}/export/dexscreener/v1/events"
STON_HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json,text/plain,*/*",
    "Accept-Language": "en-US,en;q=0.9",
}
STON_LAST_BLOCK: Optional[int] = None

def ston_latest_block() -> Optional[int]:
    """Return the latest exported block number from STON.fi export feed.

    The API sometimes returns:
      {"block": {"blockNumber": 123}}
    or other variants. We normalize safely.
    """
    try:
        r = requests.get(STON_LATEST_BLOCK_URL, headers=STON_HEADERS, timeout=12)
        if r.status_code != 200:
            return None
        js = r.json()

        # Primary format
        if isinstance(js, dict) and isinstance(js.get("block"), dict):
            v = js["block"].get("blockNumber") or js["block"].get("block_number")
            try:
                return int(v)
            except Exception:
                return None

        # Other common variants
        if isinstance(js, dict):
            v = js.get("latestBlock") or js.get("latest_block") or js.get("block")
            try:
                return int(v)
            except Exception:
                return None

        if isinstance(js, int):
            return js
        if isinstance(js, str) and js.isdigit():
            return int(js)
        return None
    except Exception:
        return None

def ston_events(from_block: int, to_block: int) -> Optional[List[Dict[str, Any]]]:
    """Fetch STON.fi export events.

    Returns:
      - list of event dicts on success
      - None on HTTP/parse failure (so callers don't advance cursors and skip buys)
    """
    params = {"fromBlock": int(from_block), "toBlock": int(to_block)}
    try:
        r = requests.get(STON_EVENTS_URL, params=params, headers=STON_HEADERS, timeout=20)
        if r.status_code != 200:
            return None
        js = r.json()
        if isinstance(js, list):
            return [x for x in js if isinstance(x, dict)]
        if isinstance(js, dict) and isinstance(js.get("events"), list):
            return [x for x in js["events"] if isinstance(x, dict)]
        return []
    except Exception:
        return None

def ensure_ton_leg_for_pool(token: Dict[str, Any]) -> Optional[int]:
    # cache 0/1 where TON is leg0(amount0*) or leg1(amount1*)
    tl = token.get("ton_leg")
    if tl in (0,1):
        return int(tl)
    pool = token.get("ston_pool")
    if not pool:
        return None
    meta = _dex_pair_lookup(pool)
    if not isinstance(meta, dict):
        return None
    base = (meta.get("baseToken") or {})
    quote = (meta.get("quoteToken") or {})
    base_sym = str(base.get("symbol") or "").upper()
    quote_sym = str(quote.get("symbol") or "").upper()
    if base_sym in ("TON","WTON","PTON"):
        token["ton_leg"] = 0
        return 0
    if quote_sym in ("TON","WTON","PTON"):
        token["ton_leg"] = 1
        return 1
    return None

# Treat TON-like wrappers as TON in swap feeds
TON_LIKE_SYMS = {"TON", "WTON", "PTON", "pTON", "wTON", "wton"}

def ston_event_ton_leg(ev: Dict[str, Any]) -> Optional[int]:
    """Infer which event leg (0/1) is TON using symbols included in STON export events."""
    def _sym(v: Any) -> str:
        if v is None:
            return ""
        return str(v).strip()

    s0 = _sym(ev.get("token0Symbol") or ev.get("token0_symbol") or ev.get("symbol0"))
    s1 = _sym(ev.get("token1Symbol") or ev.get("token1_symbol") or ev.get("symbol1"))

    # Sometimes token0/token1 are dicts
    if isinstance(ev.get("token0"), dict):
        s0 = _sym(ev["token0"].get("symbol") or ev["token0"].get("ticker") or s0)
    if isinstance(ev.get("token1"), dict):
        s1 = _sym(ev["token1"].get("symbol") or ev["token1"].get("ticker") or s1)

    s0u = s0.upper()
    s1u = s1.upper()
    ton_set = {x.upper() for x in TON_LIKE_SYMS}
    if s0u in ton_set:
        return 0
    if s1u in ton_set:
        return 1
    return None

def ston_event_is_buy(ev: Dict[str, Any], ton_leg: int):
    """Return (is_buy, ton_spent, token_received). Sells are ignored (is_buy=False)."""
    a0_in = _to_float(ev.get("amount0In"))
    a0_out = _to_float(ev.get("amount0Out"))
    a1_in = _to_float(ev.get("amount1In"))
    a1_out = _to_float(ev.get("amount1Out"))

    if ton_leg == 0:
        # BUY: TON in (leg0), token out (leg1)
        if a0_in > 0 and a1_out > 0:
            return True, a0_in, a1_out
        # SELL would be a1_in > 0 and a0_out > 0 (ignore)
        return False, 0.0, 0.0

    if ton_leg == 1:
        # BUY: TON in (leg1), token out (leg0)
        if a1_in > 0 and a0_out > 0:
            return True, a1_in, a0_out
        # SELL would be a0_in > 0 and a1_out > 0 (ignore)
        return False, 0.0, 0.0

    return False, 0.0, 0.0


# -------------------- DEDUST API (for pool discovery + trades) --------------------
DEDUST_API = os.getenv("DEDUST_API", "https://api.dedust.io").rstrip("/")

_DEDUST_POOLS_CACHE = {"ts": 0, "data": None}

def dedust_get_pools() -> List[Dict[str, Any]]:
    """Fetch available pools from DeDust API. Cached to avoid heavy downloads."""
    now = int(time.time())
    if _DEDUST_POOLS_CACHE["data"] is not None and now - int(_DEDUST_POOLS_CACHE["ts"] or 0) < 3600:
        return _DEDUST_POOLS_CACHE["data"] or []
    try:
        r = requests.get(f"{DEDUST_API}/v2/pools", timeout=25)
        if r.status_code != 200:
            return _DEDUST_POOLS_CACHE["data"] or []
        js = r.json()
        pools = js.get("pools") if isinstance(js, dict) else js
        if not isinstance(pools, list):
            pools = []
        _DEDUST_POOLS_CACHE["ts"] = now
        _DEDUST_POOLS_CACHE["data"] = pools
        return pools
    except Exception:
        return _DEDUST_POOLS_CACHE["data"] or []

def _dedust_is_ton_asset(asset: Any) -> bool:
    if not isinstance(asset, dict):
        return False
    t = (asset.get("type") or asset.get("kind") or "").lower()
    # common representations
    if t in ("native", "ton"):
        return True
    # sometimes TON shown as jetton with empty address
    sym = (asset.get("symbol") or "").upper()
    if sym in ("TON","WTON"):
        return True
    addr = (asset.get("address") or "").strip()
    # TON has no jetton master address; keep conservative
    return False

def _dedust_asset_addr(asset: Any) -> str:
    if not isinstance(asset, dict):
        return ""
    return str(asset.get("address") or asset.get("master") or asset.get("jetton") or "").strip()

def find_dedust_ton_pair_for_token(token_address: str) -> Optional[str]:
    """Find DeDust pool address for TON <-> token.

    Primary method: DexScreener token endpoint filtered to DeDust (fast + includes new pools).
    Fallback: DeDust API pools list (may lag / be paginated).
    """
    ta = (token_address or "").strip()
    if not ta:
        return None

    # 1) DexScreener (most reliable for newly created pools)
    try:
        pair = find_pair_for_token_on_dex(ta, "dedust")
        if pair:
            return pair
    except Exception:
        pass
    try:
        pools = dedust_get_pools()
        best_pool = None
        best_liq = -1.0
        for p in pools:
            if not isinstance(p, dict):
                continue
            addr = str(p.get("address") or p.get("pool") or p.get("id") or "").strip()
            if not addr:
                continue
            assets = p.get("assets") or p.get("tokens") or p.get("reserves") or []
            # assets might be dict with keys a/b
            if isinstance(assets, dict):
                assets = list(assets.values())
            if not isinstance(assets, list) or len(assets) < 2:
                continue
            a0, a1 = assets[0], assets[1]
            # Determine TON side
            ton_side = None
            tok_side_addr = ""
            if _dedust_is_ton_asset(a0):
                ton_side = 0
                tok_side_addr = _dedust_asset_addr(a1)
            elif _dedust_is_ton_asset(a1):
                ton_side = 1
                tok_side_addr = _dedust_asset_addr(a0)
            else:
                continue
            if not tok_side_addr:
                continue
            if tok_side_addr != ta:
                continue
            # liquidity score if available
            liq = 0.0
            try:
                liq = float(p.get("liquidityUsd") or p.get("liquidity_usd") or p.get("tvlUsd") or 0.0)
            except Exception:
                liq = 0.0
            if liq > best_liq:
                best_liq = liq
                best_pool = addr
        return best_pool
    except Exception:
        return None

def dedust_get_trades(pool: str, limit: int = 20) -> List[Dict[str, Any]]:
    try:
        r = requests.get(f"{DEDUST_API}/v2/pools/{pool}/trades", params={"limit": limit}, timeout=25)
        if r.status_code != 200:
            return []
        js = r.json()
        trades = js.get("trades") if isinstance(js, dict) else js
        if not isinstance(trades, list):
            return []
        return trades
    except Exception:
        return []

def dedust_trade_to_buy(tr: Dict[str, Any], token_addr: str) -> Optional[Dict[str, Any]]:
    """Convert a DeDust trade item to our buy dict if it's TON -> token."""
    if not isinstance(tr, dict):
        return None
    # common fields guesses
    tx = str(tr.get("tx") or tr.get("txHash") or tr.get("hash") or tr.get("transaction") or "").strip()
    buyer = str(tr.get("sender") or tr.get("trader") or tr.get("maker") or tr.get("wallet") or "").strip()
    trade_id = str(tr.get("id") or tr.get("tradeId") or tr.get("lt") or tr.get("seqno") or tx).strip()
    # asset in/out objects
    ain = tr.get("assetIn") or tr.get("inAsset") or tr.get("fromAsset") or tr.get("in") or {}
    aout = tr.get("assetOut") or tr.get("outAsset") or tr.get("toAsset") or tr.get("out") or {}
    # amounts
    amt_in = tr.get("amountIn") or tr.get("inAmount") or tr.get("amount_in") or tr.get("amountInJettons") or tr.get("amount_in_wei") or tr.get("in") or None
    amt_out = tr.get("amountOut") or tr.get("outAmount") or tr.get("amount_out") or tr.get("amountOutJettons") or tr.get("out") or None

    # Some APIs nest amounts with decimals
    def _as_float(x):
        try:
            if isinstance(x, dict):
                x = x.get("value") or x.get("amount")
            return float(x)
        except Exception:
            return 0.0

    amt_in_f = _as_float(amt_in)
    amt_out_f = _as_float(amt_out)

    # Determine if this is TON -> token
    is_ton_in = _dedust_is_ton_asset(ain) or (isinstance(ain, dict) and (ain.get("symbol") or "").upper() in ("TON","WTON"))
    out_addr = _dedust_asset_addr(aout)
    if not is_ton_in:
        return None
    if out_addr != token_addr:
        return None

    # TON amount is in TON (API usually already human). If API returns nano, it will be huge; we guard:
    ton_amt = amt_in_f
    if ton_amt > 1e8:  # looks like nanoTON
        ton_amt = ton_amt / 1e9

    token_amt = amt_out_f
    # DeDust API sometimes returns jetton amount in minimal units (integer-like).
    # Convert using jetton decimals when it looks too large.
    try:
        dec = int(get_jetton_meta(token_addr).get("decimals") or 9)
        if token_amt > 1e8:
            token_amt = token_amt / (10 ** dec)
    except Exception:
        pass

    return {
        "tx": tx or trade_id,
        "buyer": buyer,
        "ton": ton_amt,
        "token_amount": token_amt,
        "trade_id": trade_id,
    }


# -------------------- DEDUST (TonAPI events fallback) --------------------
DEDUST_BUY_OPS = {
    "0xa5a7cbf8",  # buy
    "0xcbc33949",  # sell (kept for filtering)
    "0x5652f1df",  # rewards/other (kept for filtering)
}

def dedust_buys_from_tonapi_event(ev: Dict[str, Any], token_addr: str, pool_addr: str) -> List[Dict[str, Any]]:
    """Extract TON->token buys from a TonAPI *pool* event.

    Why this exists:
    - DeDust "trades" API can lag or be empty for some pools.
    - TonAPI /events sometimes omits the nice "swap" action; instead you get SmartContractExec + transfers.

    Strategy (best-effort):
    1) Find JettonTransfer where jetton master == token_addr and recipient is some user => that's the buyer + token_amount.
    2) Find TON amount spent by that buyer in the same event:
       - Prefer TonTransfer buyer -> pool_addr.
       - Else use SmartContractExec.ton_attached if executor == buyer.
    """
    if not isinstance(ev, dict):
        return []
    token_addr = str(token_addr or "").strip()
    pool_addr = str(pool_addr or "").strip()
    if not token_addr or not pool_addr:
        return []

    actions = ev.get("actions") or []
    if not isinstance(actions, list):
        return []

    tx_hash = tonapi_event_tx_hash(ev)
    event_id = str(ev.get("event_id") or ev.get("id") or "").strip()

    # Collect candidate jetton transfers to users (recipient != pool).
    candidates: List[Tuple[str, float]] = []  # (buyer_addr, token_amount)
    for a in actions:
        if not isinstance(a, dict) or a.get("type") != "JettonTransfer":
            continue
        jt = a.get("JettonTransfer")
        if not isinstance(jt, dict):
            continue

        jetton = jt.get("jetton") or {}
        jetton_addr = str((jetton.get("address") if isinstance(jetton, dict) else "") or "").strip()
        if jetton_addr != token_addr:
            continue

        recipient = jt.get("recipient") or {}
        buyer_addr = str((recipient.get("address") if isinstance(recipient, dict) else "") or "").strip()
        if not buyer_addr or buyer_addr == pool_addr:
            continue

        amt_raw = jt.get("amount")
        # Prefer cached decimals from jetton master (some events omit decimals)
        dec = 9
        try:
            if isinstance(jetton, dict) and jetton.get("decimals") is not None:
                dec = int(jetton.get("decimals") or 9)
            else:
                dec = int(get_jetton_meta(token_addr).get("decimals") or 9)
        except Exception:
            dec = 9
        try:
            token_amount = int(str(amt_raw)) / (10 ** dec)
        except Exception:
            token_amount = 0.0
        if token_amount <= 0:
            continue

        candidates.append((buyer_addr, token_amount))

    if not candidates:
        return []

    # Map buyer -> TON spent (best-effort).
    # Prefer explicit TonTransfer buyer -> pool, but some pools route via vault contracts.
    ton_spent_by: Dict[str, float] = {}
    outgoing_by: Dict[str, float] = {}
    for a in actions:
        if not isinstance(a, dict) or a.get("type") not in ("TonTransfer", "TONTransfer", "Transfer"):
            continue
        tt = a.get("TonTransfer") or a.get("TONTransfer") or a.get("Transfer")
        if not isinstance(tt, dict):
            continue
        sender = tt.get("sender") or {}
        recipient = tt.get("recipient") or {}
        sender_addr = str((sender.get("address") if isinstance(sender, dict) else "") or "").strip()
        recip_addr = str((recipient.get("address") if isinstance(recipient, dict) else "") or "").strip()
        if not sender_addr or not recip_addr or sender_addr == recip_addr:
            continue
        amt = tt.get("amount")
        try:
            ton_amt = float(amt) / 1e9
        except Exception:
            ton_amt = 0.0
        if ton_amt <= 0:
            continue

        # Track outgoing TON from sender (fallback when pool routing is indirect)
        outgoing_by[sender_addr] = max(outgoing_by.get(sender_addr, 0.0), ton_amt)

        # Direct buyer -> pool transfer (best signal)
        if recip_addr == pool_addr:
            ton_spent_by[sender_addr] = max(ton_spent_by.get(sender_addr, 0.0), ton_amt)

    # If we didn't find direct transfers to the pool, fall back to the biggest outgoing TON per buyer.
    if not ton_spent_by and outgoing_by:
        ton_spent_by = outgoing_by

    # Fallback: SmartContractExec.ton_attached when executor is the buyer
    if not ton_spent_by:
        for a in actions:
            if not isinstance(a, dict) or a.get("type") != "SmartContractExec":
                continue
            sc = a.get("SmartContractExec")
            if not isinstance(sc, dict):
                continue
            op = str(sc.get("operation") or sc.get("op") or "").strip().lower()
            if op.startswith("call:"):
                op = op.replace("call:", "").strip()
            # If the opcode is present and is not our known set, skip it.
            if op and op not in DEDUST_BUY_OPS:
                continue

            executor = sc.get("executor") or {}
            ex_addr = str((executor.get("address") if isinstance(executor, dict) else "") or "").strip()
            if not ex_addr:
                continue

            ton_attached = sc.get("ton_attached") or sc.get("tonAttached") or 0
            try:
                ton_amt = float(ton_attached) / 1e9
            except Exception:
                ton_amt = 0.0
            if ton_amt <= 0:
                continue
            ton_spent_by[ex_addr] = max(ton_spent_by.get(ex_addr, 0.0), ton_amt)

    buys: List[Dict[str, Any]] = []
    for buyer_addr, token_amount in candidates:
        ton_amount = ton_spent_by.get(buyer_addr, 0.0)
        if ton_amount <= 0:
            continue
        buys.append({
            "tx": tx_hash or event_id,
            "buyer": buyer_addr,
            "ton": ton_amount,
            "token_amount": token_amount,
            "event_id": event_id,
        })
    return buys

# -------------------- STATE --------------------
DEFAULT_SETTINGS = {
    "enable_ston": True,
    "enable_dedust": True,
    "min_buy_ton": 0.0,
    "anti_spam": "MED",   # LOW | MED | HIGH
    "burst_mode": True,

    # Crypton-style options
    "strength_on": True,
    "strength_emoji": "🟢",
    "strength_step_ton": 5.0,   # 1 strength unit per X TON
    "strength_max": 30,         # max emojis

    # Optional buy alert image
    # If enabled and a file_id is set, the bot will send a Telegram photo (not a link).
    "buy_image_on": False,
    "buy_image_file_id": "",

    # Min buy can be TON or USD
    "min_buy_unit": "TON",   # TON | USD
    "min_buy_usd": 0.0,

    # Layout toggles
    "show_price": True,
    "show_liquidity": True,
    "show_mcap": True,
    "show_holders": True,
}

def _load_json(path: str, default):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default

def _save_json(path: str, obj):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2, ensure_ascii=False)
    os.replace(tmp, path)

GROUPS: Dict[str, Any] = _load_json(DATA_FILE, {})  # chat_id -> config
SEEN: Dict[str, Any] = _load_json(SEEN_FILE, {})    # chat_id -> {dedupe_key: ts}
USER_PREFS: Dict[str, Any] = _load_json(USER_PREFS_FILE, {})  # user_id -> {"lang": "en"|"ru"}
# -------------------- I18N (EN/RU) --------------------
I18N: Dict[str, Dict[str, str]] = {
  "en": {
    "btn_add_group": "➕ Add BuyBot to Group",
    "btn_cfg_token": "⚙️ Configure Token",
    "btn_settings": "🛠 Settings",
    "btn_support": "🆘 Support",
    "btn_language": "🌐 Language",
    "lang_title": "Choose language / Выберите язык",
    "lang_en": "🇬🇧 English",
    "lang_ru": "🇷🇺 Russian",
    "start_title": "🚀 *SpyTON BuyBot*",
    "start_desc": "Premium buy alerts for STON.fi + DeDust (TON).\n\n• Add to a group\n• Configure token in 10 seconds\n• Clean buy posts + ads support\n\nUse the buttons below:",
    "connected_title": "✅ *SpyTON BuyBot connected*",
    "connected_desc": "Now send the token CA here in DM.\nI will auto-detect *STON.fi* / *DeDust* pools and start posting buys in your group.\n\nTip: you can also include the token Telegram link in the same message.\nExample:\n`<CA> https://t.me/YourToken`",
    "lang_set_ok": "Language saved: English ✅",
    "lang_set_ok_ru": "Language saved: Russian ✅",
    "need_admin": "Admins only.",
    "wiz_paste_title": "🛰 SpyTON Setup — Paste Token CA",
    "wiz_paste_hint": "STON.fi / DeDust will be auto-detected.",
    "wiz_found_title": "🔎 Token found",
    "wiz_confirm": "✅ Confirm",
    "wiz_edit": "✏️ Edit",
    "wiz_cancel": "❌ Cancel",
    "wiz_control_title": "🎛 Mission Control",
    "wiz_tab_basics": "⚙️ Basics",
    "wiz_tab_display": "👁 Display",
    "wiz_tab_links": "🔗 Links",
    "wiz_done": "✅ Done",
  },
  "ru": {
    "btn_add_group": "➕ Добавить BuyBot в группу",
    "btn_cfg_token": "⚙️ Настроить токен",
    "btn_settings": "🛠 Настройки",
    "btn_support": "🆘 Поддержка",
    "btn_language": "🌐 Язык",
    "lang_title": "Выберите язык / Choose language",
    "lang_en": "🇬🇧 English",
    "lang_ru": "🇷🇺 Русский",
    "start_title": "🚀 *SpyTON BuyBot*",
    "start_desc": "Премиум-уведомления о покупках для STON.fi + DeDust (TON).\n\n• Добавьте в группу\n• Настройте токен за 10 секунд\n• Чистые buy-посты + поддержка рекламы\n\nИспользуйте кнопки ниже:",
    "connected_title": "✅ *SpyTON BuyBot подключён*",
    "connected_desc": "Теперь отправьте сюда в ЛС адрес токена (CA).\nЯ автоматически найду пулы *STON.fi* / *DeDust* и начну постить покупки в вашей группе.\n\nСовет: можно добавить ссылку на Telegram токена в том же сообщении.\nПример:\n`<CA> https://t.me/YourToken`",
    "lang_set_ok": "Язык сохранён: English ✅",
    "lang_set_ok_ru": "Язык сохранён: Русский ✅",
    "need_admin": "Только для админов.",
    "wiz_paste_title": "🛰 Настройка SpyTON — отправьте CA",
    "wiz_paste_hint": "Пулы STON.fi / DeDust будут найдены автоматически.",
    "wiz_found_title": "🔎 Токен найден",
    "wiz_confirm": "✅ Подтвердить",
    "wiz_edit": "✏️ Изменить",
    "wiz_cancel": "❌ Отмена",
    "wiz_control_title": "🎛 Панель управления",
    "wiz_tab_basics": "⚙️ Основное",
    "wiz_tab_display": "👁 Отображение",
    "wiz_tab_links": "🔗 Ссылки",
    "wiz_done": "✅ Готово",
  }
}

def _get_user_lang(user_id: Optional[int]) -> str:
    if not user_id:
        return "en"
    u = USER_PREFS.get(str(user_id), {}) if isinstance(USER_PREFS, dict) else {}
    lang = (u.get("lang") or "en").lower()
    return "ru" if lang.startswith("ru") else "en"

def _get_group_lang(chat_id: Optional[int], user_id: Optional[int] = None) -> str:
    if chat_id is None:
        return _get_user_lang(user_id)
    g = GROUPS.get(str(chat_id), {}) if isinstance(GROUPS, dict) else {}
    lang = (g.get("lang") or "").lower()
    if lang:
        return "ru" if lang.startswith("ru") else "en"
    return _get_user_lang(user_id)

def t(key: str, lang: str, **kwargs) -> str:
    lang = "ru" if str(lang).lower().startswith("ru") else "en"
    s = I18N.get(lang, {}).get(key) or I18N["en"].get(key) or key
    try:
        return s.format(**kwargs)
    except Exception:
        return s

def set_user_lang(user_id: int, lang: str):
    lang = "ru" if str(lang).lower().startswith("ru") else "en"
    USER_PREFS[str(user_id)] = {"lang": lang}
    _save_json(USER_PREFS_FILE, USER_PREFS)

def set_group_lang(chat_id: int, lang: str):
    lang = "ru" if str(lang).lower().startswith("ru") else "en"
    cfg = GROUPS.get(str(chat_id), {}) or {}
    cfg["lang"] = lang
    GROUPS[str(chat_id)] = cfg
    _save_json(DATA_FILE, GROUPS)


# Global tokens tracked for the trending channel (owner-only /addtoken)
GLOBAL_TOKENS: Dict[str, Any] = _load_json(GLOBAL_TOKENS_FILE, {})  # jetton_addr -> token dict

# Global paid ad (shown under every buy in all chats)
ADS_STATE: Dict[str, Any] = _load_json(ADS_FILE, {"active_until": 0, "text": "", "link": ""})

# Leaderboard stats (rolling window; stored as a list of timestamps so we can compute 24h volume)
# Structure: { jetton_addr: {"symbol": str, "name": str, "events": [[ts:int, ton:float], ...] } }
LEADERBOARD_STATS: Dict[str, Any] = _load_json(LEADERBOARD_STATS_FILE, {})
LEADERBOARD_MSG_STATE: Dict[str, Any] = _load_json(LEADERBOARD_MSG_FILE, {})  # {channel_id: {"message_id": int}}

# user_id -> chat_id awaiting token paste
AWAITING: Dict[int, Dict[str, Any]] = {}  # user_id -> {'group_id': int, 'stage': str, 'dex': str}

# user_id -> chat_id awaiting social link input
AWAITING_SOCIAL: Dict[int, Dict[str, Any]] = {}  # {'chat_id': int, 'field': 'telegram'|'website'|'twitter'}

# user_id -> chat_id awaiting buy image photo
AWAITING_IMAGE: Dict[int, int] = {}

# user_id -> awaiting custom strength emoji text (can be normal emoji or <tg-emoji ...>)
AWAITING_CUSTOM_EMOJI: Dict[int, int] = {}  # user_id -> chat_id

# -------------------- HELPERS --------------------
JETTON_RE = re.compile(r"\b([EU]Q[A-Za-z0-9_-]{40,80})\b")
GECKO_POOL_RE = re.compile(r"geckoterminal\.com/ton/pools/([A-Za-z0-9_-]{20,120})", re.IGNORECASE)
DEXSCREENER_PAIR_RE = re.compile(r"dexscreener\.com/ton/([A-Za-z0-9_-]{20,120})", re.IGNORECASE)
STON_POOL_RE = re.compile(r"ston\.fi/[^\s]*?(?:pool|pools)/([A-Za-z0-9_-]{20,120})", re.IGNORECASE)
DEDUST_POOL_RE = re.compile(r"dedust\.(?:io|org)/[^\s]*?(?:pool|pools)/([A-Za-z0-9_-]{20,120})", re.IGNORECASE)

def is_private(update: Update) -> bool:
    return bool(update.effective_chat and update.effective_chat.type == "private")

async def is_admin(bot, chat_id: int, user_id: int) -> bool:
    try:
        m = await bot.get_chat_member(chat_id, user_id)
        return m.status in ("administrator", "creator")
    except Exception:
        return False

def get_group(chat_id: int) -> Dict[str, Any]:
    key = str(chat_id)
    g = GROUPS.get(key)
    if not isinstance(g, dict):
        g = {}
        GROUPS[key] = g
    g.setdefault("settings", dict(DEFAULT_SETTINGS))
    g.setdefault("token", None)  # {address, symbol, name, ston_pool, dedust_pool}
    g.setdefault("created_at", int(time.time()))
    return g

def save_groups():
    # Save groups and also persist GLOBAL_TOKENS so polling state (cursors/baselines) is not lost.
    _save_json(DATA_FILE, GROUPS)
    try:
        _save_json(GLOBAL_TOKENS_FILE, GLOBAL_TOKENS)
    except Exception:
        pass

def save_seen():
    _save_json(SEEN_FILE, SEEN)

def save_ads():
    _save_json(ADS_FILE, ADS_STATE)

def save_leaderboard_stats():
    _save_json(LEADERBOARD_STATS_FILE, LEADERBOARD_STATS)

def _humanize_num(n: float) -> str:
    try:
        x = float(n)
    except Exception:
        return str(n)
    ax = abs(x)
    if ax >= 1e12:
        return f"{x/1e12:.2f}T"
    if ax >= 1e9:
        return f"{x/1e9:.2f}B"
    if ax >= 1e6:
        return f"{x/1e6:.2f}M"
    if ax >= 1e3:
        return f"{x/1e3:.2f}K"
    if ax >= 100:
        return f"{x:.0f}"
    if ax >= 10:
        return f"{x:.1f}"
    return f"{x:.2f}"

def _prune_events(events: List[List[Any]], window_sec: int) -> List[List[Any]]:
    now = int(time.time())
    out = []
    for e in events:
        try:
            ts = int(e[0])
            if now - ts <= window_sec:
                out.append([ts, float(e[1])])
        except Exception:
            continue
    return out

def record_buy_for_leaderboard(token: Dict[str, Any], ton_amount: float):
    """Record a buy into the rolling leaderboard stats.

    We keep:
      - events: rolling TON volume within window (legacy/optional)
      - mc_series: rolling market cap snapshots (USD) within window (for % change)
      - mc_usd: latest known market cap (USD) for sorting
    """
    if not LEADERBOARD_ON:
        return
    try:
        jetton = str(token.get("address") or "").strip()
        if not jetton:
            return
        sym = str(token.get("symbol") or "").strip() or "TOKEN"
        name = str(token.get("name") or "").strip() or sym
        # Keep enough events to compute both current and previous windows.
        keep_sec = (int(LEADERBOARD_WINDOW_HOURS) + int(LEADERBOARD_COMPARE_WINDOW_HOURS)) * 3600
        tg = str(token.get("telegram") or "").strip()

        bucket = LEADERBOARD_STATS.get(jetton)
        if not isinstance(bucket, dict):
            bucket = {"symbol": sym, "name": name, "telegram": tg, "events": [], "mc_series": []}
            LEADERBOARD_STATS[jetton] = bucket

        bucket["symbol"] = sym
        bucket["name"] = name
        if tg:
            bucket["telegram"] = tg

        # volume (optional)
        events = bucket.get("events")
        if not isinstance(events, list):
            events = []
        events.append([int(time.time()), float(ton_amount or 0.0)])
        bucket["events"] = _prune_events(events, keep_sec)

        # market cap snapshots (for sorting + % change)
        mc = token.get("mc_usd")
        try:
            mc_val = float(mc) if mc is not None else None
        except Exception:
            mc_val = None
        if mc_val is not None and mc_val > 0:
            bucket["mc_usd"] = mc_val
            series = bucket.get("mc_series")
            if not isinstance(series, list):
                series = []
            series.append([int(time.time()), mc_val])
            # reuse prune helper (expects [ts, val])
            bucket["mc_series"] = _prune_events(series, keep_sec)

        save_leaderboard_stats()
    except Exception:
        return

def _load_leaderboard_msg_state() -> Dict[str, Any]:
    return _load_json(LEADERBOARD_MSG_FILE, {})

def _save_leaderboard_msg_state(state: Dict[str, Any]):
    _save_json(LEADERBOARD_MSG_FILE, state)

def is_owner(user_id: int) -> bool:
    """Return True if user is bot owner (for ads/admin-only ops)."""
    try:
        uid = int(user_id)
    except Exception:
        return False
    return uid in set(OWNER_IDS or [])

def parse_duration_to_seconds(raw: str) -> Optional[int]:
    """Parse duration like '24h' or '7d' to seconds."""
    s = str(raw or '').strip().lower()
    if not s:
        return None
    m = re.match(r"^(\d+)(s|m|h|d)$", s)
    if not m:
        return None
    n = int(m.group(1))
    unit = m.group(2)
    mult = {"s": 1, "m": 60, "h": 3600, "d": 86400}.get(unit, 0)
    if n <= 0 or mult <= 0:
        return None
    return n * mult

def active_ad() -> Tuple[str, str, int]:
    """Return (text, link, seconds_left). If expired, falls back to default line."""
    now = int(time.time())
    try:
        until = int(ADS_STATE.get("active_until") or 0)
    except Exception:
        until = 0
    if until and until > now:
        text = str(ADS_STATE.get("text") or "").strip() or DEFAULT_AD_TEXT
        link = str(ADS_STATE.get("link") or "").strip() or DEFAULT_AD_LINK
        return text, link, max(0, until - now)
    return DEFAULT_AD_TEXT, DEFAULT_AD_LINK, 0




# -------------------- CACHES --------------------
TX_LT_CACHE: Dict[str, Tuple[int, str]] = {}  # key=f"{account}:{lt}" -> (ts, hash)
MARKET_CACHE: Dict[str, Dict[str, Any]] = {}  # key=pool or token -> {ts, price_usd, liq_usd, mc_usd, holders}

# Jetton metadata cache (decimals/symbol/name) to fix wrong amounts from some DEX APIs
JETTON_META_CACHE: dict[str, dict] = {}  # jetton_addr -> {ts, name, symbol, decimals}

def get_jetton_meta(jetton: str, ttl: int = 3600) -> dict:
    """Return cached jetton metadata (name/symbol/decimals) via TonAPI.

    Some DeDust endpoints return jetton amounts in minimal units; decimals are required
    to convert to human numbers. We cache to avoid spamming TonAPI.
    """
    jetton = str(jetton or '').strip()
    if not jetton:
        return {"name": "", "symbol": "", "decimals": 9, "holders_count": None}
    now = int(time.time())
    hit = JETTON_META_CACHE.get(jetton)
    if isinstance(hit, dict) and now - int(hit.get('ts') or 0) < ttl:
        return hit.get('data') or hit
    info = tonapi_jetton_info(jetton)
    data = {
        'name': str(info.get('name') or '').strip(),
        'symbol': str(info.get('symbol') or '').strip(),
        'decimals': int(info.get('decimals') or 9) if str(info.get('decimals') or '').strip().isdigit() else (info.get('decimals') or 9),
        'holders_count': info.get('holders_count'),
    }
    # harden decimals
    try:
        data['decimals'] = int(data['decimals'])
    except Exception:
        data['decimals'] = 9
    JETTON_META_CACHE[jetton] = {'ts': now, 'data': data}
    return data



# TON/USD price cache (for USD min-buy)
TON_PRICE_CACHE: Dict[str, Any] = {"ts": 0, "usd": None}

BOT_USERNAME_CACHE = None

async def get_bot_username(bot):
    global BOT_USERNAME_CACHE
    if BOT_USERNAME_CACHE:
        return BOT_USERNAME_CACHE
    me = await bot.get_me()
    BOT_USERNAME_CACHE = me.username
    return BOT_USERNAME_CACHE


async def stonfi_latest_swaps(pool: str, limit: int = 25) -> List[Dict[str, Any]]:
    """Best-effort: fetch latest pool transactions from TonAPI and treat them as swaps for warmup.
    This is used only to avoid posting old buys right after configuration."""
    try:
        txs = await _to_thread(tonapi_account_transactions, pool, int(limit))
        out = []
        for txo in txs or []:
            h = _tx_hash(txo)
            if h:
                out.append({"hash": h})
        return out
    except Exception:
        return []

async def dedust_latest_trades(pool: str, limit: int = 25) -> List[Dict[str, Any]]:
    """Fetch latest trades from DeDust API in a thread."""
    try:
        return await _to_thread(dedust_get_trades, pool, int(limit))
    except Exception:
        return []

async def warmup_seen_for_chat(chat_id: int, ston_pool: str|None, dedust_pool: str|None):
    """Mark latest swaps as seen so the bot does not spam old buys right after configuration.
    Also sets baseline last_* ids so we skip anything older than the moment the token was configured."""
    try:
        bucket = SEEN.setdefault(str(chat_id), {})
        newest_ston = None
        newest_dedust = None

        # STON.fi (warmup by pool tx hashes from TonAPI)
        if ston_pool:
            swaps = await stonfi_latest_swaps(ston_pool, limit=40)
            for s in swaps:
                txhash = (s.get('tx_hash') or s.get('txHash') or s.get('hash') or '').strip()
                if txhash:
                    bucket[f"ston:{ston_pool}:{txhash}"] = int(time.time())
                    if newest_ston is None:
                        newest_ston = txhash  # first item is newest

        # DeDust (warmup by latest trade ids and tx hashes where available)
        if dedust_pool:
            trades = await dedust_latest_trades(dedust_pool, limit=60)
            # TonAPI events baseline for DeDust pools that don't expose /trades yet (new/legacy pools)
            if not trades:
                try:
                    # Full /events is safer for DeDust pools; subject_only can omit TON transfers.
                    events = await _to_thread(tonapi_account_events, dedust_pool, 40)
                    if isinstance(events, list) and events:
                        newest = events[0]  # newest first
                        eid = str(newest.get('event_id') or newest.get('id') or '').strip()
                        ts = int(newest.get('timestamp') or 0)
                        if eid:
                            newest_dedust = eid
                        if ts:
                            newest_dedust_ts = ts
                except Exception:
                    pass

            # Some DeDust endpoints may return trades in oldest->newest order.
            # To prevent "old buys" spam, we always baseline to the MAX lt/trade_id we can see.
            max_lt_i = None
            max_ts_i = None
            for t in trades:
                lt_raw = (t.get('lt') or t.get('trade_id') or t.get('id') or '')
                lt_s = str(lt_raw).strip()
                if lt_s:
                    try:
                        lt_i = int(lt_s)
                        if (max_lt_i is None) or (lt_i > max_lt_i):
                            max_lt_i = lt_i
                    except Exception:
                        pass

                # timestamp baseline (ms or sec)
                ts_raw = (t.get('timestamp') or t.get('time') or t.get('ts') or 0)
                try:
                    ts_i = int(float(ts_raw or 0))
                    if ts_i > 10_000_000_000:  # ms
                        ts_i = ts_i // 1000
                    if ts_i > 0 and ((max_ts_i is None) or (ts_i > max_ts_i)):
                        max_ts_i = ts_i
                except Exception:
                    pass

                txhash = (t.get('tx_hash') or t.get('txHash') or t.get('hash') or '').strip()
                if txhash:
                    bucket[f"dedust:{dedust_pool}:{txhash}"] = int(time.time())

            if max_lt_i is not None:
                newest_dedust = str(max_lt_i)
            newest_dedust_ts = max_ts_i

        # save baselines into group token so polling skips older history
        g = GROUPS.get(str(chat_id)) or {}
        tok = g.get("token") if isinstance(g, dict) else None
        if isinstance(tok, dict):
            if newest_ston:
                tok["last_ston_tx"] = newest_ston
            if newest_dedust:
                tok["last_dedust_trade"] = newest_dedust
            if newest_dedust_ts:
                tok["last_dedust_ts"] = int(newest_dedust_ts)

            # baseline: ignore anything before now
            tok["ignore_before_ts"] = int(time.time())
            # baseline for STON export cursor: start from current latest block
            try:
                latest = await _to_thread(ston_latest_block)
                if latest is not None:
                    tok["ston_last_block"] = int(latest)
            except Exception:
                pass
            save_groups()

        save_seen()
    except Exception:
        return

def dedupe_ok(chat_id: int, key: str, ttl: int = 600) -> bool:
    now = int(time.time())
    bucket = SEEN.setdefault(str(chat_id), {})
    # clean a little
    if len(bucket) > 4000:
        for k, ts in list(bucket.items())[:800]:
            if now - int(ts) > ttl:
                bucket.pop(k, None)
    ts = bucket.get(key)
    if ts and now - int(ts) < ttl:
        return False
    bucket[key] = now
    return True

def anti_spam_limit(level: str) -> Tuple[int,int]:
    # returns (max_msgs_per_window, window_sec)
    lvl = (level or "MED").upper()
    if lvl == "LOW":
        return (9999, BURST_WINDOW_SEC)
    if lvl == "HIGH":
        return (4, BURST_WINDOW_SEC)
    return (8, BURST_WINDOW_SEC)

# -------------------- TONAPI --------------------
def tonapi_headers() -> Dict[str, str]:
    if not TONAPI_KEY:
        return {"Accept": "application/json"}
    return {"Authorization": f"Bearer {TONAPI_KEY}", "Accept": "application/json"}

def tonapi_get_raw(url: str, params: Optional[Dict[str, Any]] = None) -> Optional[Any]:
    """HTTP GET helper for TonAPI with light retry/backoff.

    Without a TONAPI key, TonAPI can rate-limit (429). We retry a few times and
    fall back to the last known holders value in the caller if still unavailable.
    """
    headers = tonapi_headers()
    # retry on 429 / transient 5xx
    for attempt in range(4):
        try:
            res = requests.get(url, headers=headers, params=params, timeout=20)
            # If the user provided a key but used the wrong header scheme, try X-API-Key once.
            if res.status_code in (401, 403) and TONAPI_KEY:
                res = requests.get(
                    url,
                    headers={"X-API-Key": TONAPI_KEY, "Accept": "application/json"},
                    params=params,
                    timeout=20,
                )

            if res.status_code == 200:
                return res.json()

            # rate limit or temporary server issues: backoff and retry
            if res.status_code in (429, 500, 502, 503, 504):
                try:
                    time.sleep(0.45 * (2 ** attempt))
                except Exception:
                    pass
                continue

            return None
        except Exception:
            # transient network errors
            try:
                time.sleep(0.25 * (2 ** attempt))
            except Exception:
                pass
            continue
    return None
def tonapi_get(url: str, params: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
    js = tonapi_get_raw(url, params=params)
    return js if isinstance(js, dict) else None

def tonapi_jetton_info(jetton: str) -> Dict[str, Any]:
    """Fetch basic jetton metadata from TonAPI.

    Returns a small dict used across the bot:
      - name, symbol
      - decimals (int, default 9)
      - holders_count (best-effort)

    Note: Some DEX endpoints return amounts in minimal units, so decimals are critical.
    """
    out: Dict[str, Any] = {"name": "", "symbol": "", "decimals": 9, "holders_count": None}
    js = tonapi_get(f"{TONAPI_BASE}/v2/jettons/{jetton}")
    if not js:
        return out

    meta = js.get("metadata") or {}
    out["name"] = str((meta.get("name") if isinstance(meta, dict) else None) or js.get("name") or "").strip()
    out["symbol"] = str((meta.get("symbol") if isinstance(meta, dict) else None) or js.get("symbol") or "").strip()

    # decimals may be in metadata or root and can be a string
    dec = None
    if isinstance(meta, dict):
        dec = meta.get("decimals")
    if dec is None:
        dec = js.get("decimals")
    try:
        if dec is not None and str(dec).strip() != "":
            out["decimals"] = int(str(dec).strip())
    except Exception:
        out["decimals"] = 9

    # holders count
    try:
        for k in ("holders_count", "holders", "holdersCount", "total_holders", "holders_total"):
            hc = js.get(k)
            if hc is None:
                continue
            if isinstance(hc, int):
                out["holders_count"] = int(hc)
                break
            if isinstance(hc, str) and hc.isdigit():
                out["holders_count"] = int(hc)
                break
    except Exception:
        pass

    return out

def tonapi_jetton_holders_count(jetton: str) -> Optional[int]:
    """Best-effort holders count. Some TonAPI responses don't include holders_count on the main jetton endpoint."""
    try:
        data = tonapi_get(f"{TONAPI_BASE}/v2/jettons/{jetton}/holders", params={"limit": 1, "offset": 0})
        if not isinstance(data, dict):
            return None
        # TonAPI may return total/total_count in root
        for k in ("total", "total_count", "count", "holders", "holders_count"):
            v = data.get(k)
            if isinstance(v, int):
                return v
            if isinstance(v, str) and v.isdigit():
                return int(v)
        # or nested
        meta = data.get("metadata") or {}
        if isinstance(meta, dict):
            v = meta.get("total") or meta.get("total_count")
            if isinstance(v, int):
                return v
            if isinstance(v, str) and v.isdigit():
                return int(v)
        return None
    except Exception:
        return None


def tonapi_account_transactions(address: str, limit: int = 12) -> List[Dict[str, Any]]:
    js = tonapi_get(f"{TONAPI_BASE}/v2/blockchain/accounts/{address}/transactions", params={"limit": limit})
    txs = js.get("transactions") if isinstance(js, dict) else None
    return txs if isinstance(txs, list) else []

def tonapi_account_events(address: str, limit: int = 10) -> List[Dict[str, Any]]:
    js = tonapi_get(f"{TONAPI_BASE}/v2/accounts/{address}/events", params={"limit": limit})
    ev = js.get("events") if isinstance(js, dict) else None
    return ev if isinstance(ev, list) else []


def tonapi_account_events_subject(address: str, limit: int = 30) -> List[Dict[str, Any]]:
    """TonAPI account events with subject_only=true (less noise, better for DEX pool monitoring)."""
    js = tonapi_get(
        f"{TONAPI_BASE}/v2/accounts/{address}/events",
        params={"limit": limit, "subject_only": "true"},
    )
    ev = js.get("events") if isinstance(js, dict) else None
    return ev if isinstance(ev, list) else []

def tonapi_event_tx_hash(ev: Dict[str, Any]) -> str:
    """Best-effort extraction of a real tx hash from a TonAPI event."""
    if not isinstance(ev, dict):
        return ""
    eid = str(ev.get("event_id") or ev.get("id") or "").strip()
    if eid:
        return eid
    for act in (ev.get("actions") or []):
        if not isinstance(act, dict):
            continue
        bt = act.get("base_transactions") or act.get("baseTransactions") or []
        if isinstance(bt, dict):
            bt = list(bt.values())
        if not isinstance(bt, list):
            continue
        for t in bt:
            if not isinstance(t, dict):
                continue
            tid = t.get("transaction_id") or t.get("transactionId") or {}
            if isinstance(tid, dict):
                h = tid.get("hash") or tid.get("tx_hash") or tid.get("id")
                h = str(h or "").strip()
                if h:
                    return h
            h2 = t.get("hash") or t.get("tx_hash") or t.get("id")
            h2 = str(h2 or "").strip()
            if h2:
                return h2
    return ""

def tonapi_find_tx_hash_by_lt(account: str, lt: str, limit: int = 40) -> str:
    """Find a real transaction hash for an account by LT (with cache + adaptive scan).

    Some DEX trade APIs expose only LT; Tonviewer needs the real tx hash.
    We scan recent account transactions from TonAPI and match by LT.
    """
    account = str(account or "").strip()
    if not account:
        return ""
    try:
        lt_s = str(int(str(lt).strip()))
    except Exception:
        return ""

    cache_key = f"{account}:{lt_s}"
    now = int(time.time())
    # 24h cache
    cached = TX_LT_CACHE.get(cache_key)
    if cached and now - int(cached[0]) < 86400:
        return str(cached[1] or "").strip()

    # Adaptive scan sizes (fast -> deeper)
    scan_limits = [max(40, int(limit or 40)), 120, 300, 600]
    for lim in scan_limits:
        try:
            txs = tonapi_account_transactions(account, limit=lim)
            for tx in txs:
                if not isinstance(tx, dict):
                    continue
                tid = tx.get("transaction_id") or {}
                tx_lt = str(tid.get("lt") or tx.get("lt") or "").strip()
                if not tx_lt:
                    continue
                try:
                    if str(int(tx_lt)) != lt_s:
                        continue
                except Exception:
                    continue
                h = tid.get("hash") or tx.get("hash") or tx.get("tx_hash") or tx.get("id")
                h = str(h or "").strip()
                if h:
                    TX_LT_CACHE[cache_key] = (now, h)
                    return h
        except Exception:
            # brief retry on transient errors
            try:
                time.sleep(0.35)
            except Exception:
                pass
            continue

    return ""

def ton_usd_price() -> Optional[float]:
    """Fetch TON/USD price (cached). Used only when min_buy_unit == USD."""
    now = int(time.time())
    try:
        if TON_PRICE_CACHE.get("usd") is not None and now - int(TON_PRICE_CACHE.get("ts") or 0) < 120:
            return float(TON_PRICE_CACHE.get("usd"))
    except Exception:
        pass
    # Best-effort CoinGecko simple price
    try:
        r = requests.get(
            "https://api.coingecko.com/api/v3/simple/price",
            params={"ids": "the-open-network", "vs_currencies": "usd"},
            timeout=10,
            headers={"accept": "application/json", "user-agent": "SpyTONBuyBot/1.0"},
        )
        if r.status_code == 200:
            js = r.json()
            usd = js.get("the-open-network", {}).get("usd")
            if usd is not None:
                TON_PRICE_CACHE["usd"] = float(usd)
                TON_PRICE_CACHE["ts"] = now
                return float(usd)
    except Exception:
        pass
    return None

def min_buy_ton_threshold(settings: Dict[str, Any]) -> float:
    """Return the TON amount threshold implied by settings (TON or USD)."""
    unit = str(settings.get("min_buy_unit") or "TON").upper()
    if unit != "USD":
        try:
            return float(settings.get("min_buy_ton") or 0.0)
        except Exception:
            return 0.0
    try:
        usd_thr = float(settings.get("min_buy_usd") or 0.0)
    except Exception:
        usd_thr = 0.0
    if usd_thr <= 0:
        return 0.0
    p = ton_usd_price()
    if not p or p <= 0:
        # If we can't fetch TON price, don't block buys.
        return 0.0
    return usd_thr / p

# -------------------- DEX PAIR LOOKUP --------------------

# -------------------- GECKO TERMINAL --------------------
def gecko_get(path: str, params: Optional[dict] = None) -> Optional[dict]:
    """GeckoTerminal public API (best-effort)."""
    try:
        url = f"{GECKO_BASE}{path}"
        r = requests.get(
            url,
            params=params or {},
            headers={
                "accept": "application/json",
                "user-agent": "SpyTONBuyBot/1.0",
            },
            timeout=12,
        )
        if r.status_code != 200:
            return None
        return r.json()
    except Exception:
        return None

def gecko_token_info(token_addr: str) -> Optional[dict]:
    # token_addr should be a jetton master (EQ.. / UQ..)
    j = gecko_get(f"/networks/ton/tokens/{token_addr}")
    if not j or "data" not in j:
        return None
    attrs = (j.get("data") or {}).get("attributes") or {}
    return {
        "name": attrs.get("name") or "",
        "symbol": attrs.get("symbol") or "",
        "decimals": attrs.get("decimals"),
        "price_usd": attrs.get("price_usd"),
        "market_cap_usd": attrs.get("market_cap_usd") or attrs.get("fdv_usd"),
    }

def gecko_pool_info(pool_addr: str) -> Optional[dict]:
    j = gecko_get(f"/networks/ton/pools/{pool_addr}")
    if not j or "data" not in j:
        return None
    attrs = (j.get("data") or {}).get("attributes") or {}
    return {
        "price_usd": attrs.get("base_token_price_usd") or attrs.get("price_usd"),
        "liquidity_usd": attrs.get("reserve_in_usd") or attrs.get("liquidity_usd"),
        "fdv_usd": attrs.get("fdv_usd"),
        "market_cap_usd": attrs.get("market_cap_usd") or attrs.get("fdv_usd"),
        "name": attrs.get("name"),
    }

def gecko_terminal_pool_url(pool_addr: str) -> str:
    return f"https://www.geckoterminal.com/ton/pools/{pool_addr}"

def find_pair_for_token_on_dex(token_address: str, want_dex: str) -> Optional[str]:
    url = f"{DEX_TOKEN_URL}/{token_address}"
    try:
        res = requests.get(url, timeout=20)
        if res.status_code != 200:
            return None
        js = res.json()
        pairs = js.get("pairs") if isinstance(js, dict) else None
        if not isinstance(pairs, list):
            return None

        want = want_dex.lower()
        best_pair_id = None
        best_score = -1.0

        for p in pairs:
            if not isinstance(p, dict):
                continue
            dex_id = (p.get("dexId") or "").lower()
            chain_id = (p.get("chainId") or "").lower()
            if chain_id != "ton":
                continue

            if want == "stonfi" and "ston" not in dex_id:
                continue
            if want == "dedust" and "dedust" not in dex_id:
                continue

            base = p.get("baseToken") or {}
            quote = p.get("quoteToken") or {}
            base_sym = (base.get("symbol") or "").upper()
            quote_sym = (quote.get("symbol") or "").upper()
            if base_sym not in ("TON","WTON") and quote_sym not in ("TON","WTON"):
                continue

            pair_id = (p.get("pairAddress") or p.get("pairId") or p.get("pair") or "").strip()
            if not pair_id:
                u = (p.get("url") or "")
                if "/ton/" in u:
                    pair_id = u.split("/ton/")[-1].split("?")[0].strip()
            if not pair_id:
                continue

            liq = 0.0
            vol = 0.0
            try:
                liq = float(((p.get("liquidity") or {}).get("usd") or 0) or 0)
            except Exception:
                liq = 0.0
            try:
                vol = float(((p.get("volume") or {}).get("h24") or 0) or 0)
            except Exception:
                vol = 0.0

            score = liq * 1_000_000 + vol
            if score > best_score:
                best_score = score
                best_pair_id = pair_id

        return best_pair_id
    except Exception:
        return None

def find_stonfi_ton_pair_for_token(token_address: str) -> Optional[str]:
    return find_pair_for_token_on_dex(token_address, "stonfi")


def dex_token_info(token_address: str) -> Dict[str, str]:
    """Fallback metadata from Dexscreener.

    DexScreener often has token name/symbol even when TonAPI metadata is missing.
    We pick the TON pair with best liquidity/volume and read the non-TON side.
    """
    out = {"name": "", "symbol": ""}
    try:
        g = gecko_token_info(token_address)
        if g:
            out["name"] = g.get("name") or out["name"]
            out["symbol"] = g.get("symbol") or out["symbol"]
            if out["name"] or out["symbol"]:
                return out
        res = requests.get(f"{DEX_TOKEN_URL}/{token_address}", timeout=20)
        if res.status_code != 200:
            return out
        js = res.json()
        pairs = js.get("pairs") if isinstance(js, dict) else None
        if not isinstance(pairs, list) or not pairs:
            return out

        best = None
        best_score = -1.0
        for p in pairs:
            if not isinstance(p, dict):
                continue
            if (p.get("chainId") or "").lower() != "ton":
                continue
            base = p.get("baseToken") or {}
            quote = p.get("quoteToken") or {}
            base_sym = (base.get("symbol") or "").upper()
            quote_sym = (quote.get("symbol") or "").upper()
            if base_sym not in ("TON","WTON") and quote_sym not in ("TON","WTON"):
                continue
            liq = 0.0
            vol = 0.0
            try:
                liq = float(((p.get("liquidity") or {}).get("usd") or 0) or 0)
            except Exception:
                liq = 0.0
            try:
                vol = float(((p.get("volume") or {}).get("h24") or 0) or 0)
            except Exception:
                vol = 0.0
            score = liq * 1_000_000 + vol
            if score > best_score:
                best_score = score
                best = p

        if not best:
            best = pairs[0]

        base = best.get("baseToken") or {}
        quote = best.get("quoteToken") or {}
        base_addr = str(base.get("address") or "")
        quote_addr = str(quote.get("address") or "")
        # Choose the side that matches the token_address if possible
        tok = base if base_addr == token_address else (quote if quote_addr == token_address else None)
        if not tok:
            # Otherwise choose non-TON side
            tok = quote if (str(base.get("symbol") or "").upper() in ("TON","WTON")) else base
        out["name"] = str(tok.get("name") or "").strip()
        out["symbol"] = str(tok.get("symbol") or "").strip()
        return out
    except Exception:
        return out

# -------------------- BUY EXTRACTION (simplified from your working bot) --------------------
def _tx_hash(tx: Dict[str, Any]) -> str:
    """Extract a tx hash from various TonAPI / DEX payload shapes."""
    if not isinstance(tx, dict):
        return ""
    # Common flat keys
    h = tx.get("hash") or tx.get("tx_hash") or tx.get("transaction_hash") or tx.get("id")
    if isinstance(h, str) and h.strip():
        return h.strip()
    # TonAPI account tx shape: {"transaction_id": {"hash": "...", "lt": "..."}, ...}
    tid = tx.get("transaction_id")
    if isinstance(tid, dict):
        h2 = tid.get("hash") or tid.get("tx_hash")
        if isinstance(h2, str) and h2.strip():
            return h2.strip()
    # Some payloads wrap in {"event": {"tx_hash": ...}}
    ev = tx.get("event")
    if isinstance(ev, dict):
        h3 = ev.get("tx_hash") or ev.get("hash")
        if isinstance(h3, str) and h3.strip():
            return h3.strip()
    return ""

def _normalize_tx_hash_to_hex(h: Any) -> str:
    """Return a 64-char lowercase hex tx hash when possible.

    Tonviewer transaction link format: https://tonviewer.com/transaction/<hash as hex>.
    Some APIs return base64url-encoded 32-byte hashes; we convert those to hex.
    """
    if h is None:
        return ""
    s = str(h).strip()
    if not s:
        return ""

    # If a full URL was provided, try to extract a 64-hex hash from it.
    # Examples:
    #   https://tonviewer.com/transaction/<64hex>
    #   https://tonviewer.com/transaction/<64hex>?...
    #   https://tonviewer.com/tx/<64hex>
    try:
        m = re.search(r"([0-9a-fA-F]{64})", s)
        if m:
            return m.group(1).lower()
    except Exception:
        pass
    # Already hex?
    if re.fullmatch(r"[0-9a-fA-F]{64}", s):
        return s.lower()
    # If looks like base64url, try decode -> 32 bytes
    try:
        pad = "=" * ((4 - (len(s) % 4)) % 4)
        b = base64.urlsafe_b64decode(s + pad)
        if isinstance(b, (bytes, bytearray)) and len(b) == 32:
            return bytes(b).hex()
    except Exception:
        pass
    return ""

def _action_type(a: Dict[str, Any]) -> str:
    return str(a.get("type") or a.get("action") or a.get("name") or "")

def _short_addr(a: str) -> str:
    if not a:
        return ""
    if len(a) <= 10:
        return a
    return a[:4] + "…" + a[-4:]

def _to_float(x) -> float:
    try:
        return float(x)
    except Exception:
        return 0.0

def stonfi_extract_buys_from_tonapi_tx(tx: Dict[str, Any], token_addr: str) -> List[Dict[str, Any]]:
    """Heuristic buy parser from TonAPI tx actions.
    BUY = TON -> token_addr.
    """
    out: List[Dict[str, Any]] = []
    tx_hash = _tx_hash(tx)

    actions = tx.get("actions")
    if not isinstance(actions, list):
        actions = []

    for a in actions:
        if not isinstance(a, dict):
            continue
        payload = a.get(a.get('type') or a.get('action') or a.get('name'))
        aa = dict(a)
        if isinstance(payload, dict):
            aa.update(payload)

        at = _action_type(aa).lower()
        if "swap" not in at and "dex" not in at:
            continue

        dex = aa.get("dex")
        dex_name = ""
        if isinstance(dex, dict):
            dex_name = str(dex.get("name") or dex.get("title") or dex.get("id") or "").lower()
        if dex_name and "ston" not in dex_name:
            continue

        in_asset = aa.get("asset_in") or aa.get("assetIn") or aa.get("in") or {}
        out_asset = aa.get("asset_out") or aa.get("assetOut") or aa.get("out") or {}

        def _asset_addr(x: Any) -> str:
            if isinstance(x, dict):
                addr = x.get("address") or x.get("master") or x.get("jetton_master") or x.get("jettonMaster") or ""
                return str(addr)
            return ""

        def _is_ton_asset(x: Any) -> bool:
            if not isinstance(x, dict):
                return False
            t = str(x.get("type") or x.get("kind") or x.get("asset_type") or "").lower()
            if t == "ton":
                return True
            sym = str(x.get("symbol") or x.get("ticker") or x.get("name") or "").lower()
            if sym in ("ton","wton","pton"):
                return True
            # TonAPI sometimes stores ton as a dict without address, but with decimals=9
            if _asset_addr(x) in ("", None) and str(x).lower().find("ton") != -1:
                return True
            return False

        def _parse_amount(raw: Any, asset: Any) -> Optional[float]:
            """Handle both already-decimal numbers and raw on-chain integers."""
            if raw is None:
                return None
            # numeric
            if isinstance(raw, (int, float)):
                val = float(raw)
            else:
                s = str(raw).strip()
                if not s:
                    return None
                # if it looks like an integer string, keep as int-like
                if s.replace("-", "").isdigit():
                    try:
                        val = float(int(s))
                    except Exception:
                        val = _to_float(s)
                else:
                    val = _to_float(s)

            # scale if it looks like a raw integer
            dec = None
            if isinstance(asset, dict):
                d = asset.get("decimals")
                if isinstance(d, int):
                    dec = d
                else:
                    try:
                        dec = int(d)
                    except Exception:
                        dec = None

            if dec is not None:
                # If we got a big integer-ish value and no decimal point in original, assume raw.
                raw_s = str(raw).strip() if raw is not None else ""
                if raw_s and raw_s.replace("-", "").isdigit() and abs(val) >= 10 ** (dec + 2):
                    val = val / (10 ** dec)

            return val

        in_addr = _asset_addr(in_asset)
        out_addr = _asset_addr(out_asset)

        amt_in = _parse_amount(aa.get("amount_in") or aa.get("amountIn"), in_asset)
        amt_out = _parse_amount(aa.get("amount_out") or aa.get("amountOut"), out_asset)

        # BUY must be TON -> token
        if not (_is_ton_asset(in_asset) and str(out_addr) == str(token_addr)):
            continue

        ton_in = amt_in
        jet_out = amt_out
        if not ton_in or not jet_out:
            continue

        buyer = (aa.get("user") or aa.get("sender") or aa.get("initiator") or aa.get("from") or "")
        if isinstance(buyer, dict):
            buyer = buyer.get("address") or ""
        buyer = str(buyer)

        out.append({
            "tx": tx_hash,
            "buyer": buyer,
            "ton": ton_in,
            "token_amount": jet_out,
        })

    return out

def dedust_extract_buys_from_tonapi_event(ev: Dict[str, Any], token_addr: str) -> List[Dict[str, Any]]:
    """TonAPI events endpoint sometimes provides swap action info too."""
    out: List[Dict[str, Any]] = []
    # Prefer real transaction hash when present (hex or base64url). Fall back to event id.
    tx_hash = str(ev.get("hash") or ev.get("tx_hash") or ev.get("transaction_hash") or ev.get("id") or ev.get("event_id") or "")
    actions = ev.get("actions")
    if not isinstance(actions, list):
        actions = []
    for a in actions:
        if not isinstance(a, dict):
            continue
        at = _action_type(a).lower()
        if "swap" not in at and "dex" not in at:
            continue

        dex = a.get("dex")
        dex_name = ""
        if isinstance(dex, dict):
            dex_name = str(dex.get("name") or dex.get("title") or dex.get("id") or "").lower()
        if dex_name and "dedust" not in dex_name and "de dust" not in dex_name:
            continue

        # Normalize swap data: only treat as BUY when TON -> token_addr.
        in_asset = a.get("asset_in") or a.get("assetIn") or a.get("in") or {}
        out_asset = a.get("asset_out") or a.get("assetOut") or a.get("out") or {}
        amt_in_raw = a.get("amount_in") or a.get("amountIn") or a.get("in_amount") or a.get("amount") or 0
        amt_out_raw = a.get("amount_out") or a.get("amountOut") or a.get("out_amount") or 0

        def _is_ton_asset(x: Any) -> bool:
            if not isinstance(x, dict):
                return False
            t = str(x.get("type") or "").lower()
            sym = str(x.get("symbol") or x.get("ticker") or "").lower()
            return t == "ton" or sym == "ton"

        def _parse_amount(raw: Any, asset: Any) -> float:
            s = str(raw).strip()
            if s == "" or s.lower() in ("none", "null"):
                return 0.0
            if "." in s:
                return _to_float(s)
            if s.isdigit():
                dec = 0
                if isinstance(asset, dict):
                    try:
                        dec = int(asset.get("decimals") or 0)
                    except Exception:
                        dec = 0
                try:
                    return int(s) / (10 ** max(dec, 0))
                except Exception:
                    return _to_float(s)
            return _to_float(s)

        in_is_ton = _is_ton_asset(in_asset)
        out_addr = ""
        out_symbol = ""
        if isinstance(out_asset, dict):
            out_addr = str(out_asset.get("address") or out_asset.get("master") or "")
            out_symbol = str(out_asset.get("symbol") or out_asset.get("ticker") or "")

        if not (in_is_ton and out_addr == token_addr):
            continue

        ton_in = _parse_amount(amt_in_raw, in_asset)
        jet_out = _parse_amount(amt_out_raw, out_asset)
        if ton_in <= 0 or jet_out <= 0:
            continue

        buyer = (a.get("user") or a.get("sender") or a.get("initiator") or a.get("from") or "")
        if isinstance(buyer, dict):
            buyer = buyer.get("address") or ""
        buyer = str(buyer)

        out.append({"tx": tx_hash, "buyer": buyer, "ton": ton_in, "token": jet_out, "symbol": out_symbol})
    return out

# -------------------- UI --------------------
async def build_add_to_group_url(app: Application) -> str:
    # We try to discover bot username at runtime.
    try:
        me = await app.bot.get_me()
        if me and me.username:
            return f"https://t.me/{me.username}?startgroup=true"
    except Exception:
        pass
    return "https://t.me/"  # fallback

async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    if not chat:
        return

    if chat.type == "private":
        # Deep-link from group "Click Here!" button: /start cfg_<group_id>
        if context.args:
            arg = str(context.args[0])
            if arg.startswith("cfg_"):
                try:
                    group_id = int(arg.split("_", 1)[1])
                except Exception:
                    group_id = None
                if group_id:
                    # Auto-detect mode: user sends CA, we resolve STON.fi + DeDust pools automatically.
                    AWAITING[update.effective_user.id] = {"group_id": group_id, "stage": "CA", "dex": "both"}
                    lang = _get_user_lang(update.effective_user.id if update.effective_user else None)
                    await update.message.reply_text(
                        t("connected_title", lang) + "\n\n" + t("connected_desc", lang),
                        parse_mode="Markdown"
                    )
                    return
        add_url = await build_add_to_group_url(context.application)
        lang = _get_user_lang(update.effective_user.id if update.effective_user else None)
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton(t("btn_add_group", lang), url=add_url)],
            [InlineKeyboardButton(t("btn_cfg_token", lang), callback_data="CFG_PRIVATE")],
            [InlineKeyboardButton(t("btn_settings", lang), callback_data="SET_PRIVATE")],
            [InlineKeyboardButton(t("btn_language", lang), callback_data="LANG_PRIVATE")],
            [InlineKeyboardButton(t("btn_support", lang), url="https://t.me/SpyTonEco")],
        ])
        await update.message.reply_text(
            t("start_title", lang) + "\n" + t("start_desc", lang),
            reply_markup=kb,
            parse_mode="Markdown"
        )
    else:
        # In group, show group menu
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("⚙️ Configure Token", callback_data="CFG_GROUP")],
            [InlineKeyboardButton("⚙️ Token Settings", callback_data="TOKENSET_GROUP")],
            [InlineKeyboardButton("🛠 Settings", callback_data="SET_GROUP")],
            [InlineKeyboardButton("📊 Status", callback_data="STATUS_GROUP")],
            [InlineKeyboardButton("🗑 Remove Token", callback_data="REMOVE_GROUP")],
        ])
        await update.message.reply_text(
            "✅ *SpyTON BuyBot connected*\n\n"
            "Tap *Configure Token* to set the token, or type `ca` anytime to show the token address.",
            reply_markup=kb,
            parse_mode="Markdown"
        )


async def lang_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    user = update.effective_user
    if not chat or not user:
        return
    lang = _get_group_lang(chat.id, user.id)
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton(t("lang_en", lang), callback_data="LANG_SET_en")],
        [InlineKeyboardButton(t("lang_ru", lang), callback_data="LANG_SET_ru")],
    ])
    await update.message.reply_text(t("lang_title", lang), reply_markup=kb)

async def adset_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Owner-only: /adset 24h | Your ad text | https://yourlink"""
    if not update.message or not update.effective_user:
        return
    user = update.effective_user
    if not is_owner(user.id):
        return
    raw = " ".join(context.args or []).strip()
    if not raw:
        await update.message.reply_text("Usage: /adset 24h | Your ad text | https://yourlink")
        return
    parts = [p.strip() for p in raw.split("|")]
    if len(parts) < 2:
        await update.message.reply_text("Usage: /adset 24h | Your ad text | https://yourlink")
        return
    dur_s = parse_duration_to_seconds(parts[0])
    if not dur_s:
        await update.message.reply_text("Invalid duration. Example: 24h or 7d")
        return
    ad_text = parts[1]
    ad_link = parts[2] if len(parts) >= 3 else ""
    if len(ad_text) > 160:
        await update.message.reply_text("Ad text too long. Keep it under ~160 characters.")
        return
    if ad_link and not re.match(r"^https?://", ad_link, re.IGNORECASE):
        await update.message.reply_text("Ad link must start with http:// or https://")
        return
    ADS_STATE["active_until"] = int(time.time()) + int(dur_s)
    ADS_STATE["text"] = ad_text
    ADS_STATE["link"] = ad_link
    ADS_STATE["set_by"] = int(user.id)
    save_ads()
    await update.message.reply_text("✅ Ad set. It will appear under every buy (channel + groups).")


async def adclear_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Owner-only: /adclear (reverts to default line)"""
    if not update.message or not update.effective_user:
        return
    if not is_owner(update.effective_user.id):
        return
    ADS_STATE["active_until"] = 0
    ADS_STATE["text"] = ""
    ADS_STATE["link"] = ""
    save_ads()
    await update.message.reply_text("✅ Paid ad cleared. Default line restored.")


async def adstatus_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Owner-only: /adstatus (shows current ad + time left)"""
    if not update.message or not update.effective_user:
        return
    if not is_owner(update.effective_user.id):
        return
    text, link, left = active_ad()
    if left <= 0:
        await update.message.reply_text(f"Current: DEFAULT\n{text}\n{link}")
        return
    hrs = left // 3600
    mins = (left % 3600) // 60
    await update.message.reply_text(
        "Current: PAID\n"
        f"{text}\n{link}\n"
        f"Time left: {hrs}h {mins}m"
    )


# -------------------- OWNER-ONLY: /addtoken (global tracking) --------------------
def _infer_dex_mode_from_text(t: str) -> str:
    tl = (t or "").lower()
    if "stonfi" in tl or "ston" in tl:
        if "dedust" in tl:
            return "both"
        return "stonfi"
    if "dedust" in tl:
        return "dedust"
    return "both"

def _extract_symbol_hint(rest: str) -> str:
    # pick the first sane token-like word as symbol (optional)
    for w in re.split(r"\s+", (rest or "").strip()):
        ww = re.sub(r"[^A-Za-z0-9]", "", w or "")
        if not ww:
            continue
        if 1 <= len(ww) <= 12:
            return ww.upper()
    return ""

async def addtoken_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Owner-only: /addtoken <CA/link> [SYMBOL] [telegram_link]

    Adds a token to GLOBAL_TOKENS so buys are tracked and posted in the trending channel even
    if no project added the bot to their group.
    """
    if not update.message or not update.effective_user:
        return
    user = update.effective_user
    if not is_owner(user.id):
        return

    raw = (update.message.text or "").strip()
    args_text = " ".join(context.args or []).strip()
    if not args_text:
        await update.message.reply_text(
            "Usage:\n"
            "/addtoken <TOKEN_CA or pool/link> [SYMBOL] [https://t.me/YourToken]\n\n"
            "Example:\n"
            "/addtoken EQ... TFT https://t.me/SpyTonTrending"
        )
        return

    dex_mode = _infer_dex_mode_from_text(raw)

    # Resolve jetton address from CA or supported links
    jetton = await _to_thread(resolve_jetton_from_text_sync, args_text)
    if not jetton:
        await update.message.reply_text("Could not detect a token address. Paste the jetton CA or a supported pool link.")
        return

    # Optional TG link
    tg_url = ""
    m_tg = re.search(r"https?://t\.me/[A-Za-z0-9_]{3,}(?:\S*)?", args_text)
    if m_tg:
        tg_url = m_tg.group(0).strip()

    # Optional symbol hint
    rest = re.sub(re.escape(jetton), "", args_text)
    rest = re.sub(r"https?://t\.me/\S+", "", rest)
    sym_hint = _extract_symbol_hint(rest)

    # Build token record (reuse the same logic as group setup)
    gk = gecko_token_info(jetton)
    name = (gk.get("name") or "").strip() if gk else ""
    sym = (gk.get("symbol") or "").strip() if gk else ""
    if not name and not sym:
        info = tonapi_jetton_info(jetton)
        name = (info.get("name") or "").strip()
        sym = (info.get("symbol") or "").strip()
    if not name and not sym:
        dx = dex_token_info(jetton)
        name = (dx.get("name") or "").strip()
        sym = (dx.get("symbol") or "").strip()
    if sym_hint:
        sym = sym_hint

    # Seed holders and decimals
    holders_seed: Optional[int] = None
    try:
        info_h = tonapi_jetton_info(jetton)
        hh = info_h.get("holders_count")
        if hh is not None:
            holders_seed = int(hh)
    except Exception:
        pass
    if holders_seed is None:
        try:
            hh2 = tonapi_jetton_holders_count(jetton)
            if hh2 is not None:
                holders_seed = int(hh2)
        except Exception:
            pass
    decimals_seed: int = 9
    try:
        meta_j = get_jetton_meta(jetton)
        decimals_seed = int(meta_j.get("decimals") or 9)
    except Exception:
        decimals_seed = 9

    dm = (dex_mode or "both").lower().strip()
    ston_pool = find_stonfi_ton_pair_for_token(jetton) if dm in ("both", "ston", "stonfi") else None
    dedust_pool = find_dedust_ton_pair_for_token(jetton) if dm in ("both", "dedust") else None

    # Store token for global tracking
    tok = {
        "_scope": "global",
        "address": jetton,
        "dex_mode": ("auto" if dm == "both" else dm),
        "name": name,
        "symbol": sym,
        "decimals": int(decimals_seed) if str(decimals_seed).isdigit() else 9,
        "holders": holders_seed,
        "ston_pool": ston_pool,
        "dedust_pool": dedust_pool,
        "set_at": int(time.time()),
        "init_done": True,
        "paused": False,
        "last_ston_tx": None,
        "last_dedust_trade": None,
        "ston_last_block": None,
        "ignore_before_ts": int(time.time()),
        "burst": {"window_start": int(time.time()), "count": 0},
        "telegram": tg_url.strip() if tg_url else "",
    }
    GLOBAL_TOKENS[str(jetton)] = tok
    save_groups()  # also saves GLOBAL_TOKENS

    # Warmup seen in the TRENDING channel so old swaps aren't spammed
    if TRENDING_POST_CHAT_ID:
        try:
            await warmup_seen_for_chat(int(TRENDING_POST_CHAT_ID), ston_pool, dedust_pool)
        except Exception:
            pass

    # Confirmation message like the screenshot
    dex_label = "both"
    if ston_pool and not dedust_pool:
        dex_label = "stonfi"
    elif dedust_pool and not ston_pool:
        dex_label = "dedust"
    elif ston_pool and dedust_pool:
        dex_label = "stonfi + dedust"

    disp = (sym or name or "TOKEN").strip()
    lines = [
        f"✅ Added {disp}",
        f"DEX: {dex_label}",
        f"Token:\n{jetton}",
    ]
    if ston_pool or dedust_pool:
        lines.append(f"Pair/Pool: {ston_pool or dedust_pool}")
    if tg_url:
        lines.append(f"Telegram: {tg_url}")
    lines.append("\nNotes:\n• Buys will post in the trending channel even if the project didn't add the bot to their group.")
    await update.message.reply_text("\n".join(lines), disable_web_page_preview=True)


async def tokens_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Owner-only: list tokens manually added via /addtoken."""
    if not is_owner(update.effective_user):
        return

    if not GLOBAL_TOKENS:
        await update.message.reply_text("No manually-added tokens yet.")
        return

    # Sort by symbol for readability
    items = sorted(GLOBAL_TOKENS.items(), key=lambda kv: (str(kv[1].get("symbol") or ""), kv[0]))
    lines: List[str] = ["🧾 Manual tokens (added by you):"]

    def _short(addr: str) -> str:
        if not addr:
            return "—"
        return addr[:6] + "..." + addr[-4:]

    for jetton, info in items:
        sym = str(info.get("symbol") or "TOKEN")
        tg = str(info.get("telegram") or "").strip()
        dex = str(info.get("dex") or "—")
        pool = str(info.get("stonfi_pool") or info.get("dedust_pool") or "")

        # Make symbol clickable to telegram if available
        if tg.startswith("https://t.me/") or tg.startswith("t.me/"):
            tg_url = tg if tg.startswith("https://") else "https://" + tg
            sym_disp = f"<a href=\"{tg_url}\">{html.escape(sym)}</a>"
        else:
            sym_disp = html.escape(sym)

        lines.append(f"• {sym_disp} — {html.escape(_short(jetton))} | {html.escape(dex)} | pool: {html.escape(_short(pool))}")

    lines.append("\nDelete: /deltoken <jetton_ca>  (or /delpair <jetton_ca or pool>)")
    await update.message.reply_text("\n".join(lines), parse_mode=ParseMode.HTML, disable_web_page_preview=True)


async def deltoken_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Owner-only: remove a manually-added token so it no longer shows in leaderboard/channel."""
    if not is_owner(update.effective_user):
        return

    if not context.args:
        await update.message.reply_text("Usage: /deltoken <jetton_ca>")
        return

    jetton = context.args[0].strip()
    if jetton in GLOBAL_TOKENS:
        removed = GLOBAL_TOKENS.pop(jetton)
        save_groups()
        await update.message.reply_text(f"✅ Removed {removed.get('symbol') or 'token'} ({jetton})")
        return

    await update.message.reply_text("❌ Token not found in manual list. Use /tokens to see what you added.")


async def delpair_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Owner-only: alias to delete by jetton OR by pool/pair address."""
    if not is_owner(update.effective_user):
        return

    if not context.args:
        await update.message.reply_text("Usage: /delpair <jetton_ca OR pool_address>")
        return

    key = context.args[0].strip()

    # Direct jetton match
    if key in GLOBAL_TOKENS:
        removed = GLOBAL_TOKENS.pop(key)
        save_groups()
        await update.message.reply_text(f"✅ Removed {removed.get('symbol') or 'token'} ({key})")
        return

    # Try find by pool/pair address
    for jetton, info in list(GLOBAL_TOKENS.items()):
        if key == info.get("stonfi_pool") or key == info.get("dedust_pool"):
            removed = GLOBAL_TOKENS.pop(jetton)
            save_groups()
            await update.message.reply_text(
                f"✅ Removed {removed.get('symbol') or 'token'} (by pool match)\nJetton: {jetton}\nPool: {key}"
            )
            return

    await update.message.reply_text("❌ Not found. Use /tokens to see your current manual tokens.")

async def on_button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q:
        return
    await q.answer()
    chat = q.message.chat if q.message else update.effective_chat
    user = update.effective_user
    if not chat or not user:
        return

    data = q.data or ""
    if data == "LANG_PRIVATE":
        lang = _get_user_lang(user.id)
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton(t("lang_en", lang), callback_data="LANG_SET_en")],
            [InlineKeyboardButton(t("lang_ru", lang), callback_data="LANG_SET_ru")],
        ])
        await q.edit_message_text(t("lang_title", lang), reply_markup=kb)
        return

    if data.startswith("LANG_SET_"):
        lang_code = data.split("_", 2)[2] if "_" in data else "en"
        set_user_lang(user.id, lang_code)
        new_lang = _get_user_lang(user.id)
        try:
            await q.answer(t("lang_set_ok_ru", new_lang) if new_lang=="ru" else t("lang_set_ok", new_lang), show_alert=False)
        except Exception:
            pass
        add_url = await build_add_to_group_url(context.application)
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton(t("btn_add_group", new_lang), url=add_url)],
            [InlineKeyboardButton(t("btn_cfg_token", new_lang), callback_data="CFG_PRIVATE")],
            [InlineKeyboardButton(t("btn_settings", new_lang), callback_data="SET_PRIVATE")],
            [InlineKeyboardButton(t("btn_language", new_lang), callback_data="LANG_PRIVATE")],
            [InlineKeyboardButton(t("btn_support", new_lang), url="https://t.me/SpyTonEco")],
        ])
        await q.edit_message_text(t("start_title", new_lang) + "\n" + t("start_desc", new_lang), reply_markup=kb, parse_mode="Markdown")
        return

    if data in ("CFG_PRIVATE","SET_PRIVATE"):
        # In private we configure a target group via last used group in AWAITING or ask user to do it in group
        await q.edit_message_text(
            "To configure a group:\n"
            "1) Add the bot to your group.\n"
            "2) In that group, tap *Configure Token*.",
            parse_mode="Markdown"
        )
        return

    if data == "CFG_GROUP":
        # Crypton-style: group button opens DM config (deep-link) so you don't have to reply in group.
        if not await is_admin(context.bot, chat.id, user.id):
            await q.answer("Admins only.", show_alert=True)
            return
        bot_username = await get_bot_username(context.bot)
        deep = f"https://t.me/{bot_username}?start=cfg_{chat.id}"
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("Click Here!", url=deep)]])
        await q.message.reply_text(
            "To continue, click *Click Here!* and send your token CA in DM.",
            parse_mode="Markdown",
            reply_markup=kb,
        )
        await q.answer()
        return


    # DEX selection in private DM config
    if data.startswith("DEX_STON_") or data.startswith("DEX_DEDUST_"):
        try:
            group_id = int(data.split("_", 2)[2])
        except Exception:
            group_id = None
        if not group_id:
            return
        dex = "ston" if data.startswith("DEX_STON_") else "dedust"
        AWAITING[user.id] = {"group_id": group_id, "stage": "CA", "dex": dex}
        await q.edit_message_text(
            "Send the token CA now (EQ… / UQ…) or a supported link (GT/DexS/STON/DeDust).\n\n"
            "Optional: add the token Telegram link after the CA.\n"
            "Example: EQ... https://t.me/YourTokenTG"
        )
        return

    if data == "TOKENSET_GROUP":
        if not await is_admin(context.bot, chat.id, user.id):
            await q.answer("Admins only.", show_alert=True)
            return
        await send_token_settings(chat.id, context, q.message)
        return

    if data.startswith("TS_"):
        if not await is_admin(context.bot, chat.id, user.id):
            await q.answer("Admins only.", show_alert=True)
            return
        await handle_token_settings_button(chat.id, data, update, context)
        return

    if data == "SET_GROUP":
        # Settings should open the Crypton-style module menu (Token Settings),
        # not the legacy quick-toggles panel.
        if not await is_admin(context.bot, chat.id, user.id):
            await q.answer("Admins only.", show_alert=True)
            return
        await send_token_settings(chat.id, context, q.message)
        return

    if data.startswith("TOG_"):
        if not await is_admin(context.bot, chat.id, user.id):
            await q.answer("Admins only.", show_alert=True)
            return
        g = get_group(chat.id)
        s = g["settings"]
        if data == "TOG_STON":
            s["enable_ston"] = not bool(s.get("enable_ston", True))
        elif data == "TOG_DEDUST":
            prev = bool(s.get("enable_dedust", True))
            s["enable_dedust"] = not prev
            # If turning ON, baseline DeDust so it never dumps old buys.
            if (not prev) and bool(s["enable_dedust"]):
                tok = g.get("token") if isinstance(g, dict) else None
                if isinstance(tok, dict) and tok.get("dedust_pool"):
                    try:
                        await warmup_seen_for_chat(chat.id, None, tok.get("dedust_pool"))
                    except Exception:
                        pass
                    tok["init_done"] = False
                    save_groups()
        elif data == "TOG_BURST":
            s["burst_mode"] = not bool(s.get("burst_mode", True))
        elif data == "TOG_STRENGTH":
            s["strength_on"] = not bool(s.get("strength_on", True))
        elif data == "TOG_IMAGE":
            s["buy_image_on"] = not bool(s.get("buy_image_on", False))
        save_groups()
        await send_settings(chat.id, context, q.message, edit=True)
        return

    if data == "IMG_SET":
        if not await is_admin(context.bot, chat.id, user.id):
            await q.answer("Admins only.", show_alert=True)
            return
        # Next photo from this admin will be saved as the buy image for this group.
        AWAITING_IMAGE[user.id] = chat.id
        await q.message.reply_text("Send the *buy image* now as a Telegram photo (not a file).", parse_mode="Markdown")
        return

    if data == "IMG_CLEAR":
        if not await is_admin(context.bot, chat.id, user.id):
            await q.answer("Admins only.", show_alert=True)
            return
        g = get_group(chat.id)
        g["settings"]["buy_image_file_id"] = ""
        g["settings"]["buy_image_on"] = False
        save_groups()
        await send_settings(chat.id, context, q.message, edit=True)
        return

    if data.startswith("MIN_"):
        if not await is_admin(context.bot, chat.id, user.id):
            await q.answer("Admins only.", show_alert=True)
            return
        g = get_group(chat.id)
        s = g["settings"]
        val = float(data.split("_",1)[1])
        s["min_buy_ton"] = val
        save_groups()
        await send_settings(chat.id, context, q.message, edit=True)
        return

    if data.startswith("STEP_"):
        if not await is_admin(context.bot, chat.id, user.id):
            await q.answer("Admins only.", show_alert=True)
            return
        g = get_group(chat.id)
        s = g["settings"]
        step = float(data.split("_", 1)[1])
        s["strength_step_ton"] = step
        save_groups()
        await send_settings(chat.id, context, q.message, edit=True)
        return

    if data.startswith("MAX_"):
        if not await is_admin(context.bot, chat.id, user.id):
            await q.answer("Admins only.", show_alert=True)
            return
        g = get_group(chat.id)
        s = g["settings"]
        mx = int(data.split("_", 1)[1])
        s["strength_max"] = mx
        save_groups()
        await send_settings(chat.id, context, q.message, edit=True)
        return

    if data.startswith("EMO_"):
        if not await is_admin(context.bot, chat.id, user.id):
            await q.answer("Admins only.", show_alert=True)
            return
        g = get_group(chat.id)
        s = g["settings"]
        if data == "EMO_GREEN":
            s["strength_emoji"] = "🟢"
        elif data == "EMO_PLANE":
            s["strength_emoji"] = "✈️"
        elif data == "EMO_DIAMOND":
            s["strength_emoji"] = "💎"
        save_groups()
        await send_settings(chat.id, context, q.message, edit=True)
        return

    if data.startswith("SPAM_"):
        if not await is_admin(context.bot, chat.id, user.id):
            await q.answer("Admins only.", show_alert=True)
            return
        g = get_group(chat.id)
        s = g["settings"]
        s["anti_spam"] = data.split("_",1)[1]
        save_groups()
        await send_settings(chat.id, context, q.message, edit=True)
        return

    if data == "STATUS_GROUP":
        await send_status(chat.id, context, q.message)
        return

    if data == "REMOVE_GROUP":
        if not await is_admin(context.bot, chat.id, user.id):
            await q.answer("Admins only.", show_alert=True)
            return
        g = get_group(chat.id)
        if not g.get("token"):
            await q.message.reply_text("No token configured for this group.")
            return
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("✅ Remove", callback_data="CONFIRM_REMOVE")],
            [InlineKeyboardButton("❌ Cancel", callback_data="CANCEL_REMOVE")]
        ])
        await q.message.reply_text("Remove the current token for this group?", reply_markup=kb)
        return

    if data == "CONFIRM_REMOVE":
        if not await is_admin(context.bot, chat.id, user.id):
            await q.answer("Admins only.", show_alert=True)
            return
        g = get_group(chat.id)
        g["token"] = None
        save_groups()
        await q.message.reply_text("✅ Token removed.")
        return

    if data == "CANCEL_REMOVE":
        await q.message.reply_text("Cancelled.")
        return

async def send_settings(chat_id: int, context: ContextTypes.DEFAULT_TYPE, msg, edit: bool=False):
    g = get_group(chat_id)
    s = g["settings"]
    ston = "ON ✅" if s.get("enable_ston", True) else "OFF ❌"
    dedust = "ON ✅" if s.get("enable_dedust", True) else "OFF ❌"
    burst = "ON ✅" if s.get("burst_mode", True) else "OFF ❌"
    anti = (s.get("anti_spam") or "MED").upper()
    min_buy = s.get("min_buy_ton", 0.0)

    strength = "ON ✅" if s.get("strength_on", True) else "OFF ❌"
    strength_step = float(s.get("strength_step_ton") or 5.0)
    strength_max = int(s.get("strength_max") or 30)
    strength_emoji = str(s.get("strength_emoji") or "🟢")

    img_on = bool(s.get("buy_image_on", False))
    img_set = bool((s.get("buy_image_file_id") or "").strip())
    img = "ON ✅" if img_on else "OFF ❌"
    img_note = "set" if img_set else "not set"

    text = (
        "*SpyTON BuyBot Settings*\n"
        f"• STON.fi: *{ston}*\n"
        f"• DeDust: *{dedust}*\n"
        f"• Burst mode: *{burst}*\n"
        f"• Anti-spam: *{anti}*\n"
        f"• Min buy (TON): *{min_buy}*\n"
        f"• Buy strength: *{strength}* ({strength_emoji}, step {strength_step} TON, max {strength_max})\n"
        f"• Buy image: *{img}* ({img_note})\n"
    )
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton(f"STON.fi: {ston}", callback_data="TOG_STON"),
         InlineKeyboardButton(f"DeDust: {dedust}", callback_data="TOG_DEDUST")],
        [InlineKeyboardButton(f"Burst: {burst}", callback_data="TOG_BURST")],
        [InlineKeyboardButton(f"Strength: {strength}", callback_data="TOG_STRENGTH"),
         InlineKeyboardButton(f"Image: {img}", callback_data="TOG_IMAGE")],
        [InlineKeyboardButton("🖼 Set Buy Image", callback_data="IMG_SET"),
         InlineKeyboardButton("🗑 Clear Image", callback_data="IMG_CLEAR")],
        [InlineKeyboardButton("Min 0", callback_data="MIN_0"),
         InlineKeyboardButton("0.1", callback_data="MIN_0.1"),
         InlineKeyboardButton("0.5", callback_data="MIN_0.5"),
         InlineKeyboardButton("1", callback_data="MIN_1"),
         InlineKeyboardButton("5", callback_data="MIN_5")],
        [InlineKeyboardButton("Step 1", callback_data="STEP_1"),
         InlineKeyboardButton("5", callback_data="STEP_5"),
         InlineKeyboardButton("10", callback_data="STEP_10"),
         InlineKeyboardButton("20", callback_data="STEP_20")],
        [InlineKeyboardButton("Max 10", callback_data="MAX_10"),
         InlineKeyboardButton("15", callback_data="MAX_15"),
         InlineKeyboardButton("30", callback_data="MAX_30")],
        [InlineKeyboardButton("🟢", callback_data="EMO_GREEN"),
         InlineKeyboardButton("✈️", callback_data="EMO_PLANE"),
         InlineKeyboardButton("💎", callback_data="EMO_DIAMOND")],
        [InlineKeyboardButton("Anti: LOW", callback_data="SPAM_LOW"),
         InlineKeyboardButton("MED", callback_data="SPAM_MED"),
         InlineKeyboardButton("HIGH", callback_data="SPAM_HIGH")],
    ])
    if edit:
        try:
            await msg.edit_text(text, reply_markup=kb, parse_mode="Markdown")
            return
        except Exception:
            pass
    await msg.reply_text(text, reply_markup=kb, parse_mode="Markdown")


# -------------------- Crypton-style Token Settings (modules) --------------------
async def send_token_settings(chat_id: int, context: ContextTypes.DEFAULT_TYPE, msg, edit: bool=False):
    g = get_group(chat_id)
    tok = g.get("token") if isinstance(g, dict) else None
    s = g.get("settings") or DEFAULT_SETTINGS

    token_name = "None"
    if isinstance(tok, dict):
        token_name = (tok.get("symbol") or tok.get("name") or "TOKEN").strip()
    paused = bool(tok.get("paused", False)) if isinstance(tok, dict) else False

    unit = str(s.get("min_buy_unit") or "TON").upper()
    min_buy_disp = f"{float(s.get('min_buy_ton') or 0.0)} TON" if unit != "USD" else f"${float(s.get('min_buy_usd') or 0.0)}"

    text = (
        "*Token Settings*\n"
        f"• Token: *{html.escape(token_name)}*\n"
        f"• Min Buy: *{min_buy_disp}*\n"
        f"• Status: *{'PAUSED ⏸️' if paused else 'RUNNING ✅'}*\n\n"
        "Choose a module:"
    )

    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("Min Buy", callback_data="TS_MIN"),
         InlineKeyboardButton("Emoji", callback_data="TS_EMO")],
        [InlineKeyboardButton("Manage Media", callback_data="TS_MEDIA"),
         InlineKeyboardButton("Social Links", callback_data="TS_SOC")],
        [InlineKeyboardButton("Layout", callback_data="TS_LAYOUT"),
         InlineKeyboardButton("Bot Preview", callback_data="TS_PREVIEW")],
        [InlineKeyboardButton("Pause / Resume", callback_data="TS_PAUSE"),
         InlineKeyboardButton("Remove Token", callback_data="TS_REMOVE")],
        [InlineKeyboardButton("⬅️ Back", callback_data="TS_BACK")],
    ])

    if edit:
        await msg.edit_text(text, parse_mode="Markdown", reply_markup=kb, disable_web_page_preview=True)
    else:
        await msg.reply_text(text, parse_mode="Markdown", reply_markup=kb, disable_web_page_preview=True)

async def handle_token_settings_button(chat_id: int, data: str, update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    msg = q.message if q else None
    g = get_group(chat_id)
    tok = g.get("token") if isinstance(g, dict) else None
    s = g.get("settings") or DEFAULT_SETTINGS

    if not msg:
        return

    # Back to group menu
    if data == "TS_BACK":
        await send_token_settings(chat_id, context, msg, edit=True)
        return

    # ----- Min Buy -----
    if data == "TS_MIN":
        unit = str(s.get("min_buy_unit") or "TON").upper()
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton(f"Unit: TON {'✅' if unit!='USD' else ''}", callback_data="TS_MIN_UNIT_TON"),
             InlineKeyboardButton(f"Unit: USD {'✅' if unit=='USD' else ''}", callback_data="TS_MIN_UNIT_USD")],
            [InlineKeyboardButton("0", callback_data="TS_MIN_VAL_0"),
             InlineKeyboardButton("0.1", callback_data="TS_MIN_VAL_0.1"),
             InlineKeyboardButton("1", callback_data="TS_MIN_VAL_1"),
             InlineKeyboardButton("5", callback_data="TS_MIN_VAL_5")],
            [InlineKeyboardButton("10", callback_data="TS_MIN_VAL_10"),
             InlineKeyboardButton("25", callback_data="TS_MIN_VAL_25"),
             InlineKeyboardButton("50", callback_data="TS_MIN_VAL_50")],
            [InlineKeyboardButton("⬅️ Back", callback_data="TS_BACK")],
        ])
        note = "TON threshold uses *TON spent*. USD threshold uses *TON/USD* price (best-effort)."
        await msg.edit_text(f"*Min Buy*\nCurrent unit: *{unit}*\n\n{note}", parse_mode="Markdown", reply_markup=kb, disable_web_page_preview=True)
        return

    if data.startswith("TS_MIN_UNIT_"):
        unit = data.split("_")[-1]
        s["min_buy_unit"] = "USD" if unit == "USD" else "TON"
        save_groups()
        await send_token_settings(chat_id, context, msg, edit=True)
        return

    if data.startswith("TS_MIN_VAL_"):
        val = float(data.split("_", 3)[3])
        if str(s.get("min_buy_unit") or "TON").upper() == "USD":
            s["min_buy_usd"] = val
        else:
            s["min_buy_ton"] = val
        save_groups()
        await send_token_settings(chat_id, context, msg, edit=True)
        return

    # ----- Emoji / Strength -----
    if data == "TS_EMO":
        strength = bool(s.get("strength_on", True))
        emo = str(s.get("strength_emoji") or "🟢")
        step = float(s.get("strength_step_ton") or 5.0)
        mx = int(s.get("strength_max") or 30)
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton(f"Strength: {'ON ✅' if strength else 'OFF ❌'}", callback_data="TS_EMO_TOG")],
            [InlineKeyboardButton("🟢", callback_data="TS_EMO_SET_GREEN"),
             InlineKeyboardButton("💎", callback_data="TS_EMO_SET_DIAMOND"),
             InlineKeyboardButton("✈️", callback_data="TS_EMO_SET_PLANE")],
            [InlineKeyboardButton("Custom Emoji", callback_data="TS_EMO_CUSTOM")],
            [InlineKeyboardButton("Step 1", callback_data="TS_EMO_STEP_1"),
             InlineKeyboardButton("5", callback_data="TS_EMO_STEP_5"),
             InlineKeyboardButton("10", callback_data="TS_EMO_STEP_10")],
            [InlineKeyboardButton("Max 15", callback_data="TS_EMO_MAX_15"),
             InlineKeyboardButton("30", callback_data="TS_EMO_MAX_30"),
             InlineKeyboardButton("45", callback_data="TS_EMO_MAX_45")],
            [InlineKeyboardButton("⬅️ Back", callback_data="TS_BACK")],
        ])
        await msg.edit_text(
            f"*Emoji / Buy Strength*\n• Emoji: *{emo}*\n• Step: *{step} TON*\n• Max: *{mx}*",
            parse_mode="Markdown",
            reply_markup=kb,
            disable_web_page_preview=True
        )
        return

    if data == "TS_EMO_CUSTOM":
        # ask admin to send an emoji or a Telegram premium <tg-emoji ...> tag
        AWAITING_CUSTOM_EMOJI[update.effective_user.id] = chat_id
        await msg.edit_text(
            "*Custom Buy Emoji*\n\n"
            "Send your emoji now.\n"
            "• Normal emoji: 🟢 or 🐥\n"
            "• Premium emoji: send the <tg-emoji emoji-id=...> tag (HTML)\n\n"
            "After sending, all buy posts will use it.",
            parse_mode="Markdown",
            disable_web_page_preview=True,
        )
        return

    if data == "TS_EMO_TOG":
        s["strength_on"] = not bool(s.get("strength_on", True))
        save_groups()
        await send_token_settings(chat_id, context, msg, edit=True)
        return

    if data.startswith("TS_EMO_SET_"):
        k = data.split("_")[-1]
        s["strength_emoji"] = "🟢" if k == "GREEN" else ("💎" if k == "DIAMOND" else "✈️")
        save_groups()
        await send_token_settings(chat_id, context, msg, edit=True)
        return

    if data.startswith("TS_EMO_STEP_"):
        s["strength_step_ton"] = float(data.split("_")[-1])
        save_groups()
        await send_token_settings(chat_id, context, msg, edit=True)
        return

    if data.startswith("TS_EMO_MAX_"):
        s["strength_max"] = int(data.split("_")[-1])
        save_groups()
        await send_token_settings(chat_id, context, msg, edit=True)
        return

    # ----- Media -----
    if data == "TS_MEDIA":
        img_on = bool(s.get("buy_image_on", False))
        img_set = bool((s.get("buy_image_file_id") or "").strip())
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton(f"Image: {'ON ✅' if img_on else 'OFF ❌'}", callback_data="TS_MEDIA_TOG")],
            [InlineKeyboardButton("🖼 Set Buy Image", callback_data="IMG_SET"),
             InlineKeyboardButton("🗑 Clear Image", callback_data="IMG_CLEAR")],
            [InlineKeyboardButton("⬅️ Back", callback_data="TS_BACK")],
        ])
        await msg.edit_text(
            f"*Manage Media*\n• Image mode: *{'ON' if img_on else 'OFF'}*\n• Image: *{'set' if img_set else 'not set'}*",
            parse_mode="Markdown",
            reply_markup=kb,
            disable_web_page_preview=True
        )
        return

    if data == "TS_MEDIA_TOG":
        s["buy_image_on"] = not bool(s.get("buy_image_on", False))
        save_groups()
        await send_token_settings(chat_id, context, msg, edit=True)
        return

    # ----- Social Links -----
    if data == "TS_SOC":
        tg = ""
        if isinstance(tok, dict):
            tg = str(tok.get("telegram") or "").strip()
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("Set Telegram Link", callback_data="TS_SOC_SET_TG")],
            [InlineKeyboardButton("Clear Telegram", callback_data="TS_SOC_CLR_TG")],
            [InlineKeyboardButton("⬅️ Back", callback_data="TS_BACK")],
        ])
        await msg.edit_text(
            "*Social Links*\n"
            f"Telegram: {tg if tg else '—'}\n\n"
            "To set: tap *Set Telegram Link* then send the link in DM.",
            parse_mode="Markdown",
            reply_markup=kb,
            disable_web_page_preview=True
        )
        return

    if data == "TS_SOC_SET_TG":
        # Ask in DM for safety (Telegram blocks some group flows)
        AWAITING_SOCIAL[update.effective_user.id] = {"chat_id": chat_id, "field": "telegram"}
        await msg.reply_text("Send the token Telegram link now in DM (example: https://t.me/YourToken).")
        return

    if data == "TS_SOC_CLR_TG":
        if isinstance(tok, dict):
            tok["telegram"] = ""
            save_groups()
        await send_token_settings(chat_id, context, msg, edit=True)
        return

    # ----- Layout -----
    if data == "TS_LAYOUT":
        def tog_btn(key: str, label: str):
            on = bool(s.get(key, True))
            return InlineKeyboardButton(f"{label}: {'ON ✅' if on else 'OFF ❌'}", callback_data=f"TS_LAYOUT_TOG_{key}")
        kb = InlineKeyboardMarkup([
            [tog_btn("show_price", "Price"), tog_btn("show_liquidity", "Liquidity")],
            [tog_btn("show_mcap", "MCap"), tog_btn("show_holders", "Holders")],
            [InlineKeyboardButton("⬅️ Back", callback_data="TS_BACK")],
        ])
        await msg.edit_text("*Layout*\nToggle what to show in alerts:", parse_mode="Markdown", reply_markup=kb, disable_web_page_preview=True)
        return

    if data.startswith("TS_LAYOUT_TOG_"):
        key = data.split("_", 3)[3]
        s[key] = not bool(s.get(key, True))
        save_groups()
        await send_token_settings(chat_id, context, msg, edit=True)
        return

    # ----- Preview -----
    if data == "TS_PREVIEW":
        if not isinstance(tok, dict):
            await msg.reply_text("No token configured yet.")
            return
        dummy_tx = "0" * 64
        dummy_buyer = "EQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAM9c"
        await msg.reply_text("📌 Sending preview alert to this group…")
        await post_buy(context.application, chat_id, tok, {"tx": dummy_tx, "buyer": dummy_buyer, "ton": 12.34, "token_amount": 123456.0}, source="Preview")
        return

    # ----- Pause / Resume -----
    if data == "TS_PAUSE":
        if not isinstance(tok, dict):
            await msg.reply_text("No token configured yet.")
            return
        tok["paused"] = not bool(tok.get("paused", False))
        tok["init_done"] = False  # baseline after resume
        save_groups()
        await send_token_settings(chat_id, context, msg, edit=True)
        return

    # ----- Remove -----
    if data == "TS_REMOVE":
        if not isinstance(tok, dict) or not tok:
            await msg.reply_text("No token configured.")
            return
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("✅ Remove", callback_data="TS_REMOVE_CONFIRM")],
            [InlineKeyboardButton("❌ Cancel", callback_data="TS_BACK")],
        ])
        await msg.edit_text("Remove the current token for this group?", reply_markup=kb)
        return

    if data == "TS_REMOVE_CONFIRM":
        g["token"] = None
        save_groups()
        await msg.edit_text("✅ Token removed.")
        return

async def send_status(chat_id: int, context: ContextTypes.DEFAULT_TYPE, msg):
    g = get_group(chat_id)
    token = g.get("token")
    if not token:
        await msg.reply_text("No token configured. Tap *Configure Token*.", parse_mode="Markdown")
        return
    await msg.reply_text(
        "📊 *Status*\n"
        f"Token: *{token.get('symbol') or token.get('name') or 'UNKNOWN'}*\n"
        f"Address: `{token.get('address')}`\n"
        f"STON pool: `{token.get('ston_pool') or 'NONE'}`\n"
        f"DeDust pool: `{token.get('dedust_pool') or 'NONE'}`\n",
        parse_mode="Markdown"
    )

# -------------------- TOKEN AUTO-DETECT --------------------
def detect_token_address(text: str) -> Optional[str]:
    """Extract a TON user-friendly address from arbitrary text.

    Users often paste addresses together with extra suffixes (e.g. "-Lone") or links.
    TON user-friendly base64url addresses are 48 chars long (EQ.. / UQ..).
    We normalize to the canonical 48-char form so pool lookup doesn't fail.
    """
    m = JETTON_RE.search(text or "")
    if not m:
        return None
    cand = (m.group(1) or "").strip()
    # Canonical TON user-friendly address length is 48.
    if len(cand) >= 48:
        cand = cand[:48]
    # Final sanity: must start with EQ/UQ and be urlsafe-base64-ish
    if not (cand.startswith("EQ") or cand.startswith("UQ")):
        return None
    if not re.fullmatch(r"[A-Za-z0-9_-]{48}", cand):
        return None
    return cand

def _dex_pair_lookup(pair_id: str) -> Optional[Dict[str, Any]]:
    """Return Dexscreener pair payload (TON) for a given pair/pool id."""
    pair_id = (pair_id or "").strip()
    if not pair_id:
        return None
    url = f"{DEX_PAIR_URL}/ton/{pair_id}"
    try:
        res = requests.get(url, timeout=20)
        if res.status_code != 200:
            return None
        js = res.json()
        pairs = js.get("pair") or js.get("pairs")
        if isinstance(pairs, list) and pairs:
            return pairs[0] if isinstance(pairs[0], dict) else None
        if isinstance(pairs, dict):
            return pairs
        # Some responses use "pairs" list
        if isinstance(js.get("pairs"), list) and js.get("pairs"):
            p0 = js.get("pairs")[0]
            return p0 if isinstance(p0, dict) else None
        return None
    except Exception:
        return None

def resolve_jetton_from_text_sync(text: str) -> Optional[str]:
    """Resolve a jetton master address from either a jetton address or supported pool/link."""
    t = (text or "").strip()
    if not t:
        return None

    # 1) Direct jetton address
    direct = detect_token_address(t)
    if direct:
        # If it *looks* like a pool link context, try pair lookup first
        if "pool" in t.lower() or "pools" in t.lower() or "geckoterminal" in t.lower() or "dexscreener" in t.lower():
            p = _dex_pair_lookup(direct)
            if p:
                base = p.get("baseToken") or {}
                quote = p.get("quoteToken") or {}
                base_sym = str(base.get("symbol") or "").upper()
                quote_sym = str(quote.get("symbol") or "").upper()
                base_addr = str(base.get("address") or "")
                quote_addr = str(quote.get("address") or "")
                if base_sym in ("TON","WTON","PTON") and quote_addr:
                    return quote_addr
                if quote_sym in ("TON","WTON","PTON") and base_addr:
                    return base_addr
        return direct

    # 2) GeckoTerminal / Dexscreener / ston.fi / dedust.io pool links
    pair_id = None
    for rx in (GECKO_POOL_RE, DEXSCREENER_PAIR_RE, STON_POOL_RE, DEDUST_POOL_RE):
        m = rx.search(t)
        if m:
            pair_id = m.group(1)
            break

    # 3) Fallback: if the message contains a single EQ/UQ-like id, attempt using it as pair id
    if not pair_id:
        m = JETTON_RE.search(t)
        if m:
            pair_id = m.group(1)

    if not pair_id:
        return None

    p = _dex_pair_lookup(pair_id)
    if not p:
        return None
    base = p.get("baseToken") or {}
    quote = p.get("quoteToken") or {}
    base_sym = str(base.get("symbol") or "").upper()
    quote_sym = str(quote.get("symbol") or "").upper()
    base_addr = str(base.get("address") or "")
    quote_addr = str(quote.get("address") or "")
    # choose the non-TON side
    if base_sym in ("TON","WTON","PTON") and quote_addr:
        return quote_addr
    if quote_sym in ("TON","WTON","PTON") and base_addr:
        return base_addr
    # if neither side says TON, still return base (best-effort)
    return base_addr or quote_addr or None

async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.effective_chat or not update.effective_user:
        return
    chat = update.effective_chat
    user = update.effective_user
    text = (update.message.text or "").strip()

    # "ca" shortcut in groups: show currently configured token address (like listing bots)
    if chat.type in ("group", "supergroup") and text.lower() == "ca":
        g = get_group(chat.id)
        tok = g.get("token") if isinstance(g, dict) else None
        if not isinstance(tok, dict) or not (tok.get("address") or "").strip():
            await update.message.reply_text("No token configured yet. Tap /start → Configure Token.")
            return
        name = str(tok.get("name") or tok.get("symbol") or "Token").strip()
        sym = str(tok.get("symbol") or "").strip()
        ca = str(tok.get("address") or "").strip()
        header = f"{name} — (${sym})" if sym else f"{name}"
        await update.message.reply_text(f"{header}\n\n{ca}")
        return

    # Awaiting custom buy-strength emoji (can be normal emoji or Telegram premium <tg-emoji ...>)
    if user.id in AWAITING_CUSTOM_EMOJI:
        target_chat_id = int(AWAITING_CUSTOM_EMOJI.get(user.id) or 0)
        if not target_chat_id:
            AWAITING_CUSTOM_EMOJI.pop(user.id, None)
            return
        # Only allow admins to set for that chat
        if not await is_admin(context.bot, target_chat_id, user.id):
            AWAITING_CUSTOM_EMOJI.pop(user.id, None)
            return
        raw = (update.message.text or "").strip()
        # light validation: allow a single emoji or a <tg-emoji> HTML snippet
        if len(raw) > 180:
            await update.message.reply_text("Emoji text too long. Send a single emoji or a <tg-emoji ...> tag.")
            return
        if "<tg-emoji" not in raw and len(raw) > 6:
            # Still allow multi-char unicode emoji sequences, but guard against long text
            await update.message.reply_text("Send a single emoji (e.g. 🟢) or Telegram premium custom emoji in <tg-emoji ...> format.")
            return
        g = get_group(target_chat_id)
        g["settings"]["strength_emoji"] = raw
        save_groups()
        AWAITING_CUSTOM_EMOJI.pop(user.id, None)
        await update.message.reply_text("✅ Buy emoji updated.")
        return


    # Social link input (Token Settings -> Social Links)
    if user.id in AWAITING_SOCIAL:
        cfg = AWAITING_SOCIAL.get(user.id) or {}
        target_chat_id = int(cfg.get("chat_id") or 0)
        field = str(cfg.get("field") or "telegram")
        if field == "telegram":
            m = re.search(r"https?://t\.me/[A-Za-z0-9_]{3,}(?:\S*)?", text)
            if not m:
                await update.message.reply_text("Send a valid Telegram link like: https://t.me/YourToken")
                return
            tg_url = m.group(0).strip()
            g = get_group(target_chat_id)
            tok = g.get("token") or {}
            if isinstance(tok, dict):
                tok["telegram"] = tg_url
                save_groups()
            AWAITING_SOCIAL.pop(user.id, None)
            await update.message.reply_text("✅ Token Telegram link saved.")
            return
        AWAITING_SOCIAL.pop(user.id, None)
        return

    # Resolve either a jetton address or a supported link (GT / DexScreener / STON / DeDust)
    addr = await _to_thread(resolve_jetton_from_text_sync, text)
    if not addr:
        return

    # Optional: token telegram link can be sent together with CA.
    # Example: EQ... https://t.me/YourToken
    tg_url = ""
    m_tg = re.search(r"https?://t\.me/[A-Za-z0-9_]{3,}(?:\S*)?", text)
    if m_tg:
        tg_url = m_tg.group(0).strip()

    # decide which chat to configure
    target_chat_id = None
    if chat.type == "private":
        cfg = AWAITING.get(user.id)
        if not cfg:
            await update.message.reply_text("Add the bot to your group, then tap *Configure Token* in that group.", parse_mode="Markdown")
            return
        if isinstance(cfg, dict):
            if cfg.get("stage") != "CA":
                await update.message.reply_text("Tap *Configure Token* again and choose a DEX first.", parse_mode="Markdown")
                return
            target_chat_id = int(cfg.get("group_id") or 0)
            dex_mode = str(cfg.get("dex") or "").strip() or "both"
        else:
            target_chat_id = int(cfg)
            dex_mode = "both"
        if not target_chat_id:
            await update.message.reply_text("Tap *Configure Token* again in your group.", parse_mode="Markdown")
            return
    else:
        # in group: only admins can configure
        if not await is_admin(context.bot, chat.id, user.id):
            return
        # If user pressed configure, it's this chat anyway
        target_chat_id = chat.id
        dex_mode = "both"
    await configure_group_token(target_chat_id, addr, context, reply_to_chat=chat.id, telegram=tg_url, dex_mode=dex_mode)
    # Clear awaiting state after successful input
    if chat.type == "private":
        AWAITING.pop(user.id, None)


async def handle_photo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Capture a buy image from an admin and store its Telegram file_id."""
    if not update.message or not update.effective_user or not update.effective_chat:
        return
    user = update.effective_user
    chat = update.effective_chat
    if user.id not in AWAITING_IMAGE:
        return

    target_chat_id = AWAITING_IMAGE.get(user.id)
    if not target_chat_id:
        return

    # In groups, ensure they are sending the photo inside the same group they are configuring.
    if chat.type in ("group", "supergroup") and chat.id != target_chat_id:
        return

    # In private, we trust the stored target_chat_id.
    if not await is_admin(context.bot, target_chat_id, user.id):
        AWAITING_IMAGE.pop(user.id, None)
        return

    photos = update.message.photo or []
    if not photos:
        return

    file_id = photos[-1].file_id  # largest
    g = get_group(target_chat_id)
    g["settings"]["buy_image_file_id"] = file_id
    g["settings"]["buy_image_on"] = True
    save_groups()
    AWAITING_IMAGE.pop(user.id, None)

    await update.message.reply_text("✅ Buy image saved. Image mode is now ON.")

async def configure_group_token(chat_id: int, jetton: str, context: ContextTypes.DEFAULT_TYPE, reply_to_chat: int, telegram: str = "", dex_mode: str = "both"):
    g = get_group(chat_id)
    # 1 token per group: confirm replace if exists and different
    existing = g.get("token") or None
    # Same token: allow updating telegram link without replacing anything.
    if existing and existing.get("address") == jetton and telegram:
        existing["telegram"] = telegram
        save_groups()
        await context.bot.send_message(chat_id=reply_to_chat, text="✅ Token Telegram link updated.")
        return
    if existing and existing.get("address") != jetton:
        # Ask confirmation
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("✅ Replace", callback_data=f"REPL_{chat_id}_{jetton}")],
            [InlineKeyboardButton("❌ Cancel", callback_data="CANCEL_REPL")]
        ])
        await context.bot.send_message(
            chat_id=reply_to_chat,
            text=f"This group already tracks *{existing.get('symbol') or existing.get('name') or 'a token'}*.\nReplace it with the new token?",
            reply_markup=kb,
            parse_mode="Markdown"
        )
        return
    await _set_token_now(chat_id, jetton, context, reply_to_chat, telegram=telegram, dex_mode=dex_mode)

async def on_replace_button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q:
        return
    await q.answer()
    chat = q.message.chat if q.message else update.effective_chat
    user = update.effective_user
    if not chat or not user:
        return

    data = q.data or ""
    if data.startswith("REPL_"):
        # REPL_chatid_jetton
        parts = data.split("_", 2)
        if len(parts) != 3:
            return
        target_chat_id = int(parts[1])
        jetton = parts[2]
        # ensure pressing inside that group and admin
        if chat.id != target_chat_id:
            await q.answer("Open this in the target group.", show_alert=True)
            return
        if not await is_admin(context.bot, chat.id, user.id):
            await q.answer("Admins only.", show_alert=True)
            return
        await _set_token_now(target_chat_id, jetton, context, chat.id)
        try:
            await q.message.delete()
        except Exception:
            pass
        return

    if data == "CANCEL_REPL":
        await q.message.reply_text("Cancelled.")
        return

async def _set_token_now(chat_id: int, jetton: str, context: ContextTypes.DEFAULT_TYPE, reply_chat_id: int, telegram: str = "", dex_mode: str = "both"):
    # Token metadata (GeckoTerminal first, then TonAPI, then DexScreener)
    gk = gecko_token_info(jetton)
    name = (gk.get("name") or "").strip() if gk else ""
    sym = (gk.get("symbol") or "").strip() if gk else ""
    if not name and not sym:
        info = tonapi_jetton_info(jetton)
        name = (info.get("name") or "").strip()
        sym = (info.get("symbol") or "").strip()
    if not name and not sym:
        dx = dex_token_info(jetton)
        name = (dx.get("name") or "").strip()
        sym = (dx.get("symbol") or "").strip()
    dex_mode = (dex_mode or "both").lower().strip()
    # Seed holders once at setup so first buys show holders immediately.
    holders_seed: Optional[int] = None
    try:
        info_h = tonapi_jetton_info(jetton)
        hh = info_h.get("holders_count")
        if hh is not None:
            holders_seed = int(hh)
    except Exception:
        pass
    if holders_seed is None:
        try:
            hh2 = tonapi_jetton_holders_count(jetton)
            if hh2 is not None:
                holders_seed = int(hh2)
        except Exception:
            pass
    # decimals for correct amount formatting
    decimals_seed: int = 9
    try:
        meta_j = get_jetton_meta(jetton)
        decimals_seed = int(meta_j.get("decimals") or 9)
    except Exception:
        decimals_seed = 9

    ston_pool = find_stonfi_ton_pair_for_token(jetton) if dex_mode in ("both","ston","stonfi") else None
    dedust_pool = find_dedust_ton_pair_for_token(jetton) if dex_mode in ("both","dedust") else None

    # If the user pasted a non-canonical address (e.g. a site-added suffix like "-Lone"),
    # we can still recover the correct jetton master from the resolved pool metadata.
    # This prevents "pool found but no buys" situations caused by address mismatches.
    if dedust_pool:
        try:
            p = _dex_pair_lookup(dedust_pool)
            if isinstance(p, dict):
                base = p.get("baseToken") or {}
                quote = p.get("quoteToken") or {}
                base_sym = str(base.get("symbol") or "").upper()
                quote_sym = str(quote.get("symbol") or "").upper()
                base_addr = str(base.get("address") or "").strip()
                quote_addr = str(quote.get("address") or "").strip()
                recovered = ""
                if base_sym in ("TON", "WTON") and quote_addr:
                    recovered = quote_addr
                elif quote_sym in ("TON", "WTON") and base_addr:
                    recovered = base_addr
                elif base_addr:
                    recovered = base_addr
                elif quote_addr:
                    recovered = quote_addr

                if recovered and recovered != jetton:
                    log.warning("Jetton address corrected via pool metadata: %s -> %s", jetton, recovered)
                    jetton = recovered

                    # Refresh metadata using the corrected address (best-effort).
                    gk2 = gecko_token_info(jetton)
                    name2 = (gk2.get("name") or "").strip() if gk2 else ""
                    sym2 = (gk2.get("symbol") or "").strip() if gk2 else ""
                    if not name2 and not sym2:
                        info2 = tonapi_jetton_info(jetton)
                        name2 = (info2.get("name") or "").strip()
                        sym2 = (info2.get("symbol") or "").strip()
                    if not name2 and not sym2:
                        dx2 = dex_token_info(jetton)
                        name2 = (dx2.get("name") or "").strip()
                        sym2 = (dx2.get("symbol") or "").strip()
                    if name2 or sym2:
                        name = name2 or name
                        sym = sym2 or sym
        except Exception:
            pass

    g = get_group(chat_id)
    # Auto-enable pools we actually found.
    # In auto/both mode we keep both enabled if pools exist (no manual DEX split required).
    try:
        s = g.get("settings") or {}
        if dex_mode in ("ston", "stonfi"):
            s["enable_ston"] = True
            s["enable_dedust"] = False
        elif dex_mode in ("dedust",):
            s["enable_ston"] = False
            s["enable_dedust"] = True
        else:
            s["enable_ston"] = bool(ston_pool)
            s["enable_dedust"] = bool(dedust_pool)
        g["settings"] = s
    except Exception:
        pass

    g["token"] = {
        "address": jetton,
        "dex_mode": ("auto" if dex_mode=="both" else dex_mode),
        "name": name,
        "symbol": sym,
        "decimals": int(decimals_seed) if str(decimals_seed).isdigit() else 9,
        "holders": holders_seed,
        "ston_pool": ston_pool,
        "dedust_pool": dedust_pool,
        "set_at": int(time.time()),
        "init_done": False,
        "paused": False,
        "last_ston_tx": None,
        "last_dedust_trade": None,
        "ston_last_block": None,
        "ignore_before_ts": int(time.time()),
        "burst": {"window_start": int(time.time()), "count": 0},
        "telegram": telegram.strip() if telegram else "",
    }
    save_groups()

    # Prevent posting old buys right after configuration
    await warmup_seen_for_chat(chat_id, ston_pool, dedust_pool)
    # Mark init done so tracker loop doesn't skip another full cycle
    try:
        g2 = get_group(chat_id)
        if isinstance(g2.get('token'), dict):
            g2['token']['init_done'] = True
            save_groups()
    except Exception:
        pass

    disp = sym or name or "TOKEN"
    msg = (
        f"✅ *Token Added*\n"
        f"• Token: *{html.escape(disp)}*\n"
        f"• Address: `{jetton}`\n"
        f"• STON.fi pool: `{ston_pool or 'NONE'}`\n"
        f"• DeDust pool: `{dedust_pool or 'NONE'}`\n\n"
        f"Now posting buys automatically for this group.\n"
        f"Use *Settings* to set buy strength & image."
    )

    await context.bot.send_message(
        chat_id=reply_chat_id,
        text=msg,
        parse_mode="Markdown",
        disable_web_page_preview=True,
    )
    if reply_chat_id != chat_id:
        await context.bot.send_message(
            chat_id=chat_id,
            text=msg,
            parse_mode="Markdown",
            disable_web_page_preview=True,
        )

# -------------------- TRACKERS --------------------
async def _to_thread(fn, *args, **kwargs):
    return await asyncio.to_thread(fn, *args, **kwargs)

async def poll_once(app: Application):
    # Collect all groups with configured token
    items: List[Tuple[int, Dict[str, Any]]] = []
    for k, g in GROUPS.items():
        if not isinstance(g, dict):
            continue
        token = g.get("token")
        if not isinstance(token, dict):
            continue
        items.append((int(k), g))

    # Also poll globally tracked tokens (owner-only /addtoken) into the trending channel
    if TRENDING_POST_CHAT_ID:
        try:
            tchat = int(str(TRENDING_POST_CHAT_ID))
            for _jetton, tok in (GLOBAL_TOKENS or {}).items():
                if not isinstance(tok, dict):
                    continue
                # pseudo-group config
                items.append((tchat, {"token": tok, "settings": dict(DEFAULT_SETTINGS)}))
        except Exception:
            pass

    # For each group, poll its pools
    for chat_id, g in items:
        token = g["token"]
        settings = g.get("settings") or DEFAULT_SETTINGS

        # Pause / resume
        if bool(token.get("paused", False)):
            continue

        # One-time initialization per chat to prevent "old buys" spam.
        # If the bot restarts or a token was configured long ago, we warm up cursors/seen once
        # and skip posting on that first cycle.
        if not token.get("init_done"):
            try:
                await warmup_seen_for_chat(chat_id, token.get("ston_pool"), token.get("dedust_pool"))
            except Exception:
                pass
            token["init_done"] = True
            save_groups()
            continue

        min_buy = float(min_buy_ton_threshold(settings))
        anti = (settings.get("anti_spam") or "MED").upper()
        max_msgs, window = anti_spam_limit(anti)

        burst = token.setdefault("burst", {"window_start": int(time.time()), "count": 0})
        now = int(time.time())
        if now - int(burst.get("window_start", now)) > window:
            burst["window_start"] = now
            burst["count"] = 0

        # STON (STON exported events by blocks)
        if settings.get("enable_ston", True) and token.get("ston_pool"):
            pool = token["ston_pool"]
            try:
                latest = await _to_thread(ston_latest_block)
                if latest is None:
                    raise RuntimeError("no latest block")
                # per-token cursor to avoid posting old swaps when a new group configures a token
                last_block = token.get("ston_last_block")
                if last_block is None:
                    # initialize slightly behind to avoid missing
                    last_block = max(0, int(latest) - 5)
                from_b = int(last_block) + 1
                to_b = int(latest)
                # cap range to avoid huge pulls
                if to_b - from_b > 60:
                    from_b = to_b - 60
                evs = await _to_thread(ston_events, from_b, to_b)
                if evs is None:
                    raise RuntimeError("ston events fetch failed")
                # advance cursor only on successful fetch
                token["ston_last_block"] = to_b
                # filter swaps for this pool (STON export feed)
                # ton_leg is determined per-event to avoid base/quote ordering issues
                posted_any = False
                for ev in evs:
                    if (str(ev.get("eventType") or "").lower() != "swap"):
                        continue
                    ignore_before = int(token.get("ignore_before_ts") or 0)
                    ev_ts = int(ev.get("timestamp") or ev.get("time") or ev.get("ts") or 0)
                    if ignore_before and ev_ts and ev_ts < ignore_before:
                        continue
                    pair_id = str(ev.get("pairId") or "").strip()
                    if pair_id != pool:
                        continue
                    tx = str(ev.get("txnId") or "").strip()
                    if not tx:
                        continue
                    maker = str(ev.get("maker") or "").strip()
                    a0_in = _to_float(ev.get("amount0In"))
                    a0_out = _to_float(ev.get("amount0Out"))
                    a1_in = _to_float(ev.get("amount1In"))
                    a1_out = _to_float(ev.get("amount1Out"))
                    # Determine which leg is TON using event symbols (prevents sells being posted as buys)
                    ton_leg = ston_event_ton_leg(ev)
                    if ton_leg is None:
                        ton_leg = ensure_ton_leg_for_pool(token)
                    is_buy, ton_spent, token_received = ston_event_is_buy(ev, ton_leg if ton_leg in (0,1) else -1)
                    if not is_buy:
                        continue
                    if ton_spent < min_buy:
                        continue
                    dedupe_key = f"ston:{pool}:{tx}"
                    if not dedupe_ok(chat_id, dedupe_key):
                        continue
                    if settings.get("burst_mode", True) and burst["count"] >= max_msgs:
                        continue
                    burst["count"] += 1
                    await post_buy(app, chat_id, token, {"tx": tx, "buyer": maker, "ton": ton_spent, "token_amount": token_received}, source="STON.fi")
                    posted_any = True

                # Fallback for STON.fi v2 swaps (TonAPI tx actions).
                # Some v2 pools don't appear in the export feed with matching pairId/fields,
                # but TonAPI actions still include "Swap tokens" / "Stonfi Swap V2".
                if not posted_any:
                    try:
                        txs = await _to_thread(tonapi_account_transactions, pool, 15)
                        # process oldest -> newest
                        txs = list(reversed(txs))
                        for txo in txs:
                            ignore_before = int(token.get("ignore_before_ts") or 0)
                            ut = int(txo.get("utime") or 0)
                            if ignore_before and ut and ut < ignore_before:
                                continue
                            buys = stonfi_extract_buys_from_tonapi_tx(txo, token["address"])
                            for b in buys:
                                ton_spent = float(b.get("ton") or 0.0)
                                # TonAPI sometimes returns nanoTON
                                if ton_spent > 1e5:
                                    ton_spent = ton_spent / 1e9

                                token_amt = float(b.get("token_amount") or 0.0)
                                dec = token.get("decimals")
                                try:
                                    dec_i = int(dec) if dec is not None else None
                                except Exception:
                                    dec_i = None
                                # TonAPI often returns jetton amount in minimal units
                                if dec_i is not None and token_amt > 1e8:
                                    token_amt = token_amt / (10 ** dec_i)

                                if ton_spent < min_buy:
                                    continue
                                txh = str(b.get("tx") or "").strip() or _tx_hash(txo)
                                buyer = str(b.get("buyer") or "").strip()
                                dedupe_key = f"ston:{pool}:{txh}"
                                if not dedupe_ok(chat_id, dedupe_key):
                                    continue
                                if settings.get("burst_mode", True) and burst["count"] >= max_msgs:
                                    continue
                                burst["count"] += 1
                                await post_buy(app, chat_id, token, {"tx": txh, "buyer": buyer, "ton": ton_spent, "token_amount": token_amt}, source="STON.fi v2")
                        save_groups()
                    except Exception as _e:
                        log.debug("STON v2 fallback err chat=%s %s", chat_id, _e)
                save_groups()
            except Exception as e:
                log.debug("STON poll err chat=%s %s", chat_id, e)

        # DeDust (DeDust API trades)
        if settings.get("enable_dedust", True) and token.get("dedust_pool"):
            pool = token["dedust_pool"]
            try:
                trades = await _to_thread(dedust_get_trades, pool, 40)
                if not isinstance(trades, list):
                    trades = []
                # Build sortable items with (lt, ts) so ordering is stable regardless of API order.
                items2 = []
                for tr in trades:
                    b = dedust_trade_to_buy(tr, token["address"])
                    if not b:
                        continue
                    # normalize timestamp (ms or sec)
                    ts_raw = (tr.get("timestamp") or tr.get("time") or tr.get("ts") or 0)
                    try:
                        ts_i = int(float(ts_raw or 0))
                        if ts_i > 10_000_000_000:
                            ts_i = ts_i // 1000
                    except Exception:
                        ts_i = 0
                    # lt/trade_id (prefer numeric)
                    lt_raw = (tr.get("lt") or b.get("trade_id") or tr.get("id") or "")
                    try:
                        lt_i = int(str(lt_raw).strip()) if str(lt_raw).strip() else 0
                    except Exception:
                        lt_i = 0
                    items2.append((lt_i, ts_i, b, tr))

                # sort oldest -> newest
                items2.sort(key=lambda x: (x[0] or 0, x[1] or 0))

                # baselines
                last_lt = 0
                last_ts = 0
                try:
                    last_lt = int(str(token.get("last_dedust_trade") or 0))
                except Exception:
                    last_lt = 0
                try:
                    last_ts = int(token.get("last_dedust_ts") or 0)
                except Exception:
                    last_ts = 0

                ignore_before = int(token.get("ignore_before_ts") or 0)

                posted_any = False

                # If DeDust was enabled later (or group was created before we stored baselines),
                # set a baseline FIRST and do not post historical trades on the first run.
                if (last_lt == 0 and last_ts == 0) and items2:
                    max_lt = max(i[0] for i in items2)
                    max_ts = max(i[1] for i in items2)
                    if max_lt:
                        token["last_dedust_trade"] = str(max_lt)
                    if max_ts:
                        token["last_dedust_ts"] = int(max_ts)
                    if not ignore_before:
                        token["ignore_before_ts"] = int(time.time())
                    save_groups()
                    continue

                max_seen_lt = last_lt
                max_seen_ts = last_ts

                for lt_i, ts_i, b, tr in items2:
                    # ignore old history right after token added
                    if ignore_before and ts_i and ts_i < ignore_before:
                        continue

                    is_new = False
                    if lt_i and last_lt:
                        is_new = lt_i > last_lt
                    elif lt_i and not last_lt:
                        # If we have lt but no baseline yet, treat as new only if after ignore_before
                        is_new = True
                    elif ts_i and last_ts:
                        is_new = ts_i > last_ts
                    elif ts_i and not last_ts:
                        is_new = True

                    if not is_new:
                        continue

                    ton_amt = float(b.get("ton") or 0.0)
                    if ton_amt < min_buy:
                        continue

                    # unified dedupe by normalized tx hash when possible
                    txh = _normalize_tx_hash_to_hex(b.get("tx") or "")
                    dedupe_key = f"tx:{txh}" if txh else f"dedust:{pool}:{b.get('tx')}"
                    if not dedupe_ok(chat_id, dedupe_key):
                        continue
                    if settings.get("burst_mode", True) and burst["count"] >= max_msgs:
                        continue
                    burst["count"] += 1

                    token_amt = float(b.get("token_amount") or 0.0)
                    await post_buy(app, chat_id, token, {
                        "tx": b.get("tx"),
                        "trade_id": str(lt_i or b.get("trade_id") or ""),
                        "buyer": b.get("buyer"),
                        "ton": ton_amt,
                        "token_amount": token_amt,
                    }, source="DeDust")

                    posted_any = True


                    if lt_i and lt_i > max_seen_lt:
                        max_seen_lt = lt_i
                    if ts_i and ts_i > max_seen_ts:
                        max_seen_ts = ts_i

                # update baselines
                if max_seen_lt:
                    token["last_dedust_trade"] = str(max_seen_lt)
                if max_seen_ts:
                    token["last_dedust_ts"] = int(max_seen_ts)

                                # TonAPI events fallback (covers DeDust pools where /trades is empty or lagging)
                if not posted_any:
                    try:
                        # Use full /events (subject_only=false) because subject_only can omit
                        # TonTransfer details needed to calculate TON spent on some DeDust v3 swaps.
                        events = await _to_thread(tonapi_account_events, pool, 40)
                        if isinstance(events, list) and events:
                            last_eid = str(token.get('last_dedust_event_id') or '').strip()
                            try:
                                last_ets = int(token.get('last_dedust_event_ts') or 0)
                            except Exception:
                                last_ets = 0
                
                            # First run baseline (avoid old spam)
                            if not last_eid and not last_ets:
                                newest = events[0]
                                eid0 = str(newest.get('event_id') or newest.get('id') or '').strip()
                                ts0 = int(newest.get('timestamp') or 0)
                                if eid0:
                                    token['last_dedust_event_id'] = eid0
                                if ts0:
                                    token['last_dedust_event_ts'] = ts0
                            else:
                                new_events = []
                                for ev in events:
                                    if not isinstance(ev, dict):
                                        continue
                                    eid = str(ev.get('event_id') or ev.get('id') or '').strip()
                                    ts = int(ev.get('timestamp') or 0)
                                    if last_eid and eid == last_eid:
                                        break
                                    if last_ets and ts and ts <= last_ets:
                                        continue
                                    if ignore_before and ts and ts < ignore_before:
                                        continue
                                    new_events.append(ev)
                
                                for ev in reversed(new_events):
                                    buys = dedust_buys_from_tonapi_event(ev, token['address'], pool)
                                    for b in buys:
                                        ton_amt = float(b.get('ton') or 0.0)
                                        if ton_amt < min_buy:
                                            continue
                                        txh = _normalize_tx_hash_to_hex(b.get('tx') or '')
                                        dedupe_key = ('tx:' + txh) if txh else ('dedust:' + str(pool) + ':' + str(b.get('tx')))
                                        if not dedupe_ok(chat_id, dedupe_key):
                                            continue
                                        if settings.get('burst_mode', True) and burst['count'] >= max_msgs:
                                            continue
                                        burst['count'] += 1
                                        await post_buy(app, chat_id, token, {
                                            'tx': b.get('tx'),
                                            'buyer': b.get('buyer'),
                                            'ton': ton_amt,
                                            'token_amount': float(b.get('token_amount') or 0.0),
                                        }, source='DeDust')
                                        posted_any = True
                
                                    eid_new = str(ev.get('event_id') or ev.get('id') or '').strip()
                                    ts_new = int(ev.get('timestamp') or 0)
                                    if eid_new:
                                        token['last_dedust_event_id'] = eid_new
                                    if ts_new:
                                        token['last_dedust_event_ts'] = ts_new
                    except Exception as _e:
                        log.debug('DeDust TonAPI events fallback err chat=%s %s', chat_id, _e)

                save_groups()
            except Exception as e:
                log.debug("DeDust poll err chat=%s %s", chat_id, e)



    # save seen occasionally
    save_seen()

async def post_buy(app: Application, chat_id: int, token: Dict[str, Any], b: Dict[str, Any], source: str):
    sym = (token.get("symbol") or "").strip()
    name = (token.get("name") or "").strip()
    title = sym or name or "TOKEN"

    ton_amt = float(b.get("ton") or 0.0)
    tok_amt = b.get("token_amount")
    tok_symbol = b.get("token_symbol") or sym or ""

    # Record for Top-10 leaderboard (rolling window)
    try:
        record_buy_for_leaderboard(token, ton_amt)
    except Exception:
        pass

    buyer_full = str(b.get("buyer") or "")
    buyer_short = _short_addr(buyer_full)
    buyer_url = f"https://tonviewer.com/address/{buyer_full}" if buyer_full else None
    tx = str(b.get("tx") or "")

    ston_pool = token.get("ston_pool") or ""
    dedust_pool = token.get("dedust_pool") or ""
    pool_for_market = ston_pool or dedust_pool

    # Jetton address (used for holders + market cache keys)
    jetton_addr = str(token.get("address") or "").strip()

    # Market data (prefer GeckoTerminal). Keep last known stats so
    # Liquidity/MCap don't randomly disappear when an API call fails.
    def _to_float(x):
        try:
            return float(x)
        except Exception:
            return None

    price_usd = _to_float(token.get("price_usd"))
    liq_usd = _to_float(token.get("liq_usd"))
    mc_usd = _to_float(token.get("mc_usd"))
    # Try cache first to avoid missing stats (rate limits / temporary failures)
    market_cache_key = str(pool_for_market or jetton_addr or "").strip()
    _mcached = MARKET_CACHE.get(market_cache_key) if market_cache_key else None
    _now = int(time.time())
    if _mcached and _now - int(_mcached.get("ts") or 0) < 900:
        # Merge cache (do not overwrite known values with None)
        price_usd = _to_float(_mcached.get("price_usd")) if _mcached.get("price_usd") is not None else price_usd
        liq_usd = _to_float(_mcached.get("liq_usd")) if _mcached.get("liq_usd") is not None else liq_usd
        mc_usd = _to_float(_mcached.get("mc_usd")) if _mcached.get("mc_usd") is not None else mc_usd
    if pool_for_market:
        pinfo = gecko_pool_info(pool_for_market)
        if pinfo:
            pv = _to_float(pinfo.get("price_usd"))
            if pv is not None:
                price_usd = pv
            lv = _to_float(pinfo.get("liquidity_usd"))
            if lv is not None:
                liq_usd = lv
            mv = _to_float(pinfo.get("market_cap_usd"))
            if mv is not None:
                mc_usd = mv

    if (price_usd is None or mc_usd is None) and token.get("address"):
        tinfo = gecko_token_info(token["address"])
        if tinfo:
            if price_usd is None:
                pv = _to_float(tinfo.get("price_usd"))
                if pv is not None:
                    price_usd = pv
            if mc_usd is None:
                mv = _to_float(tinfo.get("market_cap_usd"))
                if mv is not None:
                    mc_usd = mv

    # Holders (keep last known value if APIs fail)
    holders = None
    try:
        if token.get("holders") is not None:
            holders = int(token.get("holders"))
    except Exception:
        holders = None

    if jetton_addr:
        # TonAPI Jetton info sometimes includes holders_count. If not, fall back
        # to the dedicated holders endpoint.
        try:
            info = tonapi_jetton_info(jetton_addr)
            h = info.get("holders_count")
            if h is not None:
                holders = int(h)
        except Exception:
            pass
        if holders is None:
            try:
                h2 = tonapi_jetton_holders_count(jetton_addr)
                if h2 is not None:
                    holders = int(h2)
            except Exception:
                pass

    # Persist latest known holders so the field doesn't disappear in later buys.
    if holders is not None:
        try:
            token["holders"] = int(holders)
        except Exception:
            pass

    # Persist last known market stats (best-effort) - do NOT store 0/invalid values
    try:
        if price_usd is not None and float(price_usd) > 0:
            token["price_usd"] = float(price_usd)
    except Exception:
        pass
    try:
        if liq_usd is not None and float(liq_usd) > 0:
            token["liq_usd"] = float(liq_usd)
    except Exception:
        pass
    try:
        if mc_usd is not None and float(mc_usd) > 0:
            token["mc_usd"] = float(mc_usd)
    except Exception:
        pass
    # Normalize market stats: treat 0/invalid as missing so we never show $0

    try:

        if price_usd is not None and float(price_usd) <= 0:

            price_usd = None

    except Exception:

        price_usd = None

    try:

        if liq_usd is not None and float(liq_usd) <= 0:

            liq_usd = None

    except Exception:

        liq_usd = None

    try:

        if mc_usd is not None and float(mc_usd) <= 0:

            mc_usd = None

    except Exception:

        mc_usd = None


    # Store/refresh cache so later messages don't lose stats. Merge to avoid
    # overwriting non-null values with nulls.
    if market_cache_key:
        prev = MARKET_CACHE.get(market_cache_key) or {}
        MARKET_CACHE[market_cache_key] = {
            "ts": int(time.time()),
            "price_usd": price_usd if price_usd is not None else prev.get("price_usd"),
            "liq_usd": liq_usd if liq_usd is not None else prev.get("liq_usd"),
            "mc_usd": mc_usd if mc_usd is not None else prev.get("mc_usd"),
            "holders": holders if holders is not None else prev.get("holders"),
        }

    # Links row
    pair_for_links = pool_for_market or ""
    tx_hex = _normalize_tx_hash_to_hex(tx)
    # DeDust sometimes returns only LT (no hash). Resolve hash via TonAPI if possible.
    if not tx_hex and source == "DeDust":
        lt_guess = str(b.get("trade_id") or tx or "").strip()
        if lt_guess:
            resolved = tonapi_find_tx_hash_by_lt(str(dedust_pool or ""), lt_guess, limit=300)
            if not resolved:
                # quick retries for busy pools
                for _ in range(3):
                    try:
                        time.sleep(0.35)
                    except Exception:
                        pass
                    resolved = tonapi_find_tx_hash_by_lt(str(dedust_pool or ""), lt_guess, limit=600)
                    if resolved:
                        break
            tx_hex = _normalize_tx_hash_to_hex(resolved) or tx_hex
    tx_url = f"https://tonviewer.com/transaction/{tx_hex}" if tx_hex else (f"https://tonviewer.com/transaction/{quote(str(tx))}" if tx else None)
    gt_url = gecko_terminal_pool_url(pair_for_links) if pair_for_links else None
    dex_url = f"https://dexscreener.com/ton/{pair_for_links}" if pair_for_links else None

    # Percent change (best-effort) from Dexscreener (used in trending channel buy line)
    change_pct = None
    try:
        if pair_for_links:
            _p = _dex_pair_lookup(pair_for_links)
            if isinstance(_p, dict):
                _pc = _p.get("priceChange") or {}
                _ch = None
                if isinstance(_pc, dict):
                    _ch = _pc.get("h6")
                    if _ch is None:
                        _ch = _pc.get("h1")
                if _ch is None and isinstance(_p.get("priceChangeH6"), (int, float, str)):
                    _ch = _p.get("priceChangeH6")
                if _ch is not None:
                    change_pct = float(_ch)
    except Exception:
        change_pct = None
    # Token telegram button should reflect the token's own link.
    # If not set, hide the button (avoid wrong/static links).
    tg_link = (token.get("telegram") or "").strip()
    trending = TRENDING_URL

    # Pull settings for this chat (for strength + image)
    g = get_group(chat_id)
    s = g.get("settings") or DEFAULT_SETTINGS

    def fmt_usd(x: Optional[float], decimals: int = 0) -> Optional[str]:
        if x is None:
            return None
        try:
            if decimals <= 0:
                return f"${float(x):,.0f}"
            return f"${float(x):,.{decimals}f}"
        except Exception:
            return None

    def fmt_token_amount(x: float) -> str:
        try:
            ax = abs(float(x))
        except Exception:
            return str(x)
        if ax >= 1000:
            return f"{float(x):,.2f}"
        if ax >= 1:
            return f"{float(x):,.4f}"
        return f"{float(x):,.6f}"

    # -------------------- SpyTON premium buy cards (HTML) --------------------
    def h(s: Any) -> str:
        return html.escape(str(s or ""))

    # Strength block supports Telegram premium emoji if the user stored a <tg-emoji ...> tag.
    def repeat_emoji(e: str, count: int) -> str:
        if count <= 0:
            return ""
        if "<tg-emoji" in e:
            return "".join([e for _ in range(count)])
        return e * count

    # Build a chart link for the header.
    jetton_url = f"https://tonviewer.com/jetton/{quote(jetton_addr)}" if jetton_addr else ""
    chart_url = gt_url or dex_url or jetton_url or ""
    header_link = chart_url or trending or ""

    # USD display (best-effort)
    usd_spent = None
    try:
        p = ton_usd_price()
        if p and p > 0:
            usd_spent = ton_amt * float(p)
    except Exception:
        usd_spent = None
    usd_disp = f" (${usd_spent:,.2f})" if usd_spent is not None else ""

    # Buyer "New!" flag (per chat + token)
    is_new_buyer = False
    if buyer_full and jetton_addr:
        bucket = SEEN.setdefault(str(chat_id), {})
        bkey = f"buyer:{jetton_addr}:{buyer_full}"
        if bkey not in bucket:
            is_new_buyer = True
            bucket[bkey] = int(time.time())
            save_seen()

    buyer_html = h(buyer_short)
    buyer_line = buyer_html
    if buyer_url:
        buyer_line = f'<a href="{h(buyer_url)}">{buyer_html}</a>'
    if tx_url:
        if is_new_buyer:
            buyer_line = f"{buyer_line}: New! — <a href=\"{h(tx_url)}\">Txn</a>"
        else:
            buyer_line = f"{buyer_line} — <a href=\"{h(tx_url)}\">Txn</a>"
    else:
        buyer_line = f"{buyer_line}: New! — Txn" if is_new_buyer else f"{buyer_line} — Txn"

    # Strength block
    strength_html = ""
    if bool(s.get("strength_on", True)):
        try:
            step = float(s.get("strength_step_ton") or 5.0)
            max_n = int(s.get("strength_max") or 30)
            emo = str(s.get("strength_emoji") or "🟢")
            n = 1 if ton_amt > 0 else 0
            if step > 0:
                n = max(1, int(ton_amt // step))
            n = min(max_n, n)
            per_line = 15
            rows = []
            for i in range(0, n, per_line):
                rows.append(repeat_emoji(emo, min(per_line, n - i)))
            strength_html = "\n".join(rows)
        except Exception:
            strength_html = ""

    # Token amount formatting
    got_line = ""
    if tok_amt and tok_symbol:
        # Make token symbol clickable to token Telegram link (if provided)
        sym_html = h(tok_symbol)
        if tg_link:
            sym_html = f'<a href="{h(tg_link)}">{h(tok_symbol)}</a>'
        try:
            tok_amt_f = float(tok_amt)
            got_line = f"🪙 <b>{h(fmt_token_amount(tok_amt_f))} {sym_html}</b>"
        except Exception:
            got_line = f"🪙 <b>{h(tok_amt)} {sym_html}</b>"

    # Stats (match screenshot order: MarketCap, Liquidity, Holders)
    def _pos_or_none(v):
        try:
            if v is None:
                return None
            fv = float(v)
            return fv if fv > 0 else None
        except Exception:
            return None

    mc_line = f"📊 MarketCap: {h(fmt_usd(_pos_or_none(mc_usd), 0) or '—')}" if bool(s.get("show_mcap", True)) else ""
    liq_line = f"💧 Liquidity {h(fmt_usd(_pos_or_none(liq_usd), 0) or '—')}" if bool(s.get("show_liquidity", True)) else ""
    holders_line = ""
    if bool(s.get("show_holders", True)):
        hval = f"{holders:,}" if isinstance(holders, int) else (str(holders) if holders is not None else "—")
        if jetton_url and holders is not None:
            holders_line = f"👥 Holders: <a href=\"{h(jetton_url)}\">{h(hval)}</a>"
        else:
            holders_line = f"👥 Holders: {h(hval)}"

    # Links area
    listing_line = f"❤️ <a href=\"{h(LISTING_URL)}\">TonListing</a>" if LISTING_URL else ""
    chart_link = chart_url
    chart_part = f"📈 <a href=\"{h(chart_link)}\">Chart</a>" if chart_link else "📈 Chart"
    trending_part = f"🔥 <a href=\"{h(trending)}\">Trending</a>" if trending else "🔥 Trending"
    # DTrade deep link: use referral base + append token CA.
    # We URL-encode CA to avoid issues with special chars.
    from urllib.parse import quote as _urlquote
    ref = (DTRADE_REF or "https://t.me/dtrade?start=11TYq7LInG").rstrip("_")
    ca = (token.get("address") or "").strip()
    buy_url = f"{ref}_{_urlquote(ca, safe='')}" if ca else ref
    dtrade_part = f"🛒 <a href=\"{h(buy_url)}\">DTrade</a>" if buy_url else "🛒 DTrade"
    coc_part = f"💬 <a href=\"{h(tg_link)}\">COC</a>" if tg_link else ""

    links_line_parts = [chart_part, trending_part]
    if coc_part:
        links_line_parts.append(coc_part)
    links_line_parts.append(dtrade_part)
    links_line = " | ".join([p for p in links_line_parts if p])

    # Ads line
    ad_text, ad_link, _left = active_ad()
    ad_line = f"ad: <a href=\"{h(ad_link)}\">{h(ad_text)}</a>" if ad_link else f"ad: {h(ad_text)}"

    def build_group_message() -> str:
        """Compact group style like the reference (Spent/Got + Price/Liq/MCap/Holders)."""
        # Make token title clickable to its Telegram link when available
        if tg_link:
            header_text = f"<b><a href=\"{h(tg_link)}\">{h(title)}</a> Buy!</b>"
        elif chart_link:
            header_text = f"<b><a href=\"{h(chart_link)}\">{h(title)}</a> Buy!</b>"
        else:
            header_text = f"<b>{h(title)} Buy!</b>"
        blocks: List[str] = [header_text]
        if strength_html:
            blocks.append(strength_html)
        blocks.append("")
        blocks.append(f"Spent: <b>{ton_amt:,.2f} TON</b>")
        if tok_amt and tok_symbol:
            try:
                tok_amt_f = float(tok_amt)
                blocks.append(f"Got: <b>{h(fmt_token_amount(tok_amt_f))} {h(tok_symbol)}</b>")
            except Exception:
                blocks.append(f"Got: <b>{h(tok_amt)} {h(tok_symbol)}</b>")
        blocks.append("")
        # Buyer + Txn (no New Holder label in group style)
        buyer_line2 = buyer_html
        if buyer_url:
            buyer_line2 = f'<a href="{h(buyer_url)}">{buyer_html}</a>'
        if tx_url:
            buyer_line2 = f"{buyer_line2} | <a href=\"{h(tx_url)}\">Txn</a>"
        blocks.append(buyer_line2)
        blocks.append("")

        # Market stats (always show the rows; use last-known or '—')
        def _pos_or_none(v):
            try:
                if v is None:
                    return None
                fv = float(v)
                return fv if fv > 0 else None
            except Exception:
                return None

        price_disp = fmt_usd(_pos_or_none(price_usd), 6) or "—"
        liq_disp = fmt_usd(_pos_or_none(liq_usd), 0) or "—"
        mc_disp = fmt_usd(_pos_or_none(mc_usd), 0) or "—"
        blocks.append(f"Price: {h(price_disp)}")
        blocks.append(f"Liquidity: {h(liq_disp)}")
        blocks.append(f"MCap: {h(mc_disp)}")
        blocks.append(f"Holders: {h(f'{int(holders):,}' if holders is not None else '—')}")
        # Inline text links row like the reference: TX | GT | DexS | Telegram | Trending
        link_parts = []
        if tx_url:
            link_parts.append(f"<a href=\"{h(tx_url)}\">TX</a>")
        if gt_url:
            link_parts.append(f"<a href=\"{h(gt_url)}\">GT</a>")
        if dex_url:
            link_parts.append(f"<a href=\"{h(dex_url)}\">DexS</a>")
        if tg_link:
            link_parts.append(f"<a href=\"{h(tg_link)}\">Telegram</a>")
        if trending:
            link_parts.append(f"<a href=\"{h(trending)}\">Trending</a>")
        if link_parts:
            blocks.append(" | ".join(link_parts))

        blocks.append("")
        blocks.append(ad_line)
        return "\n".join([b for b in blocks if b is not None])

    def build_trending_channel_message() -> str:
        """Trending channel style (only). Keeps all clickable links, but uses the requested layout."""
        # Header: | TOKEN Buy! (TOKEN clickable to Telegram when available)
        header_token = tok_symbol or title
        if tg_link:
            header = f'| <a href="{h(tg_link)}"><b>{h(header_token)}</b></a> Buy!'
        elif chart_link:
            header = f'| <a href="{h(chart_link)}"><b>{h(header_token)}</b></a> Buy!'
        else:
            header = f'| <b>{h(header_token)}</b> Buy!'

        # Checkmark strength line (static like your example)
        checks = "✅" * 26

        # Token amount line with 🔀 and clickable symbol (if TG exists)
        token_line = ""
        if tok_amt and tok_symbol:
            sym_html = h(tok_symbol)
            if tg_link:
                sym_html = f'<a href="{h(tg_link)}">{h(tok_symbol)}</a>'
            try:
                tok_amt_f = float(tok_amt)
                token_line = f'🔀 <b>{h(fmt_token_amount(tok_amt_f))} {sym_html}</b>'
            except Exception:
                token_line = f'🔀 <b>{h(tok_amt)} {sym_html}</b>'

        # Holders compact (1.17K, 2.3M)
        def _fmt_compact_int(n: Optional[int]) -> str:
            if n is None:
                return "—"
            try:
                x = float(n)
            except Exception:
                return "—"
            if x >= 1_000_000:
                return f"{x/1_000_000:.2f}".rstrip("0").rstrip(".") + "M"
            if x >= 1_000:
                return f"{x/1_000:.2f}".rstrip("0").rstrip(".") + "K"
            return f"{int(x):,}"

        holders_compact = _fmt_compact_int(int(holders) if holders is not None else None)
        holders_line_ch = f"🔀 {h(holders_compact)} Holders"

        # Buyer line (wallet clickable) + change % (h6/h1) + Txn clickable
        buyer_html2 = h(buyer_short)
        if buyer_url:
            buyer_html2 = f'<a href="{h(buyer_url)}">{buyer_html2}</a>'
        pct_part = ""
        if isinstance(change_pct, (int, float)):
            try:
                v = float(change_pct)
                sign = "+" if v > 0 else ""
                pct_part = f": {sign}{v:.1f}%"
            except Exception:
                pct_part = ""
        txn_part = f' | <a href="{h(tx_url)}">Txn</a>' if tx_url else " | Txn"
        buyer_line_ch = f"👤 {buyer_html2}{pct_part}{txn_part}"

        # Price + MarketCap
        price_line = "💵 Price: —"
        if price_usd is not None:
            try:
                price_line = f"💵 Price: ${float(price_usd):,.6f}"
            except Exception:
                price_line = "💵 Price: —"
        mc_line_ch = f"💵 MarketCap: {h(fmt_usd(mc_usd, 0) or '—')}"

        # Links row: Listing | Buy | Chart (all clickable)
        listing_part = f'💎 <a href="{h(LISTING_URL)}">Listing</a>' if LISTING_URL else "💎 Listing"
        buy_part = f'🐸 <a href="{h(buy_url)}">Buy</a>' if buy_url else "🐸 Buy"
        chart_part = f'📊 <a href="{h(chart_link)}">Chart</a>' if chart_link else "📊 Chart"
        links_row = " | ".join([p for p in [listing_part, buy_part, chart_part] if p])

        blocks: List[str] = []
        blocks.append(header)
        blocks.append("")
        blocks.append(checks)
        blocks.append("")
        blocks.append(f" ꘜ  {ton_amt:,.2f} TON{h(usd_disp)}")
        if token_line:
            blocks.append(token_line)
        blocks.append(holders_line_ch)
        blocks.append(buyer_line_ch)
        blocks.append(price_line)
        blocks.append(mc_line_ch)
        blocks.append("")
        blocks.append(links_row)
        blocks.append(ad_line)
        return "\n".join([b for b in blocks if b is not None])
    def is_trending_dest(dest_chat_id: int) -> bool:
        return bool(TRENDING_POST_CHAT_ID and str(dest_chat_id) == str(TRENDING_POST_CHAT_ID))

    # Default message used for the original chat send
    msg = build_trending_channel_message() if is_trending_dest(int(chat_id)) else build_group_message()

    # If buy image enabled and a Telegram file_id is set, send a photo with caption.
    buy_file_id = (s.get("buy_image_file_id") or "").strip()
    use_image = bool(s.get("buy_image_on", False)) and bool(buy_file_id)

    # Buttons:
    # - Groups: compact utility row + Buy with dTrade
    # - Trending channel: Book Trending ONLY
    def build_buy_keyboard(dest_chat_id: int) -> InlineKeyboardMarkup:
        if is_trending_dest(int(dest_chat_id)):
            book_btn = InlineKeyboardButton("Book Trending", url=BOOK_TRENDING_URL)
            return InlineKeyboardMarkup([[book_btn]])

        # Groups: one clean Buy button (text links are in message body)
        buy_btn = InlineKeyboardButton(f"Buy {tok_symbol or title} with dTrade", url=buy_url)
        return InlineKeyboardMarkup([[buy_btn]])

    async def _send(dest_chat_id: int):
        kb = build_buy_keyboard(int(dest_chat_id))
        local_msg = build_trending_channel_message() if is_trending_dest(int(dest_chat_id)) else build_group_message()
        # Never send group buy image into the trending channel.
        if use_image and (not is_trending_dest(int(dest_chat_id))):
            await app.bot.send_photo(
                chat_id=dest_chat_id,
                photo=buy_file_id,
                caption=local_msg,
                parse_mode="HTML",
                reply_markup=kb,
            )
        else:
            await app.bot.send_message(
                chat_id=dest_chat_id,
                text=local_msg,
                parse_mode="HTML",
                disable_web_page_preview=True,
                reply_markup=kb,
            )

    try:
        await _send(chat_id)
    except Exception as e:
        # fallback without parse mode
        try:
            await app.bot.send_message(chat_id=chat_id, text=re.sub(r"<[^>]+>", "", msg), disable_web_page_preview=True)
        except Exception:
            log.debug("send fail %s", e)

    # Optional mirroring into official trending channel
    if MIRROR_TO_TRENDING and TRENDING_POST_CHAT_ID and str(chat_id) != str(TRENDING_POST_CHAT_ID):
        try:
            await _send(int(TRENDING_POST_CHAT_ID))
        except Exception:
            pass

async def tracker_loop(app: Application):
    while True:
        try:
            await poll_once(app)
        except Exception as e:
            log.exception("tracker loop error: %s", e)
        await asyncio.sleep(POLL_INTERVAL)


# -------------------- Trending Leaderboard (Top-10) --------------------
def build_leaderboard_text() -> str:
    """Build Top Movers leaderboard using the Bit-main format.

    Instead of buy-volume %, this uses **Dexscreener priceChange** (h6 fallback h1)
    so it updates naturally as the market moves.

    Candidates are derived from:
      - all configured group tokens
      - all GLOBAL_TOKENS

    Token symbol is clickable (token telegram if known; else tonviewer jetton page).
    """

    def h(s: Any) -> str:
        return html.escape(str(s or ""))

    # simple local cache to avoid spamming dexscreener on every tick
    # key -> {ts:int, data:dict}
    global _LB_PAIR_CACHE
    try:
        _LB_PAIR_CACHE
    except Exception:
        _LB_PAIR_CACHE = {}

    now = int(time.time())
    cache_ttl = 45

    def pair_lookup_cached(pair_id: str) -> Optional[Dict[str, Any]]:
        pair_id = (pair_id or "").strip()
        if not pair_id:
            return None
        c = _LB_PAIR_CACHE.get(pair_id)
        if isinstance(c, dict) and (now - int(c.get("ts") or 0) <= cache_ttl) and isinstance(c.get("data"), dict):
            return c["data"]
        d = _dex_pair_lookup(pair_id)
        if isinstance(d, dict):
            _LB_PAIR_CACHE[pair_id] = {"ts": now, "data": d}
        return d

    # Build candidate token list
    candidates: Dict[str, Dict[str, Any]] = {}  # jetton -> token dict

    for g in (GROUPS or {}).values():
        if not isinstance(g, dict):
            continue
        tok = g.get("token")
        if isinstance(tok, dict):
            j = str(tok.get("address") or "").strip()
            if j:
                candidates[j] = tok

    for j, tok in (GLOBAL_TOKENS or {}).items():
        if isinstance(tok, dict):
            jj = str(tok.get("address") or j or "").strip()
            if jj:
                candidates[jj] = tok

    items: List[Dict[str, Any]] = []

    # Pull top movers from dexscreener pair payloads
    for jetton, tok in candidates.items():
        if not isinstance(tok, dict):
            continue
        pair_id = (tok.get("ston_pool") or tok.get("dedust_pool") or "").strip()
        if not pair_id:
            continue

        p = pair_lookup_cached(pair_id)
        if not isinstance(p, dict):
            continue

        base = p.get("baseToken") or {}
        quote = p.get("quoteToken") or {}
        base_sym = str(base.get("symbol") or "").upper()
        quote_sym = str(quote.get("symbol") or "").upper()

        # determine the token side (the non-TON symbol)
        sym = None
        if base_sym in ("TON", "WTON", "PTON") and quote_sym:
            sym = quote_sym
        elif quote_sym in ("TON", "WTON", "PTON") and base_sym:
            sym = base_sym
        else:
            # not a TON pair
            sym = str(tok.get("symbol") or "?").strip().upper() or "?"

        # price change
        pc = p.get("priceChange") or {}
        ch = None
        try:
            if isinstance(pc, dict):
                ch = pc.get("h6")
                if ch is None:
                    ch = pc.get("h1")
            if ch is None and isinstance(p.get("priceChangeH6"), (int, float, str)):
                ch = p.get("priceChangeH6")
        except Exception:
            ch = None

        try:
            ch_f = float(ch)
        except Exception:
            continue

        tg = str(tok.get("telegram") or "").strip()
        if not tg:
            tg = ""

        items.append({
            "jetton": str(jetton),
            "sym": sym,
            "tg": tg,
            "ch": ch_f,
        })

    # Dedup by jetton, sort by absolute change
    dedup: Dict[str, Dict[str, Any]] = {}
    for it in items:
        dedup[it["jetton"]] = it
    top = sorted(dedup.values(), key=lambda x: abs(float(x.get("ch") or 0.0)), reverse=True)[:10]

    def fmt_pct(v: float) -> str:
        sign = "+" if v > 0 else ""
        try:
            return f"{sign}{float(v):.0f}%"
        except Exception:
            return "0%"

    def sym_link(sym: str, tg: str, jetton: str) -> str:
        link = tg if (tg and tg.startswith("http")) else f"https://tonviewer.com/jetton/{jetton}"
        return f"<a href='{h(link)}'>${h(sym)}</a>"

    nums = ["1️⃣","2️⃣","3️⃣","4️⃣","5️⃣","6️⃣","7️⃣","8️⃣","9️⃣","🔟"]

    text = f"TON TRENDING\n🟢 {h(LEADERBOARD_HEADER_HANDLE)}\n\n"
    if not top:
        text += "(No data yet)"
        return text

    for i, it in enumerate(top):
        badge = nums[i] if i < len(nums) else f"{i+1}."
        text += f"{badge} - {sym_link(it['sym'], it.get('tg') or '', it['jetton'])} | {fmt_pct(it['ch'])}\n"
        if i == 2:
            text += "------------------------------\n"

    return text.strip()

async def leaderboard_loop(app: Application):
    """Create/update a single Top-10 leaderboard message in the trending channel.

    Fixes:
    - Loop never dies (try/except each iteration)
    - Reuses stored message_id OR pinned message OR creates a new one
    """
    if not LEADERBOARD_ON:
        return

    chat_id_str = (LEADERBOARD_CHAT_ID_STR or TRENDING_POST_CHAT_ID or "").strip()
    if not chat_id_str:
        return
    try:
        channel_id = int(chat_id_str)
    except Exception:
        return

    fixed_msg_id: Optional[int] = None
    try:
        if LEADERBOARD_MESSAGE_ID_STR and LEADERBOARD_MESSAGE_ID_STR.isdigit():
            fixed_msg_id = int(LEADERBOARD_MESSAGE_ID_STR)
    except Exception:
        fixed_msg_id = None

    kb = InlineKeyboardMarkup([[InlineKeyboardButton("Ton Listing", url=LISTING_URL)]]) if LISTING_URL else None
    key = str(channel_id)

    while True:
        try:
            state = _load_leaderboard_msg_state()
            msg_id: Optional[int] = fixed_msg_id

            if not msg_id:
                try:
                    msg_id = int(state.get(key) or 0) or None
                except Exception:
                    msg_id = None

            # Persist fixed id (best-effort)
            if fixed_msg_id:
                try:
                    state[key] = int(fixed_msg_id)
                    _save_leaderboard_msg_state(state)
                except Exception:
                    pass

            # If no stored msg_id, try pinned message (only when not fixed)
            if (not fixed_msg_id) and (not msg_id):
                try:
                    chat = await app.bot.get_chat(channel_id)
                    pm = getattr(chat, "pinned_message", None)
                    pm_text = (getattr(pm, "text", None) or getattr(pm, "caption", None) or "") if pm else ""
                    needle = (LEADERBOARD_HEADER_HANDLE or "@Spytontrending").strip()
                    if pm and needle and needle in pm_text:
                        msg_id = int(pm.message_id)
                        state[key] = msg_id
                        _save_leaderboard_msg_state(state)
                except Exception:
                    pass

            try:

                text = build_leaderboard_text()

            except Exception as e:

                log.exception("build_leaderboard_text error: %s", e)

                text = "<b>TON TRENDING</b>\n🟢 <b>%s</b>\n\nNo data yet — waiting for buys…\n\n<blockquote>To trend use @SpyTONTrndBot to book trend</blockquote>" % (LEADERBOARD_HEADER_HANDLE or "@Spytontrending")

            # 1) Edit if possible
            if msg_id:
                try:
                    await app.bot.edit_message_text(
                        chat_id=channel_id,
                        message_id=int(msg_id),
                        text=text,
                        parse_mode="HTML",
                        disable_web_page_preview=True,
                        reply_markup=kb,
                    )
                except Exception as e:
                    emsg = str(e).lower()
                    if "message is not modified" in emsg:
                        pass
                    elif "flood" in emsg or "too many requests" in emsg:
                        pass
                    elif fixed_msg_id:
                        log.warning("leaderboard edit failed (fixed msg_id=%s): %s", fixed_msg_id, e)
                    elif ("message to edit not found" in emsg) or ("message can't be edited" in emsg) or ("message_id_invalid" in emsg):
                        try:
                            state.pop(key, None)
                            _save_leaderboard_msg_state(state)
                        except Exception:
                            pass
                        msg_id = None
                    else:
                        log.warning("leaderboard edit failed (kept msg_id): %s", e)

            # 2) Send if needed (only when not fixed)
            if (not fixed_msg_id) and (not msg_id):
                try:
                    m = await app.bot.send_message(
                        chat_id=channel_id,
                        text=text,
                        parse_mode="HTML",
                        disable_web_page_preview=True,
                        reply_markup=kb,
                    )
                    msg_id = int(m.message_id)
                    state[key] = msg_id
                    _save_leaderboard_msg_state(state)
                    try:
                        await app.bot.pin_chat_message(chat_id=channel_id, message_id=msg_id, disable_notification=True)
                    except Exception:
                        pass
                except Exception as e:
                    log.warning("leaderboard send failed: %s", e)

        except Exception as e:
            log.exception("leaderboard_loop iteration error: %s", e)

        await asyncio.sleep(LEADERBOARD_INTERVAL)


async def on_chat_member(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # when bot added to group, post premium intro
    try:
        my_chat_member = update.my_chat_member
        if not my_chat_member:
            return
        chat = my_chat_member.chat
        new = my_chat_member.new_chat_member
        if chat.type not in ("group","supergroup"):
            return
        if new and new.status in ("member","administrator"):
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("⚙️ Configure Token", callback_data="CFG_GROUP")],
                [InlineKeyboardButton("⚙️ Token Settings", callback_data="TOKENSET_GROUP")],
                [InlineKeyboardButton("🛠 Settings", callback_data="SET_GROUP")],
                [InlineKeyboardButton("📊 Status", callback_data="STATUS_GROUP")],
                [InlineKeyboardButton("🗑 Remove Token", callback_data="REMOVE_GROUP")],
            ])
            await context.bot.send_message(
                chat_id=chat.id,
                text="✅ *SpyTON BuyBot connected*\nTap *Configure Token* to start posting buys.",
                reply_markup=kb,
                parse_mode="Markdown"
            )
    except Exception:
        return

# -------------------- HEALTH SERVER --------------------
app_flask = Flask(__name__)

@app_flask.get("/")
def health():
    return "ok", 200

def run_flask():
    port = int(os.getenv("PORT", "8080"))
    app_flask.run(host="0.0.0.0", port=port)

# -------------------- MAIN --------------------
async def post_init(app: Application):
    # start tracker
    app.create_task(tracker_loop(app))
    log.info("Tracker started.")

    # start leaderboard refresher (Top-10) in trending channel
    if LEADERBOARD_ON and TRENDING_POST_CHAT_ID:
        app.create_task(leaderboard_loop(app))
        log.info("Leaderboard loop started.")

def main():
    if not BOT_TOKEN:
        raise SystemExit("BOT_TOKEN is missing.")
    application = ApplicationBuilder().token(BOT_TOKEN).post_init(post_init).build()

    application.add_handler(CommandHandler("start", start_cmd))
    application.add_handler(CommandHandler("lang", lang_cmd))
    application.add_handler(CommandHandler("addtoken", addtoken_cmd))
    application.add_handler(CommandHandler("tokens", tokens_cmd))
    application.add_handler(CommandHandler("mytokens", tokens_cmd))
    application.add_handler(CommandHandler("deltoken", deltoken_cmd))
    application.add_handler(CommandHandler("delpair", delpair_cmd))
    application.add_handler(CommandHandler("adset", adset_cmd))
    application.add_handler(CommandHandler("adclear", adclear_cmd))
    application.add_handler(CommandHandler("adstatus", adstatus_cmd))
    application.add_handler(CallbackQueryHandler(on_replace_button, pattern=r"^(REPL_|CANCEL_REPL$)"))
    application.add_handler(CallbackQueryHandler(on_button))
    application.add_handler(MessageHandler(filters.PHOTO, handle_photo))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))
    application.add_handler(ChatMemberHandler(on_chat_member, ChatMemberHandler.MY_CHAT_MEMBER))

    # flask in thread for Railway health
    import threading
    threading.Thread(target=run_flask, daemon=True).start()

    log.info("SpyTON Public BuyBot starting...")
    # If you accidentally deploy 2 instances, Telegram will throw Conflict (two getUpdates loops).
    # We retry instead of crashing the container.
    while True:
        try:
            application.run_polling(close_loop=False, drop_pending_updates=True)
            break
        except Conflict as e:
            log.error("Telegram polling Conflict (another instance running). Stop the other instance, then this will recover. %s", e)
            time.sleep(5)

if __name__ == "__main__":
    main()
