import os
import json
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

import requests
import websocket


# ============================================================
# BLOFIN
# ============================================================

BLOFIN_REST = "https://openapi.blofin.com"
BLOFIN_WS = "wss://openapi.blofin.com/ws/public"

DISCORD_WEBHOOK_URL = os.getenv("ORDERBLOCK_DISCORD_WEBHOOK_URL")


# ============================================================
# STRATEGY SETTINGS
# ============================================================

TIMEFRAME = "15m"

# 5-bar swing:
# 2 candles left + swing candle + 2 candles right
SWING_LEFT_RIGHT = 2

ATR_PERIOD = 14

# Required expansion from OB to BOS
MIN_EXPANSION_ATR = 1.5

# Alert when price is within 0.5% of OB
APPROACH_PERCENT = 0.5

# Historical candles
HISTORY_CANDLES = 500

# Maximum fresh OBs kept per symbol
MAX_ACTIVE_OBS_PER_SYMBOL = 12


# ============================================================
# WEBSOCKET SETTINGS
# ============================================================

WS_GROUP_SIZE = 25

# BloFin allows 1 new WS connection per second per IP.
WS_CONNECT_STAGGER = 1.2

# BloFin requires text "ping" if no response arrives.
HEARTBEAT_SECONDS = 15

WS_RECEIVE_TIMEOUT = 5

WS_RECONNECT_DELAY = 5


# ============================================================
# GLOBAL STATE
# ============================================================

state_lock = threading.RLock()

# symbol -> list of completed candles
HISTORY = {}

# symbol -> list of active OB dictionaries
ACTIVE_OBS = {}

# OB IDs already alerted during this run
ALERTED_OB_IDS = set()

# Last completed candle timestamp processed for each symbol
LAST_COMPLETED_TS = {}

# Current prices
CURRENT_PRICES = {}

# WebSocket group number
WS_GROUP_COUNTER = 0


# ============================================================
# HELPERS
# ============================================================

def now_utc():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def safe_float(value):
    try:
        return float(value)
    except Exception:
        return None


def candle_from_row(row):
    """
    BloFin candle:
    [ts, open, high, low, close, ... , confirm]
    """
    if not row or len(row) < 5:
        return None

    try:
        ts = int(row[0])
        o = float(row[1])
        h = float(row[2])
        l = float(row[3])
        c = float(row[4])

        confirm = "1"

        if len(row) >= 9:
            confirm = str(row[8])

        return {
            "ts": ts,
            "open": o,
            "high": h,
            "low": l,
            "close": c,
            "confirm": confirm,
        }

    except Exception:
        return None


def candle_key(c):
    return c["ts"]


def is_bull(c):
    return c["close"] > c["open"]


def is_bear(c):
    return c["close"] < c["open"]


def is_doji(c):
    return c["close"] == c["open"]


# ============================================================
# ATR
# ============================================================

def calculate_atr(candles, period=14):
    if len(candles) < period + 1:
        return None

    trs = []

    for i in range(1, len(candles)):
        current = candles[i]
        previous = candles[i - 1]

        tr = max(
            current["high"] - current["low"],
            abs(current["high"] - previous["close"]),
            abs(current["low"] - previous["close"]),
        )

        trs.append(tr)

    if len(trs) < period:
        return None

    atr = sum(trs[:period]) / period

    for tr in trs[period:]:
        atr = ((atr * (period - 1)) + tr) / period

    return atr


# ============================================================
# SWING DETECTION
# ============================================================

def is_swing_high(candles, index):
    n = SWING_LEFT_RIGHT

    if index < n or index + n >= len(candles):
        return False

    value = candles[index]["high"]

    for i in range(index - n, index):
        if candles[i]["high"] >= value:
            return False

    for i in range(index + 1, index + n + 1):
        if candles[i]["high"] > value:
            return False

    return True


def is_swing_low(candles, index):
    n = SWING_LEFT_RIGHT

    if index < n or index + n >= len(candles):
        return False

    value = candles[index]["low"]

    for i in range(index - n, index):
        if candles[i]["low"] <= value:
            return False

    for i in range(index + 1, index + n + 1):
        if candles[i]["low"] < value:
            return False

    return True


def find_last_swing_before_bos(candles, bos_index, direction):
    """
    Find the latest confirmed 5-bar swing before the BOS candle.
    """

    start = bos_index - 1

    if direction == "bull":
        for i in range(start, SWING_LEFT_RIGHT - 1, -1):
            if is_swing_high(candles, i):
                return i

    else:
        for i in range(start, SWING_LEFT_RIGHT - 1, -1):
            if is_swing_low(candles, i):
                return i

    return None


# ============================================================
# IMPULSE DETECTION
# ============================================================

def find_impulse_start(candles, bos_index, direction):
    """
    Find the beginning of the directional impulse immediately
    leading into the BOS.

    Bullish:
        consecutive bullish candles ending at BOS

    Bearish:
        consecutive bearish candles ending at BOS
    """

    if bos_index <= 0:
        return None

    start = bos_index

    if direction == "bull":

        if not is_bull(candles[bos_index]):
            return None

        while start > 0 and is_bull(candles[start - 1]):
            start -= 1

    else:

        if not is_bear(candles[bos_index]):
            return None

        while start > 0 and is_bear(candles[start - 1]):
            start -= 1

    return start


# ============================================================
# ORDER BLOCK DETECTION
# ============================================================

def find_order_block(candles, impulse_start, direction):
    """
    Bullish OB:
        last bearish candle before bullish impulse

    Bearish OB:
        last bullish candle before bearish impulse

    We do NOT arbitrarily search several candles after BOS.
    """

    if impulse_start <= 0:
        return None

    if direction == "bull":

        for i in range(impulse_start - 1, -1, -1):

            if is_bear(candles[i]):
                return i

            # Stop if we encounter another directional structure.
            if is_bull(candles[i]):
                break

    else:

        for i in range(impulse_start - 1, -1, -1):

            if is_bull(candles[i]):
                return i

            if is_bear(candles[i]):
                break

    return None


# ============================================================
# OB ID
# ============================================================

def make_ob_id(symbol, ob_ts, direction):
    return f"{symbol}|{ob_ts}|{direction}"


# ============================================================
# DETECT ORDER BLOCKS
# ============================================================

def detect_order_blocks(symbol, candles):
    """
    Exact strategy:

    1. 5-bar swing
    2. Candle BODY closes beyond swing
    3. Directional impulse into BOS
    4. Last opposite-color candle before impulse = OB
    5. OB -> BOS expansion >= 1.5 ATR
    6. OB zone = complete candle high to low
    """

    if len(candles) < max(ATR_PERIOD + 10, 30):
        return []

    results = []

    atr = calculate_atr(candles, ATR_PERIOD)

    if atr is None or atr <= 0:
        return []

    # Ignore the newest candle if it is unconfirmed.
    last_index = len(candles) - 1

    for bos_index in range(
        SWING_LEFT_RIGHT * 2 + 5,
        last_index + 1
    ):

        bos = candles[bos_index]

        if bos.get("confirm") == "0":
            continue

        # ----------------------------------------------------
        # BULLISH BOS
        # ----------------------------------------------------

        swing_high_index = find_last_swing_before_bos(
            candles,
            bos_index,
            "bull",
        )

        if swing_high_index is not None:

            swing_high = candles[swing_high_index]["high"]

            # BODY CLOSE must break the swing high.
            if bos["close"] > swing_high:

                impulse_start = find_impulse_start(
                    candles,
                    bos_index,
                    "bull",
                )

                if impulse_start is not None:

                    ob_index = find_order_block(
                        candles,
                        impulse_start,
                        "bull",
                    )

                    if ob_index is not None:

                        ob = candles[ob_index]

                        # Actual expansion from OB toward BOS.
                        expansion = bos["close"] - ob["high"]

                        expansion_atr = expansion / atr

                        if expansion > 0 and expansion_atr >= MIN_EXPANSION_ATR:

                            ob_id = make_ob_id(
                                symbol,
                                ob["ts"],
                                "bull",
                            )

                            results.append(
                                {
                                    "id": ob_id,
                                    "symbol": symbol,
                                    "direction": "BULLISH",
                                    "ob_ts": ob["ts"],
                                    "ob_index": ob_index,
                                    "ob_open": ob["open"],
                                    "ob_high": ob["high"],
                                    "ob_low": ob["low"],
                                    "ob_close": ob["close"],
                                    "bos_ts": bos["ts"],
                                    "bos_close": bos["close"],
                                    "swing_ts": candles[swing_high_index]["ts"],
                                    "swing_price": swing_high,
                                    "impulse_ts": candles[impulse_start]["ts"],
                                    "atr": atr,
                                    "expansion": expansion,
                                    "expansion_atr": expansion_atr,
                                }
                            )

        # ----------------------------------------------------
        # BEARISH BOS
        # ----------------------------------------------------

        swing_low_index = find_last_swing_before_bos(
            candles,
            bos_index,
            "bear",
        )

        if swing_low_index is not None:

            swing_low = candles[swing_low_index]["low"]

            # BODY CLOSE must break the swing low.
            if bos["close"] < swing_low:

                impulse_start = find_impulse_start(
                    candles,
                    bos_index,
                    "bear",
                )

                if impulse_start is not None:

                    ob_index = find_order_block(
                        candles,
                        impulse_start,
                        "bear",
                    )

                    if ob_index is not None:

                        ob = candles[ob_index]

                        # Actual expansion from OB toward BOS.
                        expansion = ob["low"] - bos["close"]

                        expansion_atr = expansion / atr

                        if expansion > 0 and expansion_atr >= MIN_EXPANSION_ATR:

                            ob_id = make_ob_id(
                                symbol,
                                ob["ts"],
                                "bear",
                            )

                            results.append(
                                {
                                    "id": ob_id,
                                    "symbol": symbol,
                                    "direction": "BEARISH",
                                    "ob_ts": ob["ts"],
                                    "ob_index": ob_index,
                                    "ob_open": ob["open"],
                                    "ob_high": ob["high"],
                                    "ob_low": ob["low"],
                                    "ob_close": ob["close"],
                                    "bos_ts": bos["ts"],
                                    "bos_close": bos["close"],
                                    "swing_ts": candles[swing_low_index]["ts"],
                                    "swing_price": swing_low,
                                    "impulse_ts": candles[impulse_start]["ts"],
                                    "atr": atr,
                                    "expansion": expansion,
                                    "expansion_atr": expansion_atr,
                                }
                            )

    # Remove duplicate OB IDs.
    unique = {}

    for ob in results:
        unique[ob["id"]] = ob

    return list(unique.values())


# ============================================================
# HISTORICAL OB VALIDATION
# ============================================================

def historical_ob_status(ob, candles):
    """
    Check every candle after the OB.

    Bullish:
        candle CLOSE below OB low = invalidated
        candle WICK enters OB zone = mitigated

    Bearish:
        candle CLOSE above OB high = invalidated
        candle WICK enters OB zone = mitigated
    """

    ob_ts = ob["ob_ts"]

    for candle in candles:

        if candle["ts"] <= ob_ts:
            continue

        # Invalidation by candle CLOSE.
        if ob["direction"] == "BULLISH":

            if candle["close"] < ob["ob_low"]:
                return "invalidated"

        else:

            if candle["close"] > ob["ob_high"]:
                return "invalidated"

        # Wick touches / enters zone.
        if (
            candle["high"] >= ob["ob_low"]
            and candle["low"] <= ob["ob_high"]
        ):
            return "mitigated"

    return "fresh"


# ============================================================
# LOAD HISTORICAL DATA
# ============================================================

def get_history(symbol):
    url = f"{BLOFIN_REST}/api/v1/market/candles"

    params = {
        "instId": symbol,
        "bar": TIMEFRAME,
        "limit": str(HISTORY_CANDLES),
    }

    for attempt in range(3):

        try:

            response = requests.get(
                url,
                params=params,
                timeout=15,
            )

            if response.status_code != 200:
                print(
                    f"HISTORY ERROR {symbol}: "
                    f"HTTP {response.status_code}"
                )

                time.sleep(1 + attempt)
                continue

            payload = response.json()

            if payload.get("code") != "0":
                print(
                    f"HISTORY ERROR {symbol}: "
                    f"{payload.get('msg')}"
                )

                return []

            candles = []

            for row in payload.get("data", []):
                candle = candle_from_row(row)

                if candle and candle["confirm"] == "1":
                    candles.append(candle)

            candles.sort(key=lambda x: x["ts"])

            return candles

        except Exception as e:

            print(
                f"HISTORY EXCEPTION {symbol}: {e}"
            )

            time.sleep(1 + attempt)

    return []


# ============================================================
# GET LIVE SYMBOLS
# ============================================================

def get_symbols():

    url = f"{BLOFIN_REST}/api/v1/market/instruments"

    try:

        response = requests.get(
            url,
            timeout=15,
        )

        response.raise_for_status()

        payload = response.json()

        if payload.get("code") != "0":
            raise RuntimeError(payload.get("msg"))

        symbols = []

        for item in payload.get("data", []):

            symbol = item.get("instId", "")
            state = item.get("state", "")
            inst_type = item.get("instType", "")
            settle = item.get("settleCurrency", "")

            if (
                symbol.endswith("-USDT")
                and state == "live"
                and inst_type == "SWAP"
                and settle == "USDT"
            ):
                symbols.append(symbol)

        symbols = sorted(set(symbols))

        print()
        print("=" * 70)
        print(f"BLOFIN LIVE USDT PERPETUALS: {len(symbols)}")
        print("=" * 70)

        return symbols

    except Exception as e:

        print(f"SYMBOL ERROR: {e}")
        return []


# ============================================================
# HISTORICAL SCAN
# ============================================================

def load_historical_obs(symbols):

    print()
    print("=" * 70)
    print("STARTING HISTORICAL 15M OB SCAN")
    print(f"Symbols: {len(symbols)}")
    print(f"Candles per symbol: {HISTORY_CANDLES}")
    print("=" * 70)

    fresh_total = 0
    completed = 0

    # Keep concurrency moderate to avoid REST rate-limit problems.
    max_workers = 4

    def worker(symbol):
        candles = get_history(symbol)

        if not candles:
            return symbol, [], 0

        obs = detect_order_blocks(
            symbol,
            candles,
        )

        fresh = []

        for ob in obs:

            status = historical_ob_status(
                ob,
                candles,
            )

            if status == "fresh":
                fresh.append(ob)

        # Keep newest fresh OBs only.
        fresh.sort(
            key=lambda x: x["ob_ts"],
            reverse=True,
        )

        fresh = fresh[:MAX_ACTIVE_OBS_PER_SYMBOL]

        return symbol, candles, len(fresh), fresh

    with ThreadPoolExecutor(
        max_workers=max_workers
    ) as executor:

        futures = {
            executor.submit(worker, symbol): symbol
            for symbol in symbols
        }

        for future in as_completed(futures):

            symbol = futures[future]

            try:

                result = future.result()

                if len(result) == 4:

                    symbol, candles, count, fresh = result

                else:

                    symbol, candles, count = result
                    fresh = []

                with state_lock:

                    HISTORY[symbol] = candles

                    ACTIVE_OBS[symbol] = fresh

                    if candles:
                        LAST_COMPLETED_TS[symbol] = candles[-1]["ts"]

                completed += 1
                fresh_total += count

                if completed % 25 == 0 or completed == len(symbols):

                    print(
                        f"Historical scan: "
                        f"{completed}/{len(symbols)} | "
                        f"fresh OBs: {fresh_total}"
                    )

            except Exception as e:

                print(
                    f"HISTORICAL WORKER ERROR "
                    f"{symbol}: {e}"
                )

    print()
    print("=" * 70)
    print("HISTORICAL SCAN COMPLETE")
    print(f"Symbols processed: {completed}/{len(symbols)}")
    print(f"Fresh historical OBs: {fresh_total}")
    print("=" * 70)


# ============================================================
# DISTANCE TO OB
# ============================================================

def distance_to_ob(price, ob):

    high = ob["ob_high"]
    low = ob["ob_low"]

    if low <= price <= high:
        return 0.0

    if price > high:
        return ((price - high) / price) * 100

    return ((low - price) / price) * 100


# ============================================================
# DISCORD
# ============================================================

def send_discord(ob, price, reason="APPROACH"):

    if not DISCORD_WEBHOOK_URL:
        print("DISCORD WEBHOOK NOT SET")
        return False

    distance = distance_to_ob(
        price,
        ob,
    )

    direction = ob["direction"]

    if direction == "BULLISH":
        title = "🟢 15M BULLISH ORDER BLOCK"
    else:
        title = "🔴 15M BEARISH ORDER BLOCK"

    embed = {
        "title": title,
        "description": (
            f"**{ob['symbol']}**\n"
            f"Price is within **{APPROACH_PERCENT:.2f}%** "
            f"of a fresh 15M Order Block."
        ),
        "fields": [
            {
                "name": "Current Price",
                "value": f"{price:.12g}",
                "inline": True,
            },
            {
                "name": "Distance",
                "value": f"{distance:.3f}%",
                "inline": True,
            },
            {
                "name": "Alert",
                "value": reason,
                "inline": True,
            },
            {
                "name": "OB Zone",
                "value": (
                    f"{ob['ob_low']:.12g} → "
                    f"{ob['ob_high']:.12g}"
                ),
                "inline": False,
            },
            {
                "name": "OB Candle",
                "value": datetime.fromtimestamp(
                    ob["ob_ts"] / 1000,
                    timezone.utc,
                ).strftime("%Y-%m-%d %H:%M UTC"),
                "inline": True,
            },
            {
                "name": "BOS Candle",
                "value": datetime.fromtimestamp(
                    ob["bos_ts"] / 1000,
                    timezone.utc,
                ).strftime("%Y-%m-%d %H:%M UTC"),
                "inline": True,
            },
            {
                "name": "Impulse Start",
                "value": datetime.fromtimestamp(
                    ob["impulse_ts"] / 1000,
                    timezone.utc,
                ).strftime("%Y-%m-%d %H:%M UTC"),
                "inline": True,
            },
            {
                "name": "ATR(14)",
                "value": f"{ob['atr']:.8g}",
                "inline": True,
            },
            {
                "name": "Expansion",
                "value": (
                    f"{ob['expansion']:.8g} "
                    f"({ob['expansion_atr']:.2f} ATR)"
                ),
                "inline": True,
            },
            {
                "name": "Rules",
                "value": (
                    "5-bar BOS • "
                    "Body close • "
                    "≥1.5 ATR expansion • "
                    "Full candle OB"
                ),
                "inline": False,
            },
        ],
        "footer": {
            "text": "BloFin 15M Order Block Scanner"
        },
    }

    payload = {
        "username": "15M OB Scanner",
        "embeds": [embed],
    }

    try:

        response = requests.post(
            DISCORD_WEBHOOK_URL,
            json=payload,
            timeout=10,
        )

        if response.status_code in (200, 204):

            print(
                f"DISCORD ALERT SENT | "
                f"{ob['symbol']} | "
                f"{direction} | "
                f"{distance:.3f}%"
            )

            return True

        print(
            f"DISCORD ERROR: "
            f"HTTP {response.status_code} "
            f"{response.text[:300]}"
        )

    except Exception as e:

        print(
            f"DISCORD EXCEPTION: {e}"
        )

    return False


# ============================================================
# LIVE PRICE APPROACH
# ============================================================

def process_price(symbol, price):

    if price is None:
        return

    with state_lock:

        CURRENT_PRICES[symbol] = price

        obs = list(
            ACTIVE_OBS.get(symbol, [])
        )

    for ob in obs:

        # IMPORTANT:
        # Ticker price does NOT mitigate or invalidate.
        # It ONLY triggers the approach alert.
        #
        # Mitigation/invalidation is handled by completed
        # 15M candles according to the exact rules.

        distance = distance_to_ob(
            price,
            ob,
        )

        if distance <= APPROACH_PERCENT:

            ob_id = ob["id"]

            with state_lock:

                if ob_id in ALERTED_OB_IDS:
                    continue

                # Reserve alert before sending to prevent duplicates.
                ALERTED_OB_IDS.add(ob_id)

            success = send_discord(
                ob,
                price,
                "PRICE WITHIN 0.50%",
            )

            if not success:

                with state_lock:
                    ALERTED_OB_IDS.discard(ob_id)


# ============================================================
# COMPLETED CANDLE PROCESSING
# ============================================================

def process_completed_candle(
    symbol,
    candle,
):

    if candle.get("confirm") != "1":
        return

    ts = candle["ts"]

    with state_lock:

        previous_ts = LAST_COMPLETED_TS.get(
            symbol
        )

        if previous_ts is not None and ts <= previous_ts:
            return

        LAST_COMPLETED_TS[symbol] = ts

        candles = HISTORY.setdefault(
            symbol,
            [],
        )

        # Replace same candle if necessary.
        if candles and candles[-1]["ts"] == ts:
            candles[-1] = candle
        else:
            candles.append(candle)

        # Keep history bounded.
        if len(candles) > HISTORY_CANDLES:
            del candles[
                :-HISTORY_CANDLES
            ]

        existing_obs = list(
            ACTIVE_OBS.get(symbol, [])
        )

    # --------------------------------------------------------
    # 1. EXISTING OBs:
    #    candle close invalidation
    #    OR wick mitigation
    # --------------------------------------------------------

    remaining = []

    for ob in existing_obs:

        invalidated = False
        mitigated = False

        if ob["direction"] == "BULLISH":

            if candle["close"] < ob["ob_low"]:
                invalidated = True

        else:

            if candle["close"] > ob["ob_high"]:
                invalidated = True

        # Only a subsequent candle can mitigate.
        if candle["ts"] > ob["ob_ts"]:

            if (
                candle["high"] >= ob["ob_low"]
                and candle["low"] <= ob["ob_high"]
            ):
                mitigated = True

        if invalidated:

            print(
                f"OB INVALIDATED | "
                f"{symbol} | "
                f"{ob['direction']}"
            )

            continue

        if mitigated:

            print(
                f"OB MITIGATED | "
                f"{symbol} | "
                f"{ob['direction']}"
            )

            continue

        remaining.append(ob)

    # --------------------------------------------------------
    # 2. DETECT NEW OBs
    # --------------------------------------------------------

    with state_lock:
        candles_snapshot = list(
            HISTORY.get(symbol, [])
        )

    candidates = detect_order_blocks(
        symbol,
        candles_snapshot,
    )

    existing_ids = {
        ob["id"]
        for ob in remaining
    }

    new_obs = []

    for ob in candidates:

        if ob["id"] in existing_ids:
            continue

        # Only activate candidates whose OB candle is
        # before the current completed candle.
        if ob["ob_ts"] >= candle["ts"]:
            continue

        status = historical_ob_status(
            ob,
            candles_snapshot,
        )

        if status != "fresh":
            continue

        new_obs.append(ob)

        existing_ids.add(ob["id"])

        print(
            f"NEW {ob['direction']} OB | "
            f"{symbol} | "
            f"Expansion {ob['expansion_atr']:.2f} ATR"
        )

    # Newest first.
    combined = remaining + new_obs

    combined.sort(
        key=lambda x: x["ob_ts"],
        reverse=True,
    )

    combined = combined[
        :MAX_ACTIVE_OBS_PER_SYMBOL
    ]

    with state_lock:
        ACTIVE_OBS[symbol] = combined


# ============================================================
# WEBSOCKET MESSAGE HANDLER
# ============================================================

def handle_ws_message(
    message,
    subscribed_symbols,
):

    try:
        payload = json.loads(message)

    except Exception:
        return

    # Ignore pong.
    if payload == "pong":
        return

    if isinstance(payload, dict):

        event = payload.get("event")

        if event == "error":

            print(
                f"WS SUBSCRIPTION ERROR: "
                f"{payload.get('code')} "
                f"{payload.get('msg')}"
            )

            return

        # Subscription confirmation.
        if event == "subscribe":
            return

    arg = payload.get("arg", {})
    channel = arg.get("channel")
    symbol = arg.get("instId")

    if not symbol:
        return

    if symbol not in subscribed_symbols:
        return

    data = payload.get("data")

    if not data:
        return

    # --------------------------------------------------------
    # TICKER
    # --------------------------------------------------------

    if channel == "tickers":

        try:

            ticker = data[0]

            # BloFin ticker array/object can vary.
            # The normal ticker response is an object.

            if isinstance(ticker, dict):

                price = safe_float(
                    ticker.get("last")
                )

                process_price(
                    symbol,
                    price,
                )

            return

        except Exception as e:

            print(
                f"TICKER ERROR {symbol}: {e}"
            )

            return

    # --------------------------------------------------------
    # 15M CANDLE
    # --------------------------------------------------------

    if channel == "candle15m":

        try:

            row = data[0]

            candle = candle_from_row(
                row
            )

            if candle is None:
                return

            # Only completed candle matters
            # for OB creation/mitigation/invalidation.
            if candle["confirm"] == "1":

                process_completed_candle(
                    symbol,
                    candle,
                )

        except Exception as e:

            print(
                f"CANDLE ERROR {symbol}: {e}"
            )


# ============================================================
# WEBSOCKET WORKER
# ============================================================

def websocket_worker(
    group_number,
    symbols,
):

    global WS_GROUP_COUNTER

    channels = []

    for symbol in symbols:

        channels.append(
            {
                "channel": "candle15m",
                "instId": symbol,
            }
        )

        channels.append(
            {
                "channel": "tickers",
                "instId": symbol,
            }
        )

    while True:

        ws = None

        try:

            print(
                f"WS {group_number}: "
                f"connecting ({len(symbols)} symbols)"
            )

            ws = websocket.create_connection(
                BLOFIN_WS,
                timeout=WS_RECEIVE_TIMEOUT,
                enable_multithread=True,
            )

            print(
                f"WS {group_number}: connected"
            )

            subscribe_message = {
                "op": "subscribe",
                "args": channels,
            }

            ws.send(
                json.dumps(
                    subscribe_message,
                    separators=(",", ":"),
                )
            )

            print(
                f"WS {group_number}: "
                f"subscription sent "
                f"({len(channels)} channels)"
            )

            last_message_time = time.time()

            # ------------------------------------------------
            # RECEIVE LOOP
            # ------------------------------------------------

            while True:

                try:

                    message = ws.recv()

                    if message:

                        last_message_time = time.time()

                        handle_ws_message(
                            message,
                            set(symbols),
                        )

                except websocket.WebSocketTimeoutException:

                    # BloFin requires text "ping".
                    if (
                        time.time()
                        - last_message_time
                        >= HEARTBEAT_SECONDS
                    ):

                        try:

                            ws.send("ping")

                            print(
                                f"WS {group_number}: "
                                f"heartbeat ping"
                            )

                            last_message_time = time.time()

                        except Exception as e:

                            print(
                                f"WS {group_number}: "
                                f"heartbeat failed: {e}"
                            )

                            break

                except Exception as e:

                    print(
                        f"WS {group_number} "
                        f"receive error: {e}"
                    )

                    break

        except Exception as e:

            print(
                f"WS {group_number} "
                f"connection error: {e}"
            )

        finally:

            if ws is not None:

                try:
                    ws.close()
                except Exception:
                    pass

        print(
            f"WS {group_number}: "
            f"reconnecting in "
            f"{WS_RECONNECT_DELAY} seconds..."
        )

        time.sleep(
            WS_RECONNECT_DELAY
        )


# ============================================================
# START WEBSOCKETS
# ============================================================

def start_websockets(symbols):

    print()
    print("=" * 70)
    print("STARTING BLOFIN WEBSOCKETS")
    print("=" * 70)

    groups = [
        symbols[i:i + WS_GROUP_SIZE]
        for i in range(
            0,
            len(symbols),
            WS_GROUP_SIZE,
        )
    ]

    print(
        f"WebSocket groups: {len(groups)}"
    )

    print(
        f"Symbols per group: "
        f"{WS_GROUP_SIZE}"
    )

    for number, group in enumerate(
        groups,
        start=1,
    ):

        print(
            f"Starting WS {number}/{len(groups)} "
            f"({len(group)} symbols)"
        )

        thread = threading.Thread(
            target=websocket_worker,
            args=(number, group),
            daemon=True,
        )

        thread.start()

        # BloFin connection limit:
        # 1 new connection per second/IP.
        time.sleep(
            WS_CONNECT_STAGGER
        )

    print()
    print("=" * 70)
    print("ORDER BLOCK SCANNER IS LIVE")
    print("=" * 70)

    print(
        "Historical fresh OBs: ACTIVE"
    )

    print(
        "New 15M OBs: ACTIVE"
    )

    print(
        "Approach alert: 0.50%"
    )

    print(
        "Mitigation: completed 15M candle wick"
    )

    print(
        "Invalidation: completed 15M candle close"
    )

    print(
        "EMA/VWAP/volume filters: NONE"
    )

    print(
        f"Live since: {now_utc()}"
    )

    print("=" * 70)


# ============================================================
# STATUS THREAD
# ============================================================

def status_loop():

    while True:

        time.sleep(60)

        with state_lock:

            symbol_count = len(HISTORY)

            active_count = sum(
                len(value)
                for value in ACTIVE_OBS.values()
            )

            alerted_count = len(
                ALERTED_OB_IDS
            )

        print(
            f"[STATUS {now_utc()}] "
            f"symbols={symbol_count} | "
            f"active_OBs={active_count} | "
            f"alerts_sent={alerted_count}"
        )


# ============================================================
# MAIN
# ============================================================

def main():

    print("=" * 70)
    print("15M ORDER BLOCK SCANNER")
    print("=" * 70)

    print(
        "Strategy: 5-bar BOS + "
        "1.5 ATR expansion"
    )

    print(
        "OB: last opposite candle "
        "before impulse"
    )

    print(
        "Alert threshold: 0.50%"
    )

    print("=" * 70)

    if not DISCORD_WEBHOOK_URL:

        print(
            "ERROR: "
            "ORDERBLOCK_DISCORD_WEBHOOK_URL "
            "is missing."
        )

        raise SystemExit(1)

    # --------------------------------------------------------
    # GET SYMBOLS
    # --------------------------------------------------------

    symbols = get_symbols()

    if not symbols:

        print(
            "ERROR: No live USDT perpetuals found."
        )

        raise SystemExit(1)

    # --------------------------------------------------------
    # HISTORICAL DATA
    # --------------------------------------------------------

    load_historical_obs(
        symbols
    )

    # --------------------------------------------------------
    # START STATUS THREAD
    # --------------------------------------------------------

    status_thread = threading.Thread(
        target=status_loop,
        daemon=True,
    )

    status_thread.start()

    # --------------------------------------------------------
    # START WS
    # --------------------------------------------------------

    start_websockets(
        symbols
    )

    # --------------------------------------------------------
    # KEEP PROCESS ALIVE
    # --------------------------------------------------------

    while True:

        time.sleep(60)


if __name__ == "__main__":
    main()
