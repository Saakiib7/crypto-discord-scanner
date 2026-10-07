import os
import json
import time
import threading
from datetime import datetime, timezone

import requests
import websocket


# ============================================================
# CONFIG
# ============================================================

BLOFIN_REST = "https://openapi.blofin.com"
BLOFIN_WS = "wss://openapi.blofin.com/ws/public"

DISCORD_WEBHOOK_URL = os.getenv(
    "ORDERBLOCK_DISCORD_WEBHOOK_URL"
)

TIMEFRAME = "15m"

# User requested fixed 1% approach distance
APPROACH_PERCENT = 1.0

HISTORY_CANDLES = 120

STRUCTURE_LOOKBACK = 10

# Strong displacement requirement
DISPLACEMENT_MULTIPLIER = 1.5

# OB remains fresh for maximum 20 candles = 5 hours
MAX_OB_AGE_CANDLES = 20

# Keep memory under control
MAX_ACTIVE_OBS_PER_SYMBOL = 10


# ============================================================
# GLOBAL DATA
# ============================================================

candle_history = {}
active_obs = {}
alerted_obs = set()

data_lock = threading.Lock()


# ============================================================
# LOGGING
# ============================================================

def log(message):
    now = datetime.now(timezone.utc).strftime(
        "%Y-%m-%d %H:%M:%S UTC"
    )
    print(f"[{now}] {message}", flush=True)


# ============================================================
# DISCORD
# ============================================================

def send_discord(message):

    if not DISCORD_WEBHOOK_URL:
        log(
            "ERROR: "
            "ORDERBLOCK_DISCORD_WEBHOOK_URL "
            "is missing."
        )
        return False

    try:

        response = requests.post(
            DISCORD_WEBHOOK_URL,
            json={
                "content": message
            },
            timeout=10
        )

        if response.status_code in (200, 204):
            return True

        if response.status_code == 429:

            log(
                "Discord rate limit reached."
            )

            time.sleep(2)

            return False

        log(
            f"Discord HTTP "
            f"{response.status_code}: "
            f"{response.text[:300]}"
        )

        return False

    except Exception as e:

        log(
            f"Discord exception: {e}"
        )

        return False


# ============================================================
# BLOFIN REST — SYMBOLS
# ============================================================

def get_symbols():

    url = (
        f"{BLOFIN_REST}"
        f"/api/v1/market/instruments"
    )

    response = requests.get(
        url,
        params={
            "instType": "SWAP"
        },
        timeout=20
    )

    response.raise_for_status()

    payload = response.json()

    symbols = []

    for item in payload.get("data", []):

        inst_id = item.get(
            "instId",
            ""
        )

        state = item.get(
            "state",
            "live"
        )

        if (
            inst_id.endswith("-USDT")
            and state == "live"
        ):
            symbols.append(inst_id)

    symbols = sorted(
        set(symbols)
    )

    log(
        f"Found {len(symbols)} "
        f"live USDT perpetuals."
    )

    return symbols


# ============================================================
# BLOFIN REST — HISTORICAL CANDLES
# ============================================================

def get_history(symbol):

    url = (
        f"{BLOFIN_REST}"
        f"/api/v1/market/candles"
    )

    response = requests.get(
        url,
        params={
            "instId": symbol,
            "bar": TIMEFRAME,
            "limit": HISTORY_CANDLES
        },
        timeout=20
    )

    response.raise_for_status()

    payload = response.json()

    candles = []

    for row in payload.get("data", []):

        try:

            candles.append({
                "ts": int(row[0]),
                "open": float(row[1]),
                "high": float(row[2]),
                "low": float(row[3]),
                "close": float(row[4]),
                "volume": float(row[5])
            })

        except Exception:
            continue

    candles.sort(
        key=lambda x: x["ts"]
    )

    # Remove current unfinished candle.
    now_ms = int(
        time.time() * 1000
    )

    candle_ms = 15 * 60 * 1000

    current_start = (
        now_ms // candle_ms
    ) * candle_ms

    candles = [
        c for c in candles
        if c["ts"] < current_start
    ]

    return candles[-HISTORY_CANDLES:]


# ============================================================
# FORMATTING
# ============================================================

def format_price(price):

    if price >= 1000:
        return f"{price:,.2f}"

    if price >= 1:
        return f"{price:.4f}"

    if price >= 0.01:
        return f"{price:.6f}"

    return f"{price:.8f}"


def candle_body(candle):

    return abs(
        candle["close"] -
        candle["open"]
    )


# ============================================================
# ORDER BLOCK DETECTION
# ============================================================

def detect_latest_order_block(
    symbol,
    candles
):
    """
    Detect an OB created by the latest
    completed 15m candle.

    Bullish:
        latest candle is strong bullish
        displacement and breaks recent high.

        OB = last bearish candle before it.

    Bearish:
        latest candle is strong bearish
        displacement and breaks recent low.

        OB = last bullish candle before it.
    """

    if len(candles) < 25:
        return None

    displacement = candles[-1]

    previous = candles[:-1]

    structure = previous[
        -STRUCTURE_LOOKBACK:
    ]

    recent_high = max(
        c["high"]
        for c in structure
    )

    recent_low = min(
        c["low"]
        for c in structure
    )

    avg_body_candles = previous[-20:]

    avg_body = (
        sum(
            candle_body(c)
            for c in avg_body_candles
        )
        /
        len(avg_body_candles)
    )

    if avg_body <= 0:
        return None

    displacement_body = candle_body(
        displacement
    )

    strong_displacement = (
        displacement_body
        >=
        avg_body *
        DISPLACEMENT_MULTIPLIER
    )

    # ========================================================
    # BULLISH
    # ========================================================

    bullish = (
        displacement["close"]
        >
        displacement["open"]
        and
        displacement["close"]
        >
        recent_high
        and
        strong_displacement
    )

    if bullish:

        # Find the most recent bearish candle.
        for candidate in reversed(previous[-6:]):

            if (
                candidate["close"]
                <
                candidate["open"]
            ):

                return {
                    "symbol": symbol,
                    "type": "BULLISH",
                    "ts": candidate["ts"],
                    "open": candidate["open"],
                    "high": candidate["high"],
                    "low": candidate["low"],
                    "created_ts": displacement["ts"]
                }

    # ========================================================
    # BEARISH
    # ========================================================

    bearish = (
        displacement["close"]
        <
        displacement["open"]
        and
        displacement["close"]
        <
        recent_low
        and
        strong_displacement
    )

    if bearish:

        # Find the most recent bullish candle.
        for candidate in reversed(previous[-6:]):

            if (
                candidate["close"]
                >
                candidate["open"]
            ):

                return {
                    "symbol": symbol,
                    "type": "BEARISH",
                    "ts": candidate["ts"],
                    "open": candidate["open"],
                    "high": candidate["high"],
                    "low": candidate["low"],
                    "created_ts": displacement["ts"]
                }

    return None


# ============================================================
# OB KEY
# ============================================================

def ob_key(ob):

    return (
        ob["symbol"],
        ob["type"],
        ob["ts"]
    )


# ============================================================
# OB ZONE
# ============================================================

def get_ob_zone(ob):

    if ob["type"] == "BULLISH":

        # Bullish OB:
        # low → open
        return (
            min(
                ob["low"],
                ob["open"]
            ),
            max(
                ob["low"],
                ob["open"]
            )
        )

    # Bearish OB:
    # open → high
    return (
        min(
            ob["open"],
            ob["high"]
        ),
        max(
            ob["open"],
            ob["high"]
        )
    )


# ============================================================
# DISTANCE TO OB
# ============================================================

def distance_to_ob(
    ob,
    price
):

    zone_low, zone_high = (
        get_ob_zone(ob)
    )

    # Price is inside OB
    if (
        zone_low
        <=
        price
        <=
        zone_high
    ):
        return 0.0

    # Price above OB
    if price > zone_high:

        return (
            (price - zone_high)
            /
            zone_high
            *
            100
        )

    # Price below OB
    return (
        (zone_low - price)
        /
        zone_low
        *
        100
    )


# ============================================================
# APPROACH CHECK
# ============================================================

def is_approaching(
    ob,
    price
):

    return (
        distance_to_ob(
            ob,
            price
        )
        <=
        APPROACH_PERCENT
    )


# ============================================================
# DISCORD OB ALERT
# ============================================================

def send_ob_alert(
    ob,
    price
):

    key = ob_key(ob)

    if key in alerted_obs:
        return

    zone_low, zone_high = (
        get_ob_zone(ob)
    )

    distance = distance_to_ob(
        ob,
        price
    )

    created_time = datetime.fromtimestamp(
        ob["created_ts"] / 1000,
        tz=timezone.utc
    )

    ob_time = datetime.fromtimestamp(
        ob["ts"] / 1000,
        tz=timezone.utc
    )

    direction_emoji = (
        "🟢"
        if ob["type"] == "BULLISH"
        else
        "🔴"
    )

    message = (
        f"🔔 **15M ORDER BLOCK APPROACHING**\n\n"

        f"**{ob['symbol']}**\n"

        f"{direction_emoji} "
        f"Direction: **{ob['type']}**\n\n"

        f"💰 Current Price: "
        f"`{format_price(price)}`\n"

        f"🎯 OB Zone: "
        f"`{format_price(zone_low)}`"
        f" → "
        f"`{format_price(zone_high)}`\n"

        f"📏 Distance: "
        f"**{distance:.2f}%**\n"

        f"⚡ Alert threshold: "
        f"**1.00%**\n\n"

        f"📦 OB Candle: "
        f"`{ob_time.strftime('%Y-%m-%d %H:%M UTC')}`\n"

        f"🚀 BOS/Displacement: "
        f"`{created_time.strftime('%Y-%m-%d %H:%M UTC')}`\n"

        f"⏱️ Timeframe: **15M**\n"

        f"🟡 Status: **FRESH / APPROACHING**"
    )

    if send_discord(message):

        alerted_obs.add(key)

        log(
            f"ALERT SENT: "
            f"{ob['symbol']} "
            f"{ob['type']} "
            f"{distance:.2f}% away"
        )


# ============================================================
# PRICE PROCESSING
# ============================================================

def process_price(
    symbol,
    price
):

    if price <= 0:
        return

    with data_lock:

        obs = active_obs.get(
            symbol,
            []
        )

        if not obs:
            return

        remaining = []

        for ob in obs:

            zone_low, zone_high = (
                get_ob_zone(ob)
            )

            # ------------------------------------------------
            # OB TOUCHED
            # ------------------------------------------------

            if (
                zone_low
                <=
                price
                <=
                zone_high
            ):

                log(
                    f"{symbol}: "
                    f"{ob['type']} OB touched."
                )

                continue

            # ------------------------------------------------
            # INVALIDATION
            # ------------------------------------------------

            if (
                ob["type"] == "BULLISH"
                and
                price < zone_low
            ):

                log(
                    f"{symbol}: "
                    f"bullish OB invalidated."
                )

                continue

            if (
                ob["type"] == "BEARISH"
                and
                price > zone_high
            ):

                log(
                    f"{symbol}: "
                    f"bearish OB invalidated."
                )

                continue

            # ------------------------------------------------
            # 1% APPROACH
            # ------------------------------------------------

            if is_approaching(
                ob,
                price
            ):

                send_ob_alert(
                    ob,
                    price
                )

            remaining.append(ob)

        active_obs[symbol] = remaining


# ============================================================
# COMPLETED CANDLE PROCESSING
# ============================================================

def process_completed_candle(
    symbol,
    candle
):

    with data_lock:

        history = candle_history.setdefault(
            symbol,
            []
        )

        # Prevent duplicates
        if (
            history
            and
            history[-1]["ts"]
            ==
            candle["ts"]
        ):
            return

        history.append(candle)

        history.sort(
            key=lambda x: x["ts"]
        )

        history = history[-HISTORY_CANDLES:]

        candle_history[symbol] = history

        # Detect ONLY an OB created by this
        # newly completed candle.
        ob = detect_latest_order_block(
            symbol,
            history
        )

        if ob is None:
            return

        key = ob_key(ob)

        # Already known
        for existing in active_obs.get(
            symbol,
            []
        ):

            if ob_key(existing) == key:
                return

        # Fresh OB
        active_obs.setdefault(
            symbol,
            []
        ).append(ob)

        # Limit memory
        active_obs[symbol] = (
            active_obs[symbol]
            [-MAX_ACTIVE_OBS_PER_SYMBOL:]
        )

        zone_low, zone_high = (
            get_ob_zone(ob)
        )

        log(
            f"NEW {ob['type']} OB | "
            f"{symbol} | "
            f"Zone "
            f"{format_price(zone_low)}"
            f" → "
            f"{format_price(zone_high)}"
        )


# ============================================================
# WEBSOCKET SUBSCRIPTION
# ============================================================

def make_subscription(
    symbols
):

    args = []

    for symbol in symbols:

        args.append({
            "channel": "candle15m",
            "instId": symbol
        })

        args.append({
            "channel": "tickers",
            "instId": symbol
        })

    return json.dumps({
        "op": "subscribe",
        "args": args
    })


# ============================================================
# WEBSOCKET CALLBACKS
# ============================================================

def on_open(
    ws,
    symbols
):

    log(
        f"WebSocket connected. "
        f"Subscribing to "
        f"{len(symbols)} symbols..."
    )

    subscription = make_subscription(
        symbols
    )

    ws.send(subscription)

    log(
        f"Subscription sent for "
        f"{len(symbols)} symbols."
    )


def on_message(
    ws,
    message
):

    try:
        payload = json.loads(
            message
        )
    except Exception:
        return

    # Subscription confirmation
    if payload.get("event") == "subscribe":

        arg = payload.get(
            "arg",
            {}
        )

        log(
            "Subscribed: "
            f"{arg.get('channel')} "
            f"{arg.get('instId')}"
        )

        return

    if payload.get("event") == "error":

        log(
            f"BloFin WS ERROR: "
            f"{payload}"
        )

        return

    arg = payload.get(
        "arg",
        {}
    )

    channel = arg.get(
        "channel"
    )

    symbol = arg.get(
        "instId"
    )

    rows = payload.get(
        "data",
        []
    )

    if not symbol or not rows:
        return

    # ========================================================
    # 15M CANDLE
    # ========================================================

    if channel == "candle15m":

        for row in rows:

            try:

                ts = int(row[0])

                candle = {
                    "ts": ts,
                    "open": float(row[1]),
                    "high": float(row[2]),
                    "low": float(row[3]),
                    "close": float(row[4]),
                    "volume": float(row[5])
                }

                # Futures candle:
                # index 8 = confirm
                confirm = (
                    str(row[8])
                    if len(row) > 8
                    else "0"
                )

            except Exception:
                continue

            # Only completed candles
            if confirm != "1":
                continue

            process_completed_candle(
                symbol,
                candle
            )

    # ========================================================
    # TICKER
    # ========================================================

    elif channel == "tickers":

        try:

            item = rows[0]

            price = float(
                item.get("last", 0)
            )

        except Exception:
            return

        if price > 0:

            process_price(
                symbol,
                price
            )


def on_error(
    ws,
    error
):

    log(
        f"WebSocket error: {error}"
    )


def on_close(
    ws,
    close_status,
    close_message
):

    log(
        f"WebSocket closed: "
        f"{close_status} "
        f"{close_message}"
    )


# ============================================================
# WEBSOCKET WORKER
# ============================================================

def websocket_worker(
    symbols
):

    while True:

        try:

            log(
                f"Starting WebSocket for "
                f"{len(symbols)} symbols..."
            )

            ws = websocket.WebSocketApp(
                BLOFIN_WS,

                on_open=lambda ws:
                    on_open(
                        ws,
                        symbols
                    ),

                on_message=on_message,

                on_error=on_error,

                on_close=on_close
            )

            ws.run_forever(
                ping_interval=15,
                ping_timeout=10
            )

        except Exception as e:

            log(
                f"WebSocket exception: "
                f"{e}"
            )

        log(
            "Reconnecting in 5 seconds..."
        )

        time.sleep(5)


# ============================================================
# INITIALIZATION
# ============================================================

def initialize():

    log(
        "========================================"
    )

    log(
        "15M ORDER BLOCK SCANNER"
    )

    log(
        "========================================"
    )

    log(
        "Approach distance: 1%"
    )

    log(
        "BOS/displacement: REQUIRED"
    )

    log(
        "Fresh OBs only"
    )

    log(
        "No VWAP"
    )

    log(
        "No EMA filter"
    )

    log(
        "========================================"
    )

    if not DISCORD_WEBHOOK_URL:

        raise RuntimeError(
            "ORDERBLOCK_DISCORD_WEBHOOK_URL "
            "is missing."
        )

    symbols = get_symbols()

    if not symbols:

        raise RuntimeError(
            "No BloFin USDT perpetuals found."
        )

    log(
        f"Loading historical 15m candles "
        f"for {len(symbols)} symbols..."
    )

    loaded = 0

    for symbol in symbols:

        try:

            candles = get_history(
                symbol
            )

            if candles:

                candle_history[symbol] = (
                    candles
                )

                active_obs[symbol] = []

                loaded += 1

            if loaded % 25 == 0:

                log(
                    f"History loaded: "
                    f"{loaded}/"
                    f"{len(symbols)}"
                )

            # Stay comfortably under
            # REST request limits.
            time.sleep(0.25)

        except Exception as e:

            log(
                f"History failed for "
                f"{symbol}: {e}"
            )

    log(
        f"History initialization complete: "
        f"{loaded}/{len(symbols)} symbols."
    )

    return symbols


# ============================================================
# MAIN
# ============================================================

def main():

    symbols = initialize()

    # BloFin limits the total subscription
    # message length to 4096 bytes.
    #
    # 25 symbols × 2 channels is safely
    # below that limit.

    GROUP_SIZE = 25

    groups = [
        symbols[i:i + GROUP_SIZE]
        for i in range(
            0,
            len(symbols),
            GROUP_SIZE
        )
    ]

    log(
        f"Created {len(groups)} "
        f"WebSocket groups."
    )

    threads = []

    for index, group in enumerate(
        groups
    ):

        thread = threading.Thread(
            target=websocket_worker,
            args=(group,),
            daemon=True
        )

        thread.start()

        threads.append(thread)

        # BloFin connection creation
        # should be staggered.
        time.sleep(1.2)

        log(
            f"Started WS group "
            f"{index + 1}/"
            f"{len(groups)}"
        )

    log(
        "========================================"
    )

    log(
        "ORDER BLOCK SCANNER IS LIVE."
    )

    log(
        "Waiting for fresh 15M OBs..."
    )

    log(
        "========================================"
    )

    while True:
        time.sleep(60)


# ============================================================
# START
# ============================================================

if __name__ == "__main__":
    main()
