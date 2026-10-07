import os
import json
import time
import math
import threading
from datetime import datetime, timezone

import requests
import websocket


# ============================================================
# CONFIGURATION
# ============================================================

BLOFIN_REST = "https://openapi.blofin.com"
BLOFIN_WS = "wss://openapi.blofin.com/ws/public"

DISCORD_WEBHOOK_URL = os.getenv("ORDERBLOCK_DISCORD_WEBHOOK_URL")

TIMEFRAME = "15m"

# Fixed approach distance requested by user
APPROACH_PERCENT = 1.0

# Number of historical candles used to find fresh OBs
HISTORY_CANDLES = 120

# Number of candles used to determine recent structure
STRUCTURE_LOOKBACK = 10

# Minimum displacement size.
# The displacement candle must be meaningfully larger
# than the recent average candle body.
DISPLACEMENT_MULTIPLIER = 1.5

# Maximum age of an OB before we stop considering it fresh.
# 20 candles = 5 hours on 15m timeframe.
MAX_OB_AGE_CANDLES = 20

# Prevent repeated alerts for the same OB.
alerted_obs = set()

# symbol -> list of completed candles
candle_history = {}

# symbol -> list of active order blocks
active_obs = {}

# Protect shared data between WebSocket threads
data_lock = threading.Lock()


# ============================================================
# LOGGING
# ============================================================

def log(message):
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    print(f"[{now}] {message}", flush=True)


# ============================================================
# DISCORD
# ============================================================

def send_discord(message):
    if not DISCORD_WEBHOOK_URL:
        log("ERROR: ORDERBLOCK_DISCORD_WEBHOOK_URL is missing.")
        return False

    try:
        response = requests.post(
            DISCORD_WEBHOOK_URL,
            json={"content": message},
            timeout=10,
        )

        if response.status_code in (200, 204):
            return True

        if response.status_code == 429:
            log("Discord rate limit reached.")
            time.sleep(2)
            return False

        log(
            f"Discord error: HTTP {response.status_code} "
            f"{response.text[:300]}"
        )
        return False

    except Exception as e:
        log(f"Discord exception: {e}")
        return False


# ============================================================
# BLOFIN REST
# ============================================================

def get_symbols():
    url = f"{BLOFIN_REST}/api/v1/market/instruments"

    params = {
        "instType": "SWAP"
    }

    response = requests.get(
        url,
        params=params,
        timeout=15,
    )

    response.raise_for_status()

    data = response.json()

    symbols = []

    for item in data.get("data", []):
        inst_id = item.get("instId", "")

        if (
            inst_id.endswith("-USDT")
            and item.get("state", "live") == "live"
        ):
            symbols.append(inst_id)

    symbols = sorted(set(symbols))

    log(f"Found {len(symbols)} live USDT perpetuals.")

    return symbols


def get_history(symbol):
    url = f"{BLOFIN_REST}/api/v1/market/candles"

    params = {
        "instId": symbol,
        "bar": TIMEFRAME,
        "limit": HISTORY_CANDLES,
    }

    response = requests.get(
        url,
        params=params,
        timeout=15,
    )

    response.raise_for_status()

    data = response.json()

    candles = []

    for row in data.get("data", []):
        try:
            ts = int(row[0])
            open_price = float(row[1])
            high = float(row[2])
            low = float(row[3])
            close = float(row[4])
            volume = float(row[5])

            candles.append({
                "ts": ts,
                "open": open_price,
                "high": high,
                "low": low,
                "close": close,
                "volume": volume,
            })

        except Exception:
            continue

    candles.sort(key=lambda x: x["ts"])

    # Remove currently forming candle.
    now_ms = int(time.time() * 1000)
    current_candle_start = (
        now_ms // (15 * 60 * 1000)
    ) * (15 * 60 * 1000)

    candles = [
        c for c in candles
        if c["ts"] < current_candle_start
    ]

    return candles[-HISTORY_CANDLES:]


# ============================================================
# PRICE FORMATTING
# ============================================================

def format_price(price):
    if price >= 1000:
        return f"{price:,.2f}"
    if price >= 1:
        return f"{price:.4f}"
    if price >= 0.01:
        return f"{price:.6f}"
    return f"{price:.8f}"


def format_percent(value):
    return f"{value:.2f}%"


# ============================================================
# ORDER BLOCK DETECTION
# ============================================================

def candle_body(candle):
    return abs(candle["close"] - candle["open"])


def average_body(candles):
    if not candles:
        return 0

    return sum(candle_body(c) for c in candles) / len(candles)


def detect_order_blocks(symbol, candles):
    """
    Detect fresh 15m order blocks.

    Bullish OB:
        Last bearish candle before strong bullish displacement
        that breaks recent structure.

    Bearish OB:
        Last bullish candle before strong bearish displacement
        that breaks recent structure.
    """

    if len(candles) < STRUCTURE_LOOKBACK + 5:
        return []

    found = []

    start = STRUCTURE_LOOKBACK + 2

    for i in range(start, len(candles)):
        displacement = candles[i]

        previous = candles[:i]

        structure_window = previous[-STRUCTURE_LOOKBACK:]

        recent_high = max(c["high"] for c in structure_window)
        recent_low = min(c["low"] for c in structure_window)

        avg_body = average_body(
            previous[-20:]
        )

        if avg_body <= 0:
            continue

        displacement_body = candle_body(displacement)

        strong_displacement = (
            displacement_body >=
            avg_body * DISPLACEMENT_MULTIPLIER
        )

        # ----------------------------------------------------
        # BULLISH DISPLACEMENT
        # ----------------------------------------------------

        bullish_displacement = (
            displacement["close"] > displacement["open"]
            and displacement["close"] > recent_high
            and strong_displacement
        )

        if bullish_displacement:
            # Search backwards for the last bearish candle.
            for j in range(i - 1, max(-1, i - 6), -1):
                candidate = candles[j]

                if candidate["close"] < candidate["open"]:

                    ob = {
                        "symbol": symbol,
                        "type": "BULLISH",
                        "ts": candidate["ts"],
                        "open": candidate["open"],
                        "high": candidate["high"],
                        "low": candidate["low"],
                        "formation_close": displacement["close"],
                    }

                    found.append(ob)
                    break

        # ----------------------------------------------------
        # BEARISH DISPLACEMENT
        # ----------------------------------------------------

        bearish_displacement = (
            displacement["close"] < displacement["open"]
            and displacement["close"] < recent_low
            and strong_displacement
        )

        if bearish_displacement:
            # Search backwards for the last bullish candle.
            for j in range(i - 1, max(-1, i - 6), -1):
                candidate = candles[j]

                if candidate["close"] > candidate["open"]:

                    ob = {
                        "symbol": symbol,
                        "type": "BEARISH",
                        "ts": candidate["ts"],
                        "open": candidate["open"],
                        "high": candidate["high"],
                        "low": candidate["low"],
                        "formation_close": displacement["close"],
                    }

                    found.append(ob)
                    break

    # Remove duplicates.
    unique = {}

    for ob in found:
        key = (
            ob["symbol"],
            ob["type"],
            ob["ts"],
        )
        unique[key] = ob

    return list(unique.values())


# ============================================================
# OB VALIDATION
# ============================================================

def ob_key(ob):
    return (
        ob["symbol"],
        ob["type"],
        ob["ts"],
    )


def is_ob_still_fresh(ob, current_price):
    """
    Fresh means price has not entered the OB zone.
    """

    if ob["type"] == "BULLISH":

        # Bullish zone = low -> open
        zone_low = ob["low"]
        zone_high = ob["open"]

    else:

        # Bearish zone = open -> high
        zone_low = ob["open"]
        zone_high = ob["high"]

    return not (
        zone_low <= current_price <= zone_high
    )


def ob_zone(ob):
    if ob["type"] == "BULLISH":
        return (
            min(ob["low"], ob["open"]),
            max(ob["low"], ob["open"]),
        )

    return (
        min(ob["open"], ob["high"]),
        max(ob["open"], ob["high"]),
    )


# ============================================================
# 1% APPROACH DETECTION
# ============================================================

def calculate_distance_to_ob(ob, price):

    zone_low, zone_high = ob_zone(ob)

    # Price already inside OB
    if zone_low <= price <= zone_high:
        return 0.0

    # Price above OB
    if price > zone_high:
        return ((price - zone_high) / zone_high) * 100

    # Price below OB
    return ((zone_low - price) / zone_low) * 100


def approaching_ob(ob, price):

    distance = calculate_distance_to_ob(
        ob,
        price,
    )

    return distance <= APPROACH_PERCENT


# ============================================================
# DISCORD ALERT
# ============================================================

def send_ob_alert(ob, price):

    key = ob_key(ob)

    if key in alerted_obs:
        return

    zone_low, zone_high = ob_zone(ob)

    distance = calculate_distance_to_ob(
        ob,
        price,
    )

    formation_time = datetime.fromtimestamp(
        ob["ts"] / 1000,
        tz=timezone.utc,
    )

    message = (
        f"🔔 **15M ORDER BLOCK APPROACHING**\n\n"
        f"**{ob['symbol']}**\n"
        f"Direction: **{ob['type']}**\n\n"
        f"💰 Current Price: `{format_price(price)}`\n"
        f"🟦 OB Zone: `{format_price(zone_low)}` → "
        f"`{format_price(zone_high)}`\n"
        f"📏 Distance: **{format_percent(distance)}**\n"
        f"🎯 Alert Range: **1.00%**\n\n"
        f"🕐 OB Candle: "
        f"`{formation_time.strftime('%Y-%m-%d %H:%M UTC')}`\n"
        f"⏱️ Timeframe: **15M**\n"
        f"🟢 Status: **FRESH / APPROACHING**"
    )

    if send_discord(message):
        alerted_obs.add(key)
        log(
            f"ALERT: {ob['symbol']} "
            f"{ob['type']} OB "
            f"{distance:.2f}% away"
        )


# ============================================================
# PRICE PROCESSING
# ============================================================

def process_price(symbol, price):

    if price <= 0:
        return

    with data_lock:

        obs = active_obs.get(symbol, [])

        if not obs:
            return

        remaining = []

        for ob in obs:

            key = ob_key(ob)

            zone_low, zone_high = ob_zone(ob)

            # ------------------------------------------------
            # If price enters OB:
            # Mark it as used and remove it.
            # ------------------------------------------------

            if zone_low <= price <= zone_high:

                log(
                    f"{symbol}: {ob['type']} OB touched."
                )

                continue

            # ------------------------------------------------
            # Invalidate bullish OB if price breaks below it.
            # ------------------------------------------------

            if (
                ob["type"] == "BULLISH"
                and price < zone_low
            ):
                log(
                    f"{symbol}: bullish OB invalidated."
                )
                continue

            # ------------------------------------------------
            # Invalidate bearish OB if price breaks above it.
            # ------------------------------------------------

            if (
                ob["type"] == "BEARISH"
                and price > zone_high
            ):
                log(
                    f"{symbol}: bearish OB invalidated."
                )
                continue

            # ------------------------------------------------
            # 1% APPROACH ALERT
            # ------------------------------------------------

            if approaching_ob(ob, price):
                send_ob_alert(
                    ob,
                    price,
                )

            remaining.append(ob)

        active_obs[symbol] = remaining


# ============================================================
# CANDLE PROCESSING
# ============================================================

def process_completed_candle(symbol, candle):

    with data_lock:

        history = candle_history.setdefault(
            symbol,
            []
        )

        # Avoid duplicates.
        if history and history[-1]["ts"] == candle["ts"]:
            return

        history.append(candle)

        history.sort(
            key=lambda x: x["ts"]
        )

        history = history[-HISTORY_CANDLES:]

        candle_history[symbol] = history

        # Recalculate fresh OBs.
        detected = detect_order_blocks(
            symbol,
            history,
        )

        existing = {
            ob_key(ob)
            for ob in active_obs.get(symbol, [])
        }

        current_obs = active_obs.setdefault(
            symbol,
            []
        )

        current_ts = candle["ts"]

        for ob in detected:

            age_candles = (
                current_ts - ob["ts"]
            ) / (15 * 60 * 1000)

            if age_candles < 0:
                continue

            if age_candles > MAX_OB_AGE_CANDLES:
                continue

            key = ob_key(ob)

            if key in existing:
                continue

            # Make sure the OB is still fresh.
            # We don't know exact tick history here, so
            # only add newly formed OBs whose formation
            # candle is recent.
            if ob["ts"] == current_ts - (15 * 60 * 1000):

                current_obs.append(ob)

                log(
                    f"NEW {ob['type']} OB: "
                    f"{symbol} "
                    f"{format_price(ob['low'])} - "
                    f"{format_price(ob['high'])}"
                )


# ============================================================
# WEBSOCKET
# ============================================================

def subscribe_message(symbols):

    args = []

    for symbol in symbols:

        args.append({
            "channel": "candle15m",
            "instId": symbol,
        })

        args.append({
            "channel": "tickers",
            "instId": symbol,
        })

    return json.dumps({
        "op": "subscribe",
        "args": args,
    })


def on_message(ws, message):

    try:
        data = json.loads(message)
    except Exception:
        return

    if data.get("event") in (
        "subscribe",
        "unsubscribe",
        "pong",
    ):
        return

    arg = data.get("arg", {})

    channel = arg.get("channel")
    symbol = arg.get("instId")

    rows = data.get("data", [])

    if not symbol or not rows:
        return

    # --------------------------------------------------------
    # CANDLE
    # --------------------------------------------------------

    if channel == "candle15m":

        for row in rows:

            try:
                ts = int(row[0])
                open_price = float(row[1])
                high = float(row[2])
                low = float(row[3])
                close = float(row[4])
                volume = float(row[5])

                confirm = str(
                    row[8]
                ) if len(row) > 8 else "0"

            except Exception:
                continue

            # Only completed candles.
            if confirm != "1":
                continue

            candle = {
                "ts": ts,
                "open": open_price,
                "high": high,
                "low": low,
                "close": close,
                "volume": volume,
            }

            process_completed_candle(
                symbol,
                candle,
            )

    # --------------------------------------------------------
    # TICKER
    # --------------------------------------------------------

    elif channel == "tickers":

        item = rows[0]

        try:

            price = float(
                item.get("last")
                or item.get("lastPrice")
                or 0
            )

        except Exception:
            return

        if price > 0:
            process_price(
                symbol,
                price,
            )


def on_error(ws, error):
    log(f"WebSocket error: {error}")


def on_close(ws, close_status_code, close_msg):
    log(
        f"WebSocket closed: "
        f"{close_status_code} {close_msg}"
    )


def on_open(ws):
    log("WebSocket connected.")


# ============================================================
# WEBSOCKET WORKER
# ============================================================

def websocket_worker(symbols):

    while True:

        try:

            log(
                f"Starting WebSocket for "
                f"{len(symbols)} symbols..."
            )

            ws = websocket.WebSocketApp(
                BLOFIN_WS,
                on_open=on_open,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close,
            )

            ws.run_forever(
                ping_interval=15,
                ping_timeout=10,
                reconnect=5,
            )

        except Exception as e:

            log(
                f"WebSocket worker exception: {e}"
            )

        log("Reconnecting in 5 seconds...")
        time.sleep(5)


# ============================================================
# INITIALIZATION
# ============================================================

def initialize():

    log("========================================")
    log("15M ORDER BLOCK SCANNER")
    log("========================================")
    log("Fixed approach distance: 1%")
    log("BOS/displacement required")
    log("Fresh OBs only")
    log("No VWAP")
    log("No EMA filter")
    log("========================================")

    if not DISCORD_WEBHOOK_URL:
        raise RuntimeError(
            "ORDERBLOCK_DISCORD_WEBHOOK_URL is missing."
        )

    symbols = get_symbols()

    if not symbols:
        raise RuntimeError(
            "No BloFin USDT perpetuals found."
        )

    log(
        f"Loading {HISTORY_CANDLES} candles "
        f"for {len(symbols)} symbols..."
    )

    loaded = 0

    for symbol in symbols:

        try:

            candles = get_history(symbol)

            if candles:

                candle_history[symbol] = candles

                # Do NOT alert on historical OBs.
                # We only want fresh OBs formed after
                # the scanner starts.

                active_obs[symbol] = []

                loaded += 1

            if loaded % 25 == 0:
                log(
                    f"History loaded: "
                    f"{loaded}/{len(symbols)}"
                )

            # Keep comfortably below BloFin REST limits.
            time.sleep(0.25)

        except Exception as e:

            log(
                f"History failed for {symbol}: {e}"
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

    # BloFin limits subscription message size,
    # so split symbols across several connections.

    groups = []

    current = []

    # Keep each group comfortably below message limits.
    for symbol in symbols:

        current.append(symbol)

        if len(current) >= 40:

            groups.append(current)
            current = []

    if current:
        groups.append(current)

    log(
        f"Created {len(groups)} WebSocket groups."
    )

    threads = []

    for index, group in enumerate(groups):

        thread = threading.Thread(
            target=websocket_worker,
            args=(group,),
            daemon=True,
        )

        thread.start()

        threads.append(thread)

        # Avoid opening too many connections at once.
        time.sleep(1.2)

        log(
            f"Started WS group "
            f"{index + 1}/{len(groups)}"
        )

    log("ORDER BLOCK SCANNER IS LIVE.")

    while True:
        time.sleep(60)


if __name__ == "__main__":
    main()
