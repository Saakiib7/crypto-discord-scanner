import os
import json
import time
import threading
from collections import deque
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer

import requests
import websocket


# ============================================================
# SETTINGS
# ============================================================

BLOFIN_REST = "https://openapi.blofin.com"
BLOFIN_WS = "wss://openapi.blofin.com/ws/public"

DISCORD_WEBHOOK = os.environ.get("DISCORD_WEBHOOK_URL")

FAST_EMA = 9
SLOW_EMA = 16

VOLUME_LOOKBACK = 20
VOLUME_MULTIPLIER = 2.0

HISTORY_CANDLES = 100

# BloFin subscription messages must stay below 4096 bytes.
# 50 symbols per connection keeps us comfortably below that.
SYMBOLS_PER_WS = 50

RECONNECT_DELAY = 5

DISCORD_MAX_LENGTH = 1900


# ============================================================
# GLOBAL STATE
# ============================================================

state_lock = threading.Lock()

# symbol -> deque of completed candles
# Each candle:
# {
#   timestamp,
#   open,
#   high,
#   low,
#   close,
#   volume
# }
candle_history = {}

# Prevent duplicate alerts
last_alerted_candle = {}

# Number of websocket connections currently alive
active_connections = 0


# ============================================================
# SIMPLE HEALTH SERVER FOR RENDER
# ============================================================

class HealthHandler(BaseHTTPRequestHandler):

    def do_GET(self):

        if self.path in ("/", "/health"):

            body = (
                "BloFin realtime scanner is running.\n"
            ).encode()

            self.send_response(200)
            self.send_header(
                "Content-Type",
                "text/plain"
            )
            self.send_header(
                "Content-Length",
                str(len(body))
            )
            self.end_headers()

            self.wfile.write(body)

        else:

            self.send_response(404)
            self.end_headers()

    def log_message(self, format, *args):
        return


def start_health_server():

    port = int(
        os.environ.get("PORT", "10000")
    )

    server = HTTPServer(
        ("0.0.0.0", port),
        HealthHandler
    )

    print(
        f"Health server listening on port {port}"
    )

    server.serve_forever()


# ============================================================
# GET LIVE BLOFIN USDT PERPETUALS
# ============================================================

def get_symbols():

    url = (
        BLOFIN_REST
        + "/api/v1/market/instruments"
    )

    try:

        response = requests.get(
            url,
            params={
                "instType": "SWAP"
            },
            timeout=20
        )

        response.raise_for_status()

        data = response.json()

        if str(data.get("code", "0")) != "0":

            print(
                "BloFin instruments error:",
                data.get("msg")
            )

            return []

        symbols = []

        for item in data.get("data", []):

            if not isinstance(item, dict):
                continue

            if item.get("state") != "live":
                continue

            if item.get("instType") != "SWAP":
                continue

            if item.get("contractType") != "linear":
                continue

            if item.get("settleCurrency") != "USDT":
                continue

            symbol = item.get("instId")

            if symbol:
                symbols.append(symbol)

        return sorted(set(symbols))

    except Exception as e:

        print(
            "Failed to get BloFin symbols:",
            e
        )

        return []


# ============================================================
# EMA
# ============================================================

def calculate_ema(values, period):

    if len(values) < period:
        return None

    multiplier = 2.0 / (period + 1)

    ema = (
        sum(values[:period])
        / period
    )

    for value in values[period:]:

        ema = (
            (value - ema)
            * multiplier
            + ema
        )

    return ema


# ============================================================
# LOAD HISTORICAL CANDLES
# ============================================================

def load_history(symbol):

    url = (
        BLOFIN_REST
        + "/api/v1/market/candles"
    )

    try:

        response = requests.get(
            url,
            params={
                "instId": symbol,
                "bar": "15m",
                "limit": HISTORY_CANDLES
            },
            timeout=20
        )

        response.raise_for_status()

        data = response.json()

        if str(data.get("code", "0")) != "0":
            return False

        raw_candles = data.get(
            "data",
            []
        )

        if not raw_candles:
            return False

        candles = []

        # BloFin returns newest first.
        # Convert to oldest -> newest.
        for raw in reversed(raw_candles):

            if len(raw) < 9:
                continue

            # Only completed candles
            if str(raw[8]) != "1":
                continue

            try:

                candle = {
                    "timestamp": int(raw[0]),
                    "open": float(raw[1]),
                    "high": float(raw[2]),
                    "low": float(raw[3]),
                    "close": float(raw[4]),
                    "volume": float(raw[5])
                }

                candles.append(candle)

            except (
                ValueError,
                TypeError,
                IndexError
            ):
                continue

        if len(candles) < 25:
            return False

        with state_lock:

            candle_history[symbol] = deque(
                candles,
                maxlen=HISTORY_CANDLES
            )

        return True

    except Exception as e:

        print(
            f"History error {symbol}: {e}"
        )

        return False


# ============================================================
# CHECK COMPLETED CANDLE
# ============================================================

def process_completed_candle(
    symbol,
    candle
):

    with state_lock:

        history = candle_history.get(
            symbol
        )

        if history is None:
            return

        timestamp = candle["timestamp"]

        # Prevent duplicate candle processing
        if history and history[-1]["timestamp"] == timestamp:

            history[-1] = candle

        else:

            history.append(candle)

        if len(history) < 25:
            return

        candles = list(history)

        # Current completed candle
        current = candles[-1]

        # Previous completed candle
        previous = candles[-2]

        closes = [
            x["close"]
            for x in candles
        ]

        volumes = [
            x["volume"]
            for x in candles
        ]

        previous_closes = closes[:-1]

        # EMA 9
        current_ema9 = calculate_ema(
            closes,
            FAST_EMA
        )

        previous_ema9 = calculate_ema(
            previous_closes,
            FAST_EMA
        )

        # EMA 16
        current_ema16 = calculate_ema(
            closes,
            SLOW_EMA
        )

        previous_ema16 = calculate_ema(
            previous_closes,
            SLOW_EMA
        )

        if (
            current_ema9 is None
            or previous_ema9 is None
            or current_ema16 is None
            or previous_ema16 is None
        ):
            return

        # ----------------------------------------------------
        # CROSSOVER
        # ----------------------------------------------------

        bullish = (
            previous_ema9 <= previous_ema16
            and current_ema9 > current_ema16
        )

        bearish = (
            previous_ema9 >= previous_ema16
            and current_ema9 < current_ema16
        )

        if not bullish and not bearish:
            return

        # ----------------------------------------------------
        # VOLUME
        # ----------------------------------------------------

        previous_volumes = volumes[
            -(VOLUME_LOOKBACK + 1):-1
        ]

        if len(previous_volumes) != VOLUME_LOOKBACK:
            return

        average_volume = (
            sum(previous_volumes)
            / VOLUME_LOOKBACK
        )

        if average_volume <= 0:
            return

        volume_ratio = (
            current["volume"]
            / average_volume
        )

        if volume_ratio <= VOLUME_MULTIPLIER:
            return

        # ----------------------------------------------------
        # DUPLICATE PROTECTION
        # ----------------------------------------------------

        last_timestamp = last_alerted_candle.get(
            symbol
        )

        if last_timestamp == timestamp:
            return

        last_alerted_candle[symbol] = timestamp

        direction = (
            "BULLISH"
            if bullish
            else "BEARISH"
        )

        signal = {
            "symbol": symbol,
            "direction": direction,
            "price": current["close"],
            "ema9": current_ema9,
            "ema16": current_ema16,
            "volume_ratio": volume_ratio,
            "candle_timestamp": timestamp,
            "candle_close_timestamp": (
                timestamp
                + 15 * 60 * 1000
            )
        }

    # Send outside the state lock
    send_discord_alert(signal)


# ============================================================
# DISCORD
# ============================================================

def send_discord_alert(signal):

    if not DISCORD_WEBHOOK:

        print(
            "ERROR: DISCORD_WEBHOOK_URL missing."
        )

        return

    direction = signal["direction"]

    emoji = (
        "🟢"
        if direction == "BULLISH"
        else "🔴"
    )

    open_time = datetime.fromtimestamp(
        signal["candle_timestamp"] / 1000,
        tz=timezone.utc
    ).strftime(
        "%Y-%m-%d %H:%M UTC"
    )

    close_time = datetime.fromtimestamp(
        signal["candle_close_timestamp"] / 1000,
        tz=timezone.utc
    ).strftime(
        "%Y-%m-%d %H:%M UTC"
    )

    message = (
        "⚡ **BLOFIN 15M EMA 9/16 REALTIME ALERT**\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        f"{emoji} **{direction}** "
        f"`{signal['symbol']}`\n"
        f"Price: `{signal['price']:.8g}`\n"
        f"EMA 9: `{signal['ema9']:.8g}`\n"
        f"EMA 16: `{signal['ema16']:.8g}`\n"
        f"Volume: **{signal['volume_ratio']:.2f}x**\n"
        f"Candle: `{open_time} → {close_time}`\n"
        f"✅ Closed: `{close_time}`\n"
        "━━━━━━━━━━━━━━━━━━━━"
    )

    try:

        response = requests.post(
            DISCORD_WEBHOOK,
            json={
                "content": message,
                "allowed_mentions": {
                    "parse": []
                }
            },
            timeout=15
        )

        if response.status_code in (200, 204):

            print(
                f"🚨 ALERT SENT: "
                f"{signal['direction']} "
                f"{signal['symbol']} "
                f"{signal['volume_ratio']:.2f}x"
            )

        elif response.status_code == 429:

            print(
                "Discord rate limit. "
                "Alert was rate limited."
            )

        else:

            print(
                "Discord error:",
                response.status_code,
                response.text[:300]
            )

    except Exception as e:

        print(
            "Discord error:",
            e
        )


# ============================================================
# SPLIT SYMBOLS INTO WEBSOCKET GROUPS
# ============================================================

def make_groups(symbols):

    groups = []

    for i in range(
        0,
        len(symbols),
        SYMBOLS_PER_WS
    ):

        groups.append(
            symbols[
                i:i + SYMBOLS_PER_WS
            ]
        )

    return groups


# ============================================================
# WEBSOCKET CONNECTION
# ============================================================

def websocket_worker(
    symbols,
    connection_number
):

    global active_connections

    while True:

        ws = None

        try:

            print(
                f"[WS {connection_number}] "
                f"Connecting with "
                f"{len(symbols)} symbols..."
            )

            ws = websocket.WebSocketApp(
                BLOFIN_WS,

                on_open=lambda ws:
                    on_ws_open(
                        ws,
                        symbols,
                        connection_number
                    ),

                on_message=lambda ws, message:
                    on_ws_message(
                        ws,
                        message,
                        connection_number
                    ),

                on_error=lambda ws, error:
                    on_ws_error(
                        ws,
                        error,
                        connection_number
                    ),

                on_close=lambda ws, code, msg:
                    on_ws_close(
                        ws,
                        code,
                        msg,
                        connection_number
                    )
            )

            ws.run_forever(
                ping_interval=15,
                ping_timeout=10,
                ping_payload="ping"
            )

        except Exception as e:

            print(
                f"[WS {connection_number}] "
                f"Connection exception: {e}"
            )

        time.sleep(
            RECONNECT_DELAY
        )


# ============================================================
# WEBSOCKET OPEN
# ============================================================

def on_ws_open(
    ws,
    symbols,
    connection_number
):

    global active_connections

    active_connections += 1

    print(
        f"[WS {connection_number}] "
        f"Connected."
    )

    args = []

    for symbol in symbols:

        args.append(
            {
                "channel": "candle15m",
                "instId": symbol
            }
        )

    subscribe_message = {
        "op": "subscribe",
        "args": args
    }

    payload = json.dumps(
        subscribe_message,
        separators=(",", ":")
    )

    print(
        f"[WS {connection_number}] "
        f"Subscription size: "
        f"{len(payload)} bytes"
    )

    ws.send(payload)

    print(
        f"[WS {connection_number}] "
        f"Subscribed."
    )


# ============================================================
# WEBSOCKET MESSAGE
# ============================================================

def on_ws_message(
    ws,
    message,
    connection_number
):

    try:

        data = json.loads(message)

    except json.JSONDecodeError:

        return

    # Ignore subscription confirmations
    if data.get("event") in (
        "subscribe",
        "unsubscribe"
    ):
        return

    if data.get("event") == "error":

        print(
            f"[WS {connection_number}] "
            f"BloFin error: "
            f"{data}"
        )

        return

    arg = data.get(
        "arg",
        {}
    )

    if arg.get("channel") != "candle15m":
        return

    symbol = arg.get(
        "instId"
    )

    if not symbol:
        return

    rows = data.get(
        "data",
        []
    )

    if not rows:
        return

    # Futures candle data can be an array.
    # Handle both array/object safely.
    row = rows[0]

    try:

        if isinstance(row, dict):

            timestamp = int(
                row["ts"]
            )

            close = float(
                row["close"]
            )

            volume = float(
                row["vol"]
            )

            confirm = str(
                row["confirm"]
            )

            open_price = float(
                row["open"]
            )

            high = float(
                row["high"]
            )

            low = float(
                row["low"]
            )

        else:

            timestamp = int(
                row[0]
            )

            open_price = float(
                row[1]
            )

            high = float(
                row[2]
            )

            low = float(
                row[3]
            )

            close = float(
                row[4]
            )

            volume = float(
                row[5]
            )

            confirm = str(
                row[8]
            )

    except (
        KeyError,
        ValueError,
        TypeError,
        IndexError
    ):

        return

    # We ONLY act when the candle is completely closed.
    if confirm != "1":
        return

    candle = {
        "timestamp": timestamp,
        "open": open_price,
        "high": high,
        "low": low,
        "close": close,
        "volume": volume
    }

    process_completed_candle(
        symbol,
        candle
    )


# ============================================================
# WEBSOCKET ERROR / CLOSE
# ============================================================

def on_ws_error(
    ws,
    error,
    connection_number
):

    print(
        f"[WS {connection_number}] "
        f"Error: {error}"
    )


def on_ws_close(
    ws,
    code,
    message,
    connection_number
):

    global active_connections

    active_connections = max(
        0,
        active_connections - 1
    )

    print(
        f"[WS {connection_number}] "
        f"Closed. Code={code}, "
        f"Message={message}"
    )


# ============================================================
# START WEBSOCKET CONNECTIONS
# ============================================================

def start_websockets(symbols):

    groups = make_groups(
        symbols
    )

    print(
        f"Starting {len(groups)} "
        f"WebSocket connections..."
    )

    threads = []

    for index, group in enumerate(
        groups,
        start=1
    ):

        thread = threading.Thread(
            target=websocket_worker,
            args=(
                group,
                index
            ),
            daemon=True
        )

        thread.start()

        threads.append(
            thread
        )

        # BloFin limits new WS connections
        # to 1 per second per IP.
        time.sleep(1.1)

    return threads


# ============================================================
# MAIN
# ============================================================

def main():

    print("=" * 60)
    print(
        "BLOFIN REALTIME 15M EMA 9/16 SCANNER"
    )
    print("=" * 60)

    print(
        "Strategy:"
    )

    print(
        "15M EMA 9/16 crossover + "
        ">2x volume"
    )

    print(
        "Only completed candles trigger alerts."
    )

    print(
        "No VWAP."
    )

    print(
        "No 1H EMA200."
    )

    print(
        "Realtime WebSocket mode."
    )

    print()

    # --------------------------------------------------------
    # HEALTH SERVER
    # --------------------------------------------------------

    health_thread = threading.Thread(
        target=start_health_server,
        daemon=True
    )

    health_thread.start()

    # --------------------------------------------------------
    # GET SYMBOLS
    # --------------------------------------------------------

    symbols = get_symbols()

    if not symbols:

        print(
            "ERROR: No BloFin symbols found."
        )

        return

    print(
        f"Found {len(symbols)} "
        "live USDT perpetuals."
    )

    # --------------------------------------------------------
    # LOAD HISTORY
    # --------------------------------------------------------

    print()
    print(
        "Loading historical 15M candles..."
    )

    successful = 0

    for index, symbol in enumerate(
        symbols,
        start=1
    ):

        if load_history(symbol):

            successful += 1

        if (
            index % 50 == 0
            or index == len(symbols)
        ):

            print(
                f"History: "
                f"{index}/{len(symbols)} "
                f"({successful} ready)"
            )

        # Conservative REST rate.
        time.sleep(
            60.0 / 150.0
        )

    print()
    print(
        f"Historical data ready for "
        f"{successful}/{len(symbols)} symbols."
    )

    if successful < len(symbols):

        print(
            "Warning: some symbols have "
            "no history and may not generate "
            "signals until enough data arrives."
        )

    # --------------------------------------------------------
    # START WS
    # --------------------------------------------------------

    start_websockets(
        symbols
    )

    print()
    print(
        "=========================================="
    )
    print(
        "REALTIME SCANNER IS LIVE"
    )
    print(
        "Waiting for completed 15M candles..."
    )
    print(
        "=========================================="
    )

    # Keep main process alive.
    while True:

        time.sleep(60)

        print(
            f"[STATUS] "
            f"Active WS connections: "
            f"{active_connections}"
        )


if __name__ == "__main__":

    main()
