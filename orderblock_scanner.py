import os
import json
import time
import threading
from datetime import datetime, timezone
from collections import defaultdict

import requests
import websocket


# ============================================================
# BLOFIN
# ============================================================

BLOFIN_REST = "https://openapi.blofin.com"
BLOFIN_WS = "wss://openapi.blofin.com/ws/public"

DISCORD_WEBHOOK_URL = os.getenv(
    "ORDERBLOCK_DISCORD_WEBHOOK_URL"
)


# ============================================================
# STRATEGY SETTINGS
# ============================================================

TIMEFRAME = "15m"

# 5-bar swing:
# 2 candles on the left + swing candle + 2 candles on right
SWING_LEFT_RIGHT = 2

# ATR
ATR_PERIOD = 14

# Required expansion from OB to BOS
MIN_EXPANSION_ATR = 1.5

# Discord alert when price is within this distance
APPROACH_PERCENT = 0.5


# ============================================================
# HISTORICAL DATA
# ============================================================

# 500 completed 15M candles ≈ 5.2 days
HISTORY_CANDLES = 500

# Maximum active fresh OBs per coin
MAX_ACTIVE_OBS_PER_SYMBOL = 12


# ============================================================
# WEBSOCKET
# ============================================================

WS_GROUP_SIZE = 25


# ============================================================
# GLOBAL STATE
# ============================================================

symbols = []

candle_history = {}

active_obs = defaultdict(list)

latest_prices = {}

seen_ob_ids = set()

alerted_ob_ids = set()

locks = defaultdict(threading.Lock)


# ============================================================
# HELPERS
# ============================================================

def fmt_ts(ts_ms):
    return datetime.fromtimestamp(
        int(ts_ms) / 1000,
        tz=timezone.utc
    ).strftime("%Y-%m-%d %H:%M UTC")


def candle_direction(candle):
    if candle["close"] > candle["open"]:
        return "bull"

    if candle["close"] < candle["open"]:
        return "bear"

    return "doji"


def parse_candles(raw):
    candles = []

    for row in raw:

        if len(row) < 6:
            continue

        try:
            candles.append({
                "ts": int(row[0]),
                "open": float(row[1]),
                "high": float(row[2]),
                "low": float(row[3]),
                "close": float(row[4]),
                "confirm": str(row[-1]),
            })

        except (TypeError, ValueError):
            continue

    # BloFin returns newest first.
    # Our calculations require oldest -> newest.
    candles.sort(
        key=lambda x: x["ts"]
    )

    return candles


# ============================================================
# ATR(14)
# ============================================================

def calculate_atr(candles, period=ATR_PERIOD):

    atr_values = [None] * len(candles)

    if len(candles) <= period:
        return atr_values

    true_ranges = []

    for i, candle in enumerate(candles):

        if i == 0:

            tr = (
                candle["high"]
                - candle["low"]
            )

        else:

            previous_close = candles[
                i - 1
            ]["close"]

            tr = max(
                candle["high"]
                - candle["low"],

                abs(
                    candle["high"]
                    - previous_close
                ),

                abs(
                    candle["low"]
                    - previous_close
                ),
            )

        true_ranges.append(tr)

    # Initial ATR
    initial_atr = (
        sum(
            true_ranges[1:period + 1]
        )
        / period
    )

    atr_values[period] = initial_atr

    previous_atr = initial_atr

    # Wilder smoothing
    for i in range(
        period + 1,
        len(candles)
    ):

        previous_atr = (
            (
                previous_atr
                * (period - 1)
            )
            + true_ranges[i]
        ) / period

        atr_values[i] = previous_atr

    return atr_values


# ============================================================
# 5-BAR SWING DETECTION
# ============================================================

def is_swing_high(candles, index):

    if index < SWING_LEFT_RIGHT:
        return False

    if (
        index + SWING_LEFT_RIGHT
        >= len(candles)
    ):
        return False

    pivot_high = candles[
        index
    ]["high"]

    # Left side
    for i in range(
        index - SWING_LEFT_RIGHT,
        index
    ):

        if pivot_high <= candles[i]["high"]:
            return False

    # Right side
    for i in range(
        index + 1,
        index + SWING_LEFT_RIGHT + 1
    ):

        if pivot_high < candles[i]["high"]:
            return False

    return True


def is_swing_low(candles, index):

    if index < SWING_LEFT_RIGHT:
        return False

    if (
        index + SWING_LEFT_RIGHT
        >= len(candles)
    ):
        return False

    pivot_low = candles[
        index
    ]["low"]

    # Left side
    for i in range(
        index - SWING_LEFT_RIGHT,
        index
    ):

        if pivot_low >= candles[i]["low"]:
            return False

    # Right side
    for i in range(
        index + 1,
        index + SWING_LEFT_RIGHT + 1
    ):

        if pivot_low > candles[i]["low"]:
            return False

    return True


# ============================================================
# FIND LAST CONFIRMED SWING BEFORE BOS
# ============================================================

def find_last_swing_before_bos(
    candles,
    bos_index,
    direction
):

    # A 5-bar swing needs 2 candles AFTER
    # the pivot to be confirmed.

    latest_possible_swing = (
        bos_index
        - SWING_LEFT_RIGHT
    )

    for swing_index in range(
        latest_possible_swing,
        SWING_LEFT_RIGHT - 1,
        -1
    ):

        if direction == "bull":

            if is_swing_high(
                candles,
                swing_index
            ):

                return swing_index

        else:

            if is_swing_low(
                candles,
                swing_index
            ):

                return swing_index

    return None


# ============================================================
# FIND IMPULSE START
# ============================================================

def find_impulse_start(
    candles,
    bos_index,
    direction
):

    """
    Trace backwards from the BOS through the
    directional impulse candles.

    The first candle of the continuous directional
    impulse becomes the impulse start.
    """

    start = bos_index

    while start - 1 >= 0:

        previous_direction = candle_direction(
            candles[start - 1]
        )

        if previous_direction == direction:

            start -= 1

        else:

            break

    return start


# ============================================================
# FIND LAST OPPOSITE-COLOR CANDLE
# ============================================================

def find_order_block(
    candles,
    impulse_start,
    direction
):

    opposite = (
        "bear"
        if direction == "bull"
        else "bull"
    )

    # The OB must be the last opposite-color
    # candle immediately before the impulse.
    for index in range(
        impulse_start - 1,
        -1,
        -1
    ):

        if candle_direction(
            candles[index]
        ) == opposite:

            return index

        # Once we encounter another directional
        # candle before the OB, keep walking backward.
        #
        # This allows the code to find the actual
        # last opposite candle preceding the impulse.

    return None


# ============================================================
# DETECT BOS + OB
# ============================================================

def detect_order_blocks(
    symbol,
    candles
):

    if len(candles) < (
        ATR_PERIOD
        + SWING_LEFT_RIGHT * 2
        + 5
    ):

        return []

    atr_values = calculate_atr(
        candles
    )

    candidates = []

    # BOS candle
    for bos_index in range(
        ATR_PERIOD + 5,
        len(candles)
    ):

        bos_candle = candles[
            bos_index
        ]

        direction = candle_direction(
            bos_candle
        )

        if direction not in (
            "bull",
            "bear"
        ):

            continue

        # ----------------------------------------------------
        # 1. FIND LAST CONFIRMED 5-BAR SWING
        # ----------------------------------------------------

        swing_index = (
            find_last_swing_before_bos(
                candles,
                bos_index,
                direction
            )
        )

        if swing_index is None:
            continue

        swing_price = (
            candles[swing_index]["high"]
            if direction == "bull"
            else candles[swing_index]["low"]
        )

        # ----------------------------------------------------
        # 2. BODY CLOSE MUST BREAK STRUCTURE
        # ----------------------------------------------------

        if direction == "bull":

            if bos_candle["close"] <= swing_price:
                continue

        else:

            if bos_candle["close"] >= swing_price:
                continue

        # ----------------------------------------------------
        # 3. FIND IMPULSE START
        # ----------------------------------------------------

        impulse_start = find_impulse_start(
            candles,
            bos_index,
            direction
        )

        # Need an actual impulse candle before BOS
        if impulse_start >= bos_index:
            continue

        # ----------------------------------------------------
        # 4. FIND LAST OPPOSITE CANDLE
        # ----------------------------------------------------

        ob_index = find_order_block(
            candles,
            impulse_start,
            direction
        )

        if ob_index is None:
            continue

        ob_candle = candles[
            ob_index
        ]

        # ----------------------------------------------------
        # 5. ATR
        # ----------------------------------------------------

        atr = atr_values[
            bos_index
        ]

        if atr is None or atr <= 0:
            continue

        # ----------------------------------------------------
        # 6. SIGNIFICANT EXPANSION
        #
        # Measure from the OB boundary toward BOS.
        # ----------------------------------------------------

        if direction == "bull":

            expansion = (
                bos_candle["close"]
                - ob_candle["high"]
            )

        else:

            expansion = (
                ob_candle["low"]
                - bos_candle["close"]
            )

        if expansion <= 0:
            continue

        expansion_atr = (
            expansion / atr
        )

        if expansion_atr < MIN_EXPANSION_ATR:
            continue

        # ----------------------------------------------------
        # UNIQUE SYMBOL-SPECIFIC OB ID
        # ----------------------------------------------------

        ob_id = (
            f"{symbol}:"
            f"{ob_candle['ts']}:"
            f"{direction}"
        )

        candidates.append({

            "id": ob_id,

            "symbol": symbol,

            "direction": direction,

            "ob_ts": ob_candle["ts"],

            "ob_index": ob_index,

            "ob_open": ob_candle["open"],

            # FULL CANDLE ZONE
            "zone_high": ob_candle["high"],
            "zone_low": ob_candle["low"],

            "bos_ts": bos_candle["ts"],

            "bos_close": bos_candle["close"],

            "swing_price": swing_price,

            "atr": atr,

            "expansion": expansion,

            "expansion_atr": expansion_atr,

            "impulse_start_ts":
                candles[impulse_start]["ts"],
        })

    # Remove duplicate IDs
    unique = {}

    for ob in candidates:
        unique[ob["id"]] = ob

    return list(
        unique.values()
    )


# ============================================================
# MITIGATION / INVALIDATION
# ============================================================

def evaluate_historical_ob(
    ob,
    candles
):

    """
    Check every completed candle AFTER the OB.

    Rules:
    - Any wick touching [low, high] = mitigated.
    - Bull close below low = invalidated.
    - Bear close above high = invalidated.
    """

    for index in range(
        ob["ob_index"] + 1,
        len(candles)
    ):

        candle = candles[index]

        # ----------------------------------------------------
        # INVALIDATION
        # ----------------------------------------------------

        if (
            ob["direction"] == "bull"
            and candle["close"]
            < ob["zone_low"]
        ):

            return "invalidated"

        if (
            ob["direction"] == "bear"
            and candle["close"]
            > ob["zone_high"]
        ):

            return "invalidated"

        # ----------------------------------------------------
        # MITIGATION
        #
        # Any wick entering the zone.
        # ----------------------------------------------------

        wick_touches = (
            candle["low"]
            <= ob["zone_high"]
            and
            candle["high"]
            >= ob["zone_low"]
        )

        if wick_touches:

            return "mitigated"

    return "fresh"


# ============================================================
# LOAD HISTORICAL FRESH OBs
# ============================================================

def load_historical_obs(
    symbol,
    candles
):

    candidates = detect_order_blocks(
        symbol,
        candles
    )

    fresh = []

    # Newest first
    candidates.sort(
        key=lambda x: x["ob_ts"],
        reverse=True
    )

    for ob in candidates:

        status = evaluate_historical_ob(
            ob,
            candles
        )

        if status != "fresh":
            continue

        if ob["id"] in seen_ob_ids:
            continue

        seen_ob_ids.add(
            ob["id"]
        )

        fresh.append(
            ob
        )

        if len(fresh) >= (
            MAX_ACTIVE_OBS_PER_SYMBOL
        ):

            break

    return fresh


# ============================================================
# BLOFIN SYMBOLS
# ============================================================

def get_symbols():

    response = requests.get(
        f"{BLOFIN_REST}/api/v1/market/instruments",
        params={
            "instType": "SWAP"
        },
        timeout=20
    )

    response.raise_for_status()

    payload = response.json()

    result = []

    for item in payload.get(
        "data",
        []
    ):

        symbol = item.get(
            "instId",
            ""
        )

        state = item.get(
            "state",
            ""
        )

        if (
            symbol.endswith("-USDT")
            and state == "live"
        ):

            result.append(
                symbol
            )

    return sorted(
        set(result)
    )


# ============================================================
# HISTORICAL CANDLES
# ============================================================

def get_history(symbol):

    response = requests.get(
        f"{BLOFIN_REST}/api/v1/market/candles",
        params={
            "instId": symbol,
            "bar": TIMEFRAME,
            "limit": HISTORY_CANDLES
        },
        timeout=20
    )

    response.raise_for_status()

    payload = response.json()

    candles = parse_candles(
        payload.get(
            "data",
            []
        )
    )

    # Only completed candles
    candles = [
        candle
        for candle in candles
        if candle["confirm"] == "1"
    ]

    return candles


# ============================================================
# DISCORD
# ============================================================

def send_discord(
    ob,
    price,
    distance
):

    if not DISCORD_WEBHOOK_URL:

        print(
            "ERROR: "
            "ORDERBLOCK_DISCORD_WEBHOOK_URL "
            "is missing."
        )

        return False

    if ob["direction"] == "bull":

        direction_text = "🟢 BULLISH"

    else:

        direction_text = "🔴 BEARISH"

    embed = {

        "title":
            "🔔 15M ORDER BLOCK APPROACHING",

        "description":
            f"**{ob['symbol']}**",

        "fields": [

            {
                "name": "Direction",
                "value":
                    direction_text,
                "inline": True
            },

            {
                "name": "Current Price",
                "value":
                    f"`{price:g}`",
                "inline": True
            },

            {
                "name": "Distance",
                "value":
                    f"**{distance:.2f}%**",
                "inline": True
            },

            {
                "name": "OB Zone",
                "value":
                    f"`{ob['zone_low']:g}` → "
                    f"`{ob['zone_high']:g}`",
                "inline": False
            },

            {
                "name": "Alert Threshold",
                "value":
                    "`0.50%`",
                "inline": True
            },

            {
                "name": "OB Candle",
                "value":
                    f"`{fmt_ts(ob['ob_ts'])}`",
                "inline": True
            },

            {
                "name": "BOS Candle",
                "value":
                    f"`{fmt_ts(ob['bos_ts'])}`",
                "inline": True
            },

            {
                "name": "Impulse Start",
                "value":
                    f"`{fmt_ts(ob['impulse_start_ts'])}`",
                "inline": False
            },

            {
                "name": "ATR(14)",
                "value":
                    f"`{ob['atr']:.8g}`",
                "inline": True
            },

            {
                "name": "Expansion",
                "value":
                    f"`{ob['expansion_atr']:.2f}× ATR`",
                "inline": True
            },

            {
                "name": "Status",
                "value":
                    "`FRESH / APPROACHING`",
                "inline": False
            }
        ],

        "footer": {
            "text":
                "BloFin • 15M Order Block Scanner"
        }
    }

    payload = {
        "embeds": [embed]
    }

    try:

        response = requests.post(
            DISCORD_WEBHOOK_URL,
            json=payload,
            timeout=10
        )

        if response.status_code == 429:

            retry_after = 2

            try:

                retry_after = float(
                    response.json().get(
                        "retry_after",
                        2
                    )
                )

            except Exception:
                pass

            time.sleep(
                min(
                    retry_after + 0.5,
                    10
                )
            )

            response = requests.post(
                DISCORD_WEBHOOK_URL,
                json=payload,
                timeout=10
            )

        response.raise_for_status()

        print(
            f"ALERT: {ob['symbol']} "
            f"{ob['direction']} "
            f"distance={distance:.3f}%"
        )

        return True

    except Exception as error:

        print(
            f"Discord error for "
            f"{ob['symbol']}: {error}"
        )

        return False


# ============================================================
# LIVE PRICE DISTANCE
# ============================================================

def distance_to_ob(
    ob,
    price
):

    # Price already inside the block
    if (
        ob["zone_low"]
        <= price
        <= ob["zone_high"]
    ):

        return 0.0

    if ob["direction"] == "bull":

        # Bullish OB is normally below price.
        # Distance to nearest edge = high.
        if price > ob["zone_high"]:

            return (
                (
                    price
                    - ob["zone_high"]
                )
                / price
            ) * 100

        # Price below block.
        return (
            (
                ob["zone_low"]
                - price
            )
            / price
        ) * 100

    else:

        # Bearish OB is normally above price.
        # Distance to nearest edge = low.
        if price < ob["zone_low"]:

            return (
                (
                    ob["zone_low"]
                    - price
                )
                / price
            ) * 100

        # Price above block.
        return (
            (
                price
                - ob["zone_high"]
            )
            / price
        ) * 100


# ============================================================
# PROCESS LIVE PRICE
# ============================================================

def process_price(
    symbol,
    price
):

    latest_prices[
        symbol
    ] = price

    with locks[symbol]:

        remaining = []

        for ob in active_obs[
            symbol
        ]:

            # ------------------------------------------------
            # LIVE INVALIDATION
            # ------------------------------------------------

            if ob["direction"] == "bull":

                if price < ob["zone_low"]:

                    print(
                        f"INVALIDATED: "
                        f"{symbol} bullish OB"
                    )

                    continue

            else:

                if price > ob["zone_high"]:

                    print(
                        f"INVALIDATED: "
                        f"{symbol} bearish OB"
                    )

                    continue

            # ------------------------------------------------
            # LIVE MITIGATION
            # ------------------------------------------------

            if (
                ob["zone_low"]
                <= price
                <= ob["zone_high"]
            ):

                print(
                    f"MITIGATED: "
                    f"{symbol} "
                    f"{ob['direction']} OB"
                )

                continue

            # ------------------------------------------------
            # APPROACH
            # ------------------------------------------------

            distance = distance_to_ob(
                ob,
                price
            )

            if (
                distance
                <= APPROACH_PERCENT
            ):

                if (
                    ob["id"]
                    not in alerted_ob_ids
                ):

                    if send_discord(
                        ob,
                        price,
                        distance
                    ):

                        alerted_ob_ids.add(
                            ob["id"]
                        )

            remaining.append(
                ob
            )

        active_obs[
            symbol
        ] = remaining


# ============================================================
# PROCESS NEW COMPLETED 15M CANDLE
# ============================================================

def process_completed_candle(
    symbol,
    candle
):

    history = candle_history[
        symbol
    ]

    # Update same candle if necessary
    if (
        history
        and history[-1]["ts"]
        == candle["ts"]
    ):

        history[-1] = candle

    else:

        history.append(
            candle
        )

    if len(history) > HISTORY_CANDLES:

        candle_history[
            symbol
        ] = history[
            -HISTORY_CANDLES:
        ]

    else:

        candle_history[
            symbol
        ] = history

    # Recalculate all recent candidates
    candidates = detect_order_blocks(
        symbol,
        candle_history[symbol]
    )

    with locks[symbol]:

        for ob in candidates:

            # Already active
            if any(
                existing["id"]
                == ob["id"]
                for existing
                in active_obs[symbol]
            ):

                continue

            # Already seen
            if (
                ob["id"]
                in seen_ob_ids
            ):

                continue

            status = evaluate_historical_ob(
                ob,
                candle_history[symbol]
            )

            # Only untouched + valid blocks
            if status != "fresh":

                seen_ob_ids.add(
                    ob["id"]
                )

                continue

            # Mark known
            seen_ob_ids.add(
                ob["id"]
            )

            # Keep maximum number
            if len(
                active_obs[symbol]
            ) >= MAX_ACTIVE_OBS_PER_SYMBOL:

                active_obs[symbol].sort(
                    key=lambda x:
                        x["ob_ts"],
                    reverse=True
                )

                active_obs[symbol] = (
                    active_obs[symbol]
                    [
                        :MAX_ACTIVE_OBS_PER_SYMBOL - 1
                    ]
                )

            active_obs[
                symbol
            ].append(
                ob
            )

            print(
                f"NEW "
                f"{ob['direction'].upper()} OB | "
                f"{symbol} | "
                f"OB={fmt_ts(ob['ob_ts'])} | "
                f"BOS={fmt_ts(ob['bos_ts'])} | "
                f"Expansion="
                f"{ob['expansion_atr']:.2f}x ATR"
            )


# ============================================================
# WEBSOCKET SUBSCRIPTION
# ============================================================

def make_subscription(
    group
):

    args = []

    for symbol in group:

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
# WEBSOCKET WORKER
# ============================================================

def websocket_worker(
    group,
    group_number
):

    subscription = make_subscription(
        group
    )

    while True:

        try:

            def on_open(ws):

                print(
                    f"WS {group_number}: "
                    f"connected "
                    f"({len(group)} symbols)"
                )

                ws.send(
                    subscription
                )

                print(
                    f"WS {group_number}: "
                    f"subscription sent"
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
                if (
                    payload.get("event")
                    == "subscribe"
                ):

                    arg = payload.get(
                        "arg",
                        {}
                    )

                    print(
                        f"Subscribed: "
                        f"{arg.get('channel')} "
                        f"{arg.get('instId')}"
                    )

                    return

                # WebSocket error
                if (
                    payload.get("event")
                    == "error"
                ):

                    print(
                        f"WS {group_number} "
                        f"ERROR: "
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

                data = payload.get(
                    "data"
                )

                if (
                    not symbol
                    or not data
                ):

                    return

                # ------------------------------------------------
                # LIVE TICKER
                # ------------------------------------------------

                if channel == "tickers":

                    try:

                        item = data[0]

                        price = float(
                            item.get(
                                "last",
                                0
                            )
                        )

                        if price > 0:

                            process_price(
                                symbol,
                                price
                            )

                    except Exception:
                        pass

                # ------------------------------------------------
                # 15M CANDLE
                # ------------------------------------------------

                elif channel == "candle15m":

                    try:

                        row = data[0]

                        if len(row) < 6:
                            return

                        candle = {

                            "ts":
                                int(row[0]),

                            "open":
                                float(row[1]),

                            "high":
                                float(row[2]),

                            "low":
                                float(row[3]),

                            "close":
                                float(row[4]),

                            "confirm":
                                str(row[-1])
                        }

                        # Only completed candles.
                        if (
                            candle["confirm"]
                            != "1"
                        ):

                            return

                        process_completed_candle(
                            symbol,
                            candle
                        )

                    except Exception as error:

                        print(
                            f"Candle error "
                            f"{symbol}: "
                            f"{error}"
                        )

            def on_error(
                ws,
                error
            ):

                print(
                    f"WS {group_number} "
                    f"error: {error}"
                )

            def on_close(
                ws,
                code,
                message
            ):

                print(
                    f"WS {group_number} "
                    f"closed: "
                    f"{code} {message}"
                )

            ws = websocket.WebSocketApp(

                BLOFIN_WS,

                on_open=on_open,

                on_message=on_message,

                on_error=on_error,

                on_close=on_close
            )

            ws.run_forever(
                ping_interval=20,
                ping_timeout=10
            )

        except Exception as error:

            print(
                f"WS {group_number} "
                f"crashed: {error}"
            )

        print(
            f"WS {group_number}: "
            f"reconnecting in 5 seconds..."
        )

        time.sleep(5)


# ============================================================
# MAIN
# ============================================================

def main():

    if not DISCORD_WEBHOOK_URL:

        print(
            "ERROR: "
            "ORDERBLOCK_DISCORD_WEBHOOK_URL "
            "is not configured."
        )

        return

    print("=" * 70)

    print(
        "STRICT 15M ORDER BLOCK SCANNER"
    )

    print(
        "5-BAR BOS | BODY CLOSE | "
        "1.5x ATR(14)"
    )

    print(
        "HISTORICAL + REALTIME"
    )

    print(
        "APPROACH ALERT = 0.50%"
    )

    print("=" * 70)

    global symbols

    # --------------------------------------------------------
    # SYMBOLS
    # --------------------------------------------------------

    try:

        symbols = get_symbols()

    except Exception as error:

        print(
            f"Failed to get BloFin symbols: "
            f"{error}"
        )

        return

    print(
        f"Found {len(symbols)} "
        f"live USDT perpetuals."
    )

    # --------------------------------------------------------
    # HISTORICAL SCAN
    # --------------------------------------------------------

    print(
        "Loading historical 15M candles..."
    )

    print(
        "Checking old BOS + ATR + OB "
        "structures..."
    )

    for number, symbol in enumerate(
        symbols,
        1
    ):

        try:

            candles = get_history(
                symbol
            )

            candle_history[
                symbol
            ] = candles

            fresh_obs = load_historical_obs(
                symbol,
                candles
            )

            if fresh_obs:

                active_obs[
                    symbol
                ].extend(
                    fresh_obs
                )

                print(
                    f"[{number}/"
                    f"{len(symbols)}] "
                    f"{symbol}: "
                    f"{len(fresh_obs)} "
                    f"fresh historical OB(s)"
                )

        except Exception as error:

            print(
                f"[{number}/"
                f"{len(symbols)}] "
                f"{symbol}: "
                f"history error: "
                f"{error}"
            )

    total_active = sum(
        len(value)
        for value in active_obs.values()
    )

    print(
        "Historical scan complete."
    )

    print(
        f"Fresh historical OBs: "
        f"{total_active}"
    )

    # --------------------------------------------------------
    # WEBSOCKET GROUPS
    # --------------------------------------------------------

    groups = [

        symbols[i:i + WS_GROUP_SIZE]

        for i in range(
            0,
            len(symbols),
            WS_GROUP_SIZE
        )
    ]

    print(
        f"Starting {len(groups)} "
        f"WebSocket groups..."
    )

    for group_number, group in enumerate(
        groups,
        1
    ):

        thread = threading.Thread(

            target=websocket_worker,

            args=(
                group,
                group_number
            ),

            daemon=True
        )

        thread.start()

        # Stagger connections
        time.sleep(1.2)

        print(
            f"Started WS group "
            f"{group_number}/"
            f"{len(groups)}"
        )

    # --------------------------------------------------------
    # LIVE
    # --------------------------------------------------------

    print("=" * 70)

    print(
        "ORDER BLOCK SCANNER IS LIVE"
    )

    print(
        "Historical fresh OBs: ACTIVE"
    )

    print(
        "New 15M OBs: ACTIVE"
    )

    print(
        "Alert threshold: 0.50%"
    )

    print(
        "EMA/VWAP/volume filters: NONE"
    )

    print("=" * 70)

    while True:

        time.sleep(60)


# ============================================================
# START
# ============================================================

if __name__ == "__main__":
    main()
