import os
import json
import time
import threading
from datetime import datetime, timezone
from collections import defaultdict

import requests
import websocket

BLOFIN_REST = "https://openapi.blofin.com"
BLOFIN_WS = "wss://openapi.blofin.com/ws/public"
DISCORD_WEBHOOK_URL = os.getenv("ORDERBLOCK_DISCORD_WEBHOOK_URL")

TIMEFRAME = "15m"

# Strategy rules
ATR_PERIOD = 14
SWING_LEFT_RIGHT = 2          # 5-bar swing = 2 candles each side
MIN_EXPANSION_ATR = 1.5
APPROACH_PERCENT = 0.5

# Historical data
HISTORY_CANDLES = 500

# Safety / connection settings
MAX_ACTIVE_OBS_PER_SYMBOL = 12
WS_GROUP_SIZE = 25

symbols = []
active_obs = defaultdict(list)
seen_ob_ids = set()
alerted_ob_ids = set()
latest_prices = {}
candle_history = {}
locks = defaultdict(threading.Lock)


def fmt_ts(ts_ms):
    return datetime.fromtimestamp(
        int(ts_ms) / 1000,
        tz=timezone.utc
    ).strftime("%Y-%m-%d %H:%M UTC")


def body_direction(candle):
    if candle["close"] > candle["open"]:
        return "bull"

    if candle["close"] < candle["open"]:
        return "bear"

    return "doji"


def true_range(candle, previous_close):
    return max(
        candle["high"] - candle["low"],
        abs(candle["high"] - previous_close),
        abs(candle["low"] - previous_close),
    )


def atr_series(candles, period=ATR_PERIOD):
    """
    Wilder ATR(14).
    """

    result = [None] * len(candles)
    trs = [None] * len(candles)

    for i, candle in enumerate(candles):

        if i == 0:
            trs[i] = candle["high"] - candle["low"]

        else:
            trs[i] = true_range(
                candle,
                candles[i - 1]["close"]
            )

    if len(candles) <= period:
        return result

    initial_atr = sum(trs[1:period + 1]) / period

    result[period] = initial_atr

    previous_atr = initial_atr

    for i in range(period + 1, len(candles)):

        previous_atr = (
            (previous_atr * (period - 1)) + trs[i]
        ) / period

        result[i] = previous_atr

    return result


def is_swing_high(candles, index):

    if index < SWING_LEFT_RIGHT:
        return False

    if index + SWING_LEFT_RIGHT >= len(candles):
        return False

    high = candles[index]["high"]

    for j in range(
        index - SWING_LEFT_RIGHT,
        index
    ):

        if high <= candles[j]["high"]:
            return False

    for j in range(
        index + 1,
        index + SWING_LEFT_RIGHT + 1
    ):

        if high < candles[j]["high"]:
            return False

    return True


def is_swing_low(candles, index):

    if index < SWING_LEFT_RIGHT:
        return False

    if index + SWING_LEFT_RIGHT >= len(candles):
        return False

    low = candles[index]["low"]

    for j in range(
        index - SWING_LEFT_RIGHT,
        index
    ):

        if low >= candles[j]["low"]:
            return False

    for j in range(
        index + 1,
        index + SWING_LEFT_RIGHT + 1
    ):

        if low > candles[j]["low"]:
            return False

    return True


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

    candles.sort(
        key=lambda candle: candle["ts"]
    )

    return candles


def get_symbols():

    response = requests.get(
        f"{BLOFIN_REST}/api/v1/market/instruments",
        params={
            "instType": "SWAP"
        },
        timeout=15,
    )

    response.raise_for_status()

    payload = response.json()

    result = []

    for item in payload.get("data", []):

        inst_id = item.get("instId", "")
        state = item.get("state", "")

        if (
            inst_id.endswith("-USDT")
            and state == "live"
        ):

            result.append(inst_id)

    return sorted(set(result))


def get_history(symbol):

    response = requests.get(
        f"{BLOFIN_REST}/api/v1/market/candles",
        params={
            "instId": symbol,
            "bar": TIMEFRAME,
            "limit": HISTORY_CANDLES,
        },
        timeout=20,
    )

    response.raise_for_status()

    payload = response.json()

    candles = parse_candles(
        payload.get("data", [])
    )

    # Only completed 15M candles.
    candles = [
        candle
        for candle in candles
        if candle["confirm"] == "1"
    ]

    return candles


def find_last_opposite_before_impulse(
    candles,
    impulse_start,
    direction
):

    wanted_color = (
        "bear"
        if direction == "bull"
        else "bull"
    )

    for index in range(
        impulse_start - 1,
        -1,
        -1
    ):

        if body_direction(
            candles[index]
        ) == wanted_color:

            return index

    return None


def detect_ob_candidates(candles):

    """
    Exact strategy:

    1. 5-bar swing structure.
    2. Body closes through last swing.
    3. Trace backwards to find impulse start.
    4. Last opposite-color candle before impulse = OB.
    5. Expansion from OB to BOS >= 1.5 ATR(14).
    """

    if len(candles) < ATR_PERIOD + 10:
        return []

    atrs = atr_series(candles)

    candidates = []

    for bos_index in range(
        ATR_PERIOD + 5,
        len(candles)
    ):

        bos_candle = candles[bos_index]

        direction = body_direction(
            bos_candle
        )

        if direction not in (
            "bull",
            "bear"
        ):
            continue

        # Find the most recent confirmed
        # 5-bar swing before the BOS.
        swing_index = None

        for swing in range(
            bos_index - 1,
            SWING_LEFT_RIGHT - 1,
            -1
        ):

            if swing + SWING_LEFT_RIGHT >= bos_index:
                continue

            if (
                direction == "bull"
                and is_swing_high(
                    candles,
                    swing
                )
            ):

                swing_index = swing
                break

            if (
                direction == "bear"
                and is_swing_low(
                    candles,
                    swing
                )
            ):

                swing_index = swing
                break

        if swing_index is None:
            continue

        swing_price = (
            candles[swing_index]["high"]
            if direction == "bull"
            else candles[swing_index]["low"]
        )

        # BODY CLOSE must break structure.
        if (
            direction == "bull"
            and bos_candle["close"] <= swing_price
        ):
            continue

        if (
            direction == "bear"
            and bos_candle["close"] >= swing_price
        ):
            continue

        # Trace backwards through the
        # directional impulse run.
        impulse_start = bos_index

        while impulse_start - 1 >= 0:

            previous_direction = body_direction(
                candles[impulse_start - 1]
            )

            if previous_direction == direction:
                impulse_start -= 1

            else:
                break

        # Last opposite-color candle before
        # the impulse started.
        ob_index = find_last_opposite_before_impulse(
            candles,
            impulse_start,
            direction
        )

        if ob_index is None:
            continue

        ob_candle = candles[ob_index]

        atr = atrs[bos_index]

        if atr is None or atr <= 0:
            continue

        # Significant expansion from the OB
        # to the BOS.
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

        if expansion < (
            MIN_EXPANSION_ATR * atr
        ):
            continue

        ob_id = (
            f"{ob_candle['ts']}:{direction}"
        )

        if ob_id in seen_ob_ids:
            continue

        candidates.append({

            "id": ob_id,

            "symbol": None,

            "direction": direction,

            "ob_ts": ob_candle["ts"],

            "ob_index": ob_index,

            "ob_open": ob_candle["open"],

            # FULL candle high → low.
            "zone_high": ob_candle["high"],
            "zone_low": ob_candle["low"],

            "bos_ts": bos_candle["ts"],
            "bos_close": bos_candle["close"],

            "swing_price": swing_price,

            "atr": atr,

            "expansion": expansion,

            "impulse_start_ts":
                candles[impulse_start]["ts"],
        })

    return candidates


def historical_active_obs(
    symbol,
    candles
):

    candidates = detect_ob_candidates(
        candles
    )

    active = []

    for ob in candidates:

        touched = False
        invalidated = False

        # Examine every candle AFTER
        # the OB candle.
        for index in range(
            ob["ob_index"] + 1,
            len(candles)
        ):

            candle = candles[index]

            # Invalidation.
            if (
                ob["direction"] == "bull"
                and candle["close"]
                < ob["zone_low"]
            ):

                invalidated = True
                break

            if (
                ob["direction"] == "bear"
                and candle["close"]
                > ob["zone_high"]
            ):

                invalidated = True
                break

            # Any wick touching the block
            # means mitigation.
            if (
                candle["low"]
                <= ob["zone_high"]
                and
                candle["high"]
                >= ob["zone_low"]
            ):

                touched = True
                break

        if touched or invalidated:
            continue

        ob["symbol"] = symbol

        active.append(ob)

    active.sort(
        key=lambda x: x["ob_ts"],
        reverse=True
    )

    return active[
        :MAX_ACTIVE_OBS_PER_SYMBOL
    ]


def send_discord(
    ob,
    price,
    distance_pct
):

    if not DISCORD_WEBHOOK_URL:

        print(
            "ERROR: ORDERBLOCK_DISCORD_WEBHOOK_URL "
            "is missing."
        )

        return False

    direction = (
        "🟢 BULLISH"
        if ob["direction"] == "bull"
        else "🔴 BEARISH"
    )

    embed = {

        "title":
            f"{direction} 15M ORDER BLOCK",

        "description":
            f"**{ob['symbol']}** is within "
            f"**{distance_pct:.2f}%** of a "
            f"fresh 15M order block.",

        "fields": [

            {
                "name": "Current Price",
                "value": f"`{price:g}`",
                "inline": True
            },

            {
                "name": "OB Zone",
                "value":
                    f"`{ob['zone_low']:g} → "
                    f"{ob['zone_high']:g}`",
                "inline": True
            },

            {
                "name": "Alert Threshold",
                "value": "`0.50%`",
                "inline": True
            },

            {
                "name": "OB Candle",
                "value":
                    f"`{fmt_ts(ob['ob_ts'])}`",
                "inline": False
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
                "inline": True
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
                    f"`{ob['expansion']:.8g}` "
                    f"({ob['expansion'] / ob['atr']:.2f}× ATR)",
                "inline": True
            },

            {
                "name": "Status",
                "value":
                    "`FRESH / APPROACHING`",
                "inline": False
            },
        ],

        "footer": {
            "text":
                "BloFin 15M Order Block Scanner"
        },
    }

    try:

        response = requests.post(
            DISCORD_WEBHOOK_URL,
            json={
                "embeds": [embed]
            },
            timeout=10,
        )

        if response.status_code == 429:

            retry_after = 2

            try:

                retry_after = float(
                    response.json()
                    .get(
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
                json={
                    "embeds": [embed]
                },
                timeout=10,
            )

        response.raise_for_status()

        print(
            f"ALERT {ob['symbol']} "
            f"{ob['direction']} "
            f"distance={distance_pct:.3f}%"
        )

        return True

    except Exception as error:

        print(
            f"Discord error for "
            f"{ob['symbol']}: {error}"
        )

        return False


def price_distance(
    ob,
    price
):

    if ob["direction"] == "bull":

        if price <= ob["zone_high"]:
            return 0.0

        return (
            (price - ob["zone_high"])
            / price
        ) * 100.0

    else:

        if price >= ob["zone_low"]:
            return 0.0

        return (
            (ob["zone_low"] - price)
            / price
        ) * 100.0


def process_price(
    symbol,
    price
):

    latest_prices[symbol] = price

    with locks[symbol]:

        remaining = []

        for ob in active_obs[symbol]:

            # Bullish block.
            if ob["direction"] == "bull":

                # Price broke completely below
                # the block.
                if price < ob["zone_low"]:

                    print(
                        f"INVALIDATED LIVE: "
                        f"{symbol} bullish OB"
                    )

                    continue

                # Price entered the block.
                if (
                    ob["zone_low"]
                    <= price
                    <= ob["zone_high"]
                ):

                    print(
                        f"MITIGATED LIVE: "
                        f"{symbol} bullish OB"
                    )

                    continue

            # Bearish block.
            else:

                # Price broke completely above
                # the block.
                if price > ob["zone_high"]:

                    print(
                        f"INVALIDATED LIVE: "
                        f"{symbol} bearish OB"
                    )

                    continue

                # Price entered the block.
                if (
                    ob["zone_low"]
                    <= price
                    <= ob["zone_high"]
                ):

                    print(
                        f"MITIGATED LIVE: "
                        f"{symbol} bearish OB"
                    )

                    continue

            distance = price_distance(
                ob,
                price
            )

            # 0.5% approach alert.
            if (
                distance
                <= APPROACH_PERCENT
            ):

                if ob["id"] not in alerted_ob_ids:

                    if send_discord(
                        ob,
                        price,
                        distance
                    ):

                        alerted_ob_ids.add(
                            ob["id"]
                        )

            remaining.append(ob)

        active_obs[symbol] = remaining


def process_completed_candle(
    symbol,
    candle
):

    history = candle_history[symbol]

    if (
        history
        and history[-1]["ts"]
        == candle["ts"]
    ):

        history[-1] = candle

    else:

        history.append(candle)

    if len(history) > HISTORY_CANDLES:

        del history[
            :-HISTORY_CANDLES
        ]

    candidates = detect_ob_candidates(
        history
    )

    with locks[symbol]:

        for ob in candidates:

            ob["symbol"] = symbol

            touched = False
            invalidated = False

            # Re-check every candle after
            # the OB formed.
            for index in range(
                ob["ob_index"] + 1,
                len(history)
            ):

                candle_check = history[index]

                # Invalidation.
                if (
                    ob["direction"] == "bull"
                    and candle_check["close"]
                    < ob["zone_low"]
                ):

                    invalidated = True
                    break

                if (
                    ob["direction"] == "bear"
                    and candle_check["close"]
                    > ob["zone_high"]
                ):

                    invalidated = True
                    break

                # Mitigation.
                if (
                    candle_check["low"]
                    <= ob["zone_high"]
                    and
                    candle_check["high"]
                    >= ob["zone_low"]
                ):

                    touched = True
                    break

            if touched or invalidated:
                continue

            if any(
                existing["id"] == ob["id"]
                for existing
                in active_obs[symbol]
            ):

                continue

            if (
                len(active_obs[symbol])
                >= MAX_ACTIVE_OBS_PER_SYMBOL
            ):

                active_obs[symbol].sort(
                    key=lambda x: x["ob_ts"],
                    reverse=True
                )

                active_obs[symbol] = active_obs[
                    symbol
                ][:MAX_ACTIVE_OBS_PER_SYMBOL - 1]

            active_obs[symbol].append(
                ob
            )

            seen_ob_ids.add(
                ob["id"]
            )

            print(
                f"NEW {ob['direction'].upper()} OB "
                f"{symbol} | "
                f"OB={fmt_ts(ob['ob_ts'])} | "
                f"BOS={fmt_ts(ob['bos_ts'])} | "
                f"Expansion="
                f"{ob['expansion'] / ob['atr']:.2f}x ATR"
            )


def make_subscription(group):

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


def websocket_worker(
    group,
    group_no
):

    subscription = make_subscription(
        group
    )

    while True:

        try:

            def on_open(ws):

                print(
                    f"WS group {group_no} "
                    f"connected "
                    f"({len(group)} symbols)"
                )

                ws.send(
                    subscription
                )

                print(
                    f"WS group {group_no}: "
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

                if (
                    payload.get("event")
                    == "error"
                ):

                    print(
                        f"WS group {group_no} "
                        f"ERROR: {payload}"
                    )

                    return

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

                if not symbol or not data:
                    return

                # Live ticker.
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

                # Completed 15M candle.
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
                                str(row[-1]),
                        }

                        # Only completed candles.
                        if (
                            candle["confirm"]
                            == "1"
                        ):

                            last = (
                                candle_history[
                                    symbol
                                ][-1]
                                if candle_history[
                                    symbol
                                ]
                                else None
                            )

                            if (
                                last is None
                                or
                                last["ts"]
                                != candle["ts"]
                            ):

                                process_completed_candle(
                                    symbol,
                                    candle
                                )

                    except Exception as error:

                        print(
                            f"Candle parse error "
                            f"{symbol}: {error}"
                        )

            def on_error(
                ws,
                error
            ):

                print(
                    f"WS group {group_no} "
                    f"error: {error}"
                )

            def on_close(
                ws,
                code,
                message
            ):

                print(
                    f"WS group {group_no} "
                    f"closed: "
                    f"{code} {message}"
                )

            ws = websocket.WebSocketApp(

                BLOFIN_WS,

                on_open=on_open,

                on_message=on_message,

                on_error=on_error,

                on_close=on_close,
            )

            ws.run_forever(
                ping_interval=20,
                ping_timeout=10,
            )

        except Exception as error:

            print(
                f"WS group {group_no} "
                f"crashed: {error}"
            )

        print(
            f"WS group {group_no} "
            f"reconnecting in 5 seconds..."
        )

        time.sleep(5)


def main():

    if not DISCORD_WEBHOOK_URL:

        print(
            "ERROR: "
            "ORDERBLOCK_DISCORD_WEBHOOK_URL "
            "is not set."
        )

        return

    print("=" * 70)

    print(
        "STRICT 15M ORDER BLOCK SCANNER"
    )

    print(
        "5-bar BOS | 1.5x ATR(14) | "
        "FULL HIGH-LOW OB"
    )

    print(
        "Historical + realtime | "
        "0.5% approach alert"
    )

    print("=" * 70)

    global symbols

    symbols = get_symbols()

    print(
        f"Found {len(symbols)} "
        f"live USDT perpetuals."
    )

    print(
        "Loading historical 15M candles "
        "and reconstructing fresh OBs..."
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

            active = historical_active_obs(
                symbol,
                candles
            )

            for ob in active:

                ob["symbol"] = symbol

                active_obs[
                    symbol
                ].append(ob)

                seen_ob_ids.add(
                    ob["id"]
                )

            if active:

                print(
                    f"[{number}/"
                    f"{len(symbols)}] "
                    f"{symbol}: "
                    f"{len(active)} "
                    f"fresh historical OB(s)"
                )

        except Exception as error:

            print(
                f"[{number}/"
                f"{len(symbols)}] "
                f"{symbol}: "
                f"history error: {error}"
            )

    total_active = sum(
        len(value)
        for value in active_obs.values()
    )

    print(
        f"Historical scan complete. "
        f"Fresh OBs loaded: "
        f"{total_active}"
    )

    # BloFin limits subscription message
    # length, so use groups of 25 symbols.
    groups = [

        symbols[i:i + WS_GROUP_SIZE]

        for i in range(
            0,
            len(symbols),
            WS_GROUP_SIZE
        )
    ]

    for index, group in enumerate(
        groups,
        1
    ):

        thread = threading.Thread(

            target=websocket_worker,

            args=(
                group,
                index
            ),

            daemon=True,
        )

        thread.start()

        time.sleep(1.2)

    print("=" * 70)

    print(
        "ORDER BLOCK SCANNER IS LIVE."
    )

    print(
        "Historical fresh OBs + "
        "new 15M OBs are being monitored."
    )

    print(
        "Alert threshold: 0.50%"
    )

    print("=" * 70)

    while True:

        time.sleep(60)


if __name__ == "__main__":

    main()
