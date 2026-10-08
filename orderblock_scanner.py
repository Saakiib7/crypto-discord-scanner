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
SWING_LEFT_RIGHT = 2
ATR_PERIOD = 14
MIN_EXPANSION_ATR = 1.5
APPROACH_PERCENT = 0.5
HISTORY_CANDLES = 500
MAX_ACTIVE_OBS_PER_SYMBOL = 12

WS_GROUP_SIZE = 25
WS_RECEIVE_TIMEOUT = 20
HEARTBEAT_SECONDS = 15
WS_RECONNECT_DELAY = 5
WS_CONNECT_STAGGER = 1.1

HISTORY = {}
ACTIVE_OBS = defaultdict(list)
CURRENT_PRICES = {}
LAST_COMPLETED_TS = {}
ALERTED_OB_IDS = set()
state_lock = threading.RLock()


def now_utc():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def fmt_ts(ts_ms):
    return datetime.fromtimestamp(
        int(ts_ms) / 1000,
        tz=timezone.utc
    ).strftime("%Y-%m-%d %H:%M UTC")


def safe_float(value):
    try:
        return float(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def candle_from_row(row):
    if not isinstance(row, list) or len(row) < 6:
        return None

    try:
        return {
            "ts": int(row[0]),
            "open": float(row[1]),
            "high": float(row[2]),
            "low": float(row[3]),
            "close": float(row[4]),
            "confirm": str(row[-1]),
        }
    except (TypeError, ValueError, IndexError):
        return None


def parse_candles(raw):
    candles = []

    for row in raw:
        candle = candle_from_row(row)

        if candle is not None:
            candles.append(candle)

    candles.sort(key=lambda x: x["ts"])

    return candles


def candle_direction(candle):
    if candle["close"] > candle["open"]:
        return "BULLISH"

    if candle["close"] < candle["open"]:
        return "BEARISH"

    return "DOJI"


def calculate_atr(candles, period=ATR_PERIOD):
    atr_values = [None] * len(candles)

    if len(candles) <= period:
        return atr_values

    true_ranges = []

    for i, candle in enumerate(candles):
        if i == 0:
            tr = candle["high"] - candle["low"]

        else:
            previous_close = candles[i - 1]["close"]

            tr = max(
                candle["high"] - candle["low"],
                abs(candle["high"] - previous_close),
                abs(candle["low"] - previous_close),
            )

        true_ranges.append(tr)

    initial_atr = sum(
        true_ranges[1:period + 1]
    ) / period

    atr_values[period] = initial_atr

    previous_atr = initial_atr

    for i in range(period + 1, len(candles)):
        previous_atr = (
            previous_atr * (period - 1)
            + true_ranges[i]
        ) / period

        atr_values[i] = previous_atr

    return atr_values


def is_swing_high(candles, index):
    if index < SWING_LEFT_RIGHT:
        return False

    if index + SWING_LEFT_RIGHT >= len(candles):
        return False

    pivot_high = candles[index]["high"]

    for i in range(
        index - SWING_LEFT_RIGHT,
        index
    ):
        if pivot_high <= candles[i]["high"]:
            return False

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

    if index + SWING_LEFT_RIGHT >= len(candles):
        return False

    pivot_low = candles[index]["low"]

    for i in range(
        index - SWING_LEFT_RIGHT,
        index
    ):
        if pivot_low >= candles[i]["low"]:
            return False

    for i in range(
        index + 1,
        index + SWING_LEFT_RIGHT + 1
    ):
        if pivot_low > candles[i]["low"]:
            return False

    return True


def find_last_swing_before_bos(
    candles,
    bos_index,
    direction
):
    latest_possible_swing = (
        bos_index - SWING_LEFT_RIGHT
    )

    for swing_index in range(
        latest_possible_swing,
        SWING_LEFT_RIGHT - 1,
        -1
    ):
        if direction == "BULLISH":
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


def find_impulse_start(
    candles,
    bos_index,
    direction
):
    start = bos_index

    while start - 1 >= 0:
        if (
            candle_direction(
                candles[start - 1]
            ) == direction
        ):
            start -= 1
        else:
            break

    return start


def find_order_block(
    candles,
    impulse_start,
    direction
):
    opposite = (
        "BEARISH"
        if direction == "BULLISH"
        else "BULLISH"
    )

    for index in range(
        impulse_start - 1,
        -1,
        -1
    ):
        if (
            candle_direction(
                candles[index]
            ) == opposite
        ):
            return index

    return None


def detect_order_blocks(
    symbol,
    candles
):
    minimum_length = (
        ATR_PERIOD
        + SWING_LEFT_RIGHT * 2
        + 5
    )

    if len(candles) < minimum_length:
        return []

    atr_values = calculate_atr(candles)

    candidates = []

    for bos_index in range(
        ATR_PERIOD + 5,
        len(candles)
    ):
        bos_candle = candles[bos_index]

        direction = candle_direction(
            bos_candle
        )

        if direction not in (
            "BULLISH",
            "BEARISH"
        ):
            continue

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
            if direction == "BULLISH"
            else candles[swing_index]["low"]
        )

        if direction == "BULLISH":
            if bos_candle["close"] <= swing_price:
                continue

        else:
            if bos_candle["close"] >= swing_price:
                continue

        impulse_start = find_impulse_start(
            candles,
            bos_index,
            direction
        )

        if impulse_start >= bos_index:
            continue

        ob_index = find_order_block(
            candles,
            impulse_start,
            direction
        )

        if ob_index is None:
            continue

        ob_candle = candles[ob_index]

        atr = atr_values[bos_index]

        if atr is None or atr <= 0:
            continue

        if direction == "BULLISH":
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

        expansion_atr = expansion / atr

        if expansion_atr < MIN_EXPANSION_ATR:
            continue

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
            "ob_high": ob_candle["high"],
            "ob_low": ob_candle["low"],
            "bos_ts": bos_candle["ts"],
            "bos_close": bos_candle["close"],
            "swing_price": swing_price,
            "atr": atr,
            "expansion": expansion,
            "expansion_atr": expansion_atr,
            "impulse_ts": candles[
                impulse_start
            ]["ts"],
        })

    unique = {}

    for ob in candidates:
        unique[ob["id"]] = ob

    return list(unique.values())


def historical_ob_status(
    ob,
    candles
):
    for index in range(
        ob["ob_index"] + 1,
        len(candles)
    ):
        candle = candles[index]

        if (
            ob["direction"] == "BULLISH"
            and candle["close"] < ob["ob_low"]
        ):
            return "invalidated"

        if (
            ob["direction"] == "BEARISH"
            and candle["close"] > ob["ob_high"]
        ):
            return "invalidated"

        wick_touches = (
            candle["low"] <= ob["ob_high"]
            and candle["high"] >= ob["ob_low"]
        )

        if wick_touches:
            return "mitigated"

    return "fresh"
    def get_symbols():
    url = f"{BLOFIN_REST}/api/v1/market/instruments"

    try:
        response = requests.get(
            url,
            params={"instType": "SWAP"},
            timeout=20,
        )

        response.raise_for_status()
        payload = response.json()

        result = []

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
                result.append(symbol)

        result = sorted(set(result))

        print()
        print("=" * 70)
        print(
            f"BLOFIN LIVE USDT PERPETUALS: "
            f"{len(result)}"
        )
        print("=" * 70)

        return result

    except Exception as e:
        print(f"SYMBOL ERROR: {e}")
        return []


def get_history(symbol):
    url = f"{BLOFIN_REST}/api/v1/market/candles"

    params = {
        "instId": symbol,
        "bar": TIMEFRAME,
        "limit": HISTORY_CANDLES,
        "confirm": "1",
    }

    max_attempts = 5

    for attempt in range(max_attempts):
        try:
            response = requests.get(
                url,
                params=params,
                timeout=20,
            )

            if response.status_code == 200:
                payload = response.json()

                if payload.get("code") not in (
                    None,
                    "0",
                    0
                ):
                    print(
                        f"[HISTORY ERROR] {symbol}: "
                        f"API code "
                        f"{payload.get('code')} "
                        f"{payload.get('msg', '')}"
                    )
                    return []

                candles = parse_candles(
                    payload.get("data", [])
                )

                return [
                    candle
                    for candle in candles
                    if candle["confirm"] == "1"
                ]

            if response.status_code == 429:
                retry_after = (
                    response.headers.get(
                        "Retry-After"
                    )
                )

                if retry_after:
                    try:
                        wait_time = float(
                            retry_after
                        )
                    except ValueError:
                        wait_time = 5.0
                else:
                    wait_time = min(
                        30.0,
                        5.0 * (2 ** attempt)
                    )

                print(
                    f"[RATE LIMIT] {symbol}: "
                    f"HTTP 429 -> waiting "
                    f"{wait_time:.1f}s "
                    f"(attempt "
                    f"{attempt + 1}/"
                    f"{max_attempts})"
                )

                time.sleep(wait_time)
                continue

            print(
                f"[HISTORY ERROR] {symbol}: "
                f"HTTP "
                f"{response.status_code}"
            )

        except requests.RequestException as e:
            wait_time = min(
                30.0,
                5.0 * (2 ** attempt)
            )

            print(
                f"[HISTORY ERROR] {symbol}: "
                f"{e} -> retrying in "
                f"{wait_time:.1f}s"
            )

            time.sleep(wait_time)

    print(
        f"[HISTORY FAILED] {symbol}: "
        f"all {max_attempts} attempts failed"
    )

    return []


def load_historical_obs(symbols):
    print()
    print("=" * 70)
    print("STARTING HISTORICAL 15M OB SCAN")
    print(
        f"Symbols: {len(symbols)}"
    )
    print(
        f"Candles per symbol: "
        f"{HISTORY_CANDLES}"
    )
    print(
        "Mode: sequential + "
        "0.5s REST pacing"
    )
    print("=" * 70)

    successful = 0
    completed = 0
    fresh_total = 0

    for symbol in symbols:
        completed += 1

        try:
            candles = get_history(symbol)

            if candles:
                successful += 1

                HISTORY[symbol] = candles

                LAST_COMPLETED_TS[symbol] = (
                    candles[-1]["ts"]
                )

                obs = detect_order_blocks(
                    symbol,
                    candles
                )

                obs.sort(
                    key=lambda x: x["ob_ts"],
                    reverse=True
                )

                fresh = []

                for ob in obs:
                    if (
                        historical_ob_status(
                            ob,
                            candles
                        )
                        == "fresh"
                    ):
                        fresh.append(ob)

                    if (
                        len(fresh)
                        >= MAX_ACTIVE_OBS_PER_SYMBOL
                    ):
                        break

                with state_lock:
                    ACTIVE_OBS[symbol] = fresh

                fresh_total += len(fresh)

        except Exception as e:
            print(
                f"HISTORICAL ERROR "
                f"{symbol}: {e}"
            )

        if (
            completed % 25 == 0
            or completed == len(symbols)
        ):
            print(
                f"Historical scan: "
                f"{completed}/"
                f"{len(symbols)} | "
                f"successful: "
                f"{successful} | "
                f"fresh OBs: "
                f"{fresh_total}"
            )

        if completed < len(symbols):
            time.sleep(0.5)

    print()
    print("=" * 70)
    print("HISTORICAL SCAN COMPLETE")
    print(
        f"Symbols processed: "
        f"{completed}/"
        f"{len(symbols)}"
    )
    print(
        f"Successful histories: "
        f"{successful}/"
        f"{len(symbols)}"
    )
    print(
        f"Fresh historical OBs: "
        f"{fresh_total}"
    )
    print("=" * 70)


def distance_to_ob(price, ob):
    high = ob["ob_high"]
    low = ob["ob_low"]

    if low <= price <= high:
        return 0.0

    if price > high:
        return (
            (price - high)
            / price
            * 100
        )

    return (
        (low - price)
        / price
        * 100
    )


def send_discord(
    ob,
    price,
    reason="APPROACH"
):
    if not DISCORD_WEBHOOK_URL:
        print(
            "DISCORD WEBHOOK NOT SET"
        )
        return False

    distance = distance_to_ob(
        price,
        ob
    )

    title = (
        "🟢 15M BULLISH ORDER BLOCK"
        if ob["direction"] == "BULLISH"
        else
        "🔴 15M BEARISH ORDER BLOCK"
    )

    embed = {
        "title": title,
        "description": (
            f"**{ob['symbol']}**\n"
            f"Price is within "
            f"**{APPROACH_PERCENT:.2f}%** "
            f"of a fresh 15M "
            f"Order Block."
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
                    f"{ob['ob_low']:.12g}"
                    f" → "
                    f"{ob['ob_high']:.12g}"
                ),
                "inline": False,
            },
            {
                "name": "OB Candle",
                "value": fmt_ts(
                    ob["ob_ts"]
                ),
                "inline": True,
            },
            {
                "name": "BOS Candle",
                "value": fmt_ts(
                    ob["bos_ts"]
                ),
                "inline": True,
            },
            {
                "name": "Impulse Start",
                "value": fmt_ts(
                    ob["impulse_ts"]
                ),
                "inline": True,
            },
            {
                "name": "ATR(14)",
                "value": (
                    f"{ob['atr']:.8g}"
                ),
                "inline": True,
            },
            {
                "name": "Expansion",
                "value": (
                    f"{ob['expansion']:.8g} "
                    f"({ob['expansion_atr']:.2f} "
                    f"ATR)"
                ),
                "inline": True,
            },
            {
                "name": "Rules",
                "value": (
                    "5-bar BOS • Body close • "
                    "≥1.5 ATR expansion • "
                    "Full candle OB"
                ),
                "inline": False,
            },
        ],
        "footer": {
            "text":
                "BloFin 15M "
                "Order Block Scanner"
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

        if response.status_code in (
            200,
            204
        ):
            print(
                f"DISCORD ALERT SENT | "
                f"{ob['symbol']} | "
                f"{ob['direction']} | "
                f"{distance:.3f}%"
            )

            return True

        print(
            f"DISCORD ERROR: HTTP "
            f"{response.status_code} "
            f"{response.text[:300]}"
        )

    except Exception as e:
        print(
            f"DISCORD EXCEPTION: {e}"
        )

    return False


def process_price(
    symbol,
    price
):
    if price is None:
        return

    with state_lock:
        CURRENT_PRICES[symbol] = price
        obs = list(
            ACTIVE_OBS.get(
                symbol,
                []
            )
        )

    for ob in obs:
        distance = distance_to_ob(
            price,
            ob
        )

        if distance > APPROACH_PERCENT:
            continue

        ob_id = ob["id"]

        with state_lock:
            if ob_id in ALERTED_OB_IDS:
                continue

            ALERTED_OB_IDS.add(ob_id)

        success = send_discord(
            ob,
            price,
            "PRICE WITHIN 0.50%"
        )

        if not success:
            with state_lock:
                ALERTED_OB_IDS.discard(
                    ob_id
                )


def process_completed_candle(
    symbol,
    candle
):
    if candle.get("confirm") != "1":
        return

    ts = candle["ts"]

    with state_lock:
        previous_ts = (
            LAST_COMPLETED_TS.get(
                symbol
            )
        )

        if (
            previous_ts is not None
            and ts <= previous_ts
        ):
            return

        LAST_COMPLETED_TS[symbol] = ts

        candles = HISTORY.setdefault(
            symbol,
            []
        )

        if (
            candles
            and candles[-1]["ts"] == ts
        ):
            candles[-1] = candle

        else:
            candles.append(candle)

        if len(candles) > HISTORY_CANDLES:
            del candles[
                :-HISTORY_CANDLES
            ]

        existing_obs = list(
            ACTIVE_OBS.get(
                symbol,
                []
            )
        )

    remaining = []

    for ob in existing_obs:
        invalidated = False
        mitigated = False

        if (
            ob["direction"]
            == "BULLISH"
            and candle["close"]
            < ob["ob_low"]
        ):
            invalidated = True

        if (
            ob["direction"]
            == "BEARISH"
            and candle["close"]
            > ob["ob_high"]
        ):
            invalidated = True

        if (
            candle["ts"] > ob["ob_ts"]
            and candle["high"]
            >= ob["ob_low"]
            and candle["low"]
            <= ob["ob_high"]
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

    with state_lock:
        candles_snapshot = list(
            HISTORY.get(
                symbol,
                []
            )
        )

    candidates = detect_order_blocks(
        symbol,
        candles_snapshot
    )

    existing_ids = {
        ob["id"]
        for ob in remaining
    }

    new_obs = []

    candidates.sort(
        key=lambda x: x["ob_ts"],
        reverse=True
    )

    for ob in candidates:
        if ob["id"] in existing_ids:
            continue

        if ob["ob_ts"] >= candle["ts"]:
            continue

        if (
            historical_ob_status(
                ob,
                candles_snapshot
            )
            != "fresh"
        ):
            continue

        new_obs.append(ob)
        existing_ids.add(ob["id"])

        print(
            f"NEW {ob['direction']} OB | "
            f"{symbol} | Expansion "
            f"{ob['expansion_atr']:.2f} ATR"
        )

    combined = remaining + new_obs

    combined.sort(
        key=lambda x: x["ob_ts"],
        reverse=True
    )

    combined = combined[
        :MAX_ACTIVE_OBS_PER_SYMBOL
    ]

    with state_lock:
        ACTIVE_OBS[symbol] = combined
        def handle_ws_message(message, subscribed_symbols):
    try:
        if message == "pong":
            return

        payload = json.loads(message)

    except Exception:
        return

    if not isinstance(payload, dict):
        return

    event = payload.get("event")

    if event == "error":
        print(
            f"WS SUBSCRIPTION ERROR: "
            f"{payload.get('code')} "
            f"{payload.get('msg')}"
        )
        return

    if event == "subscribe":
        return

    arg = payload.get("arg", {})

    channel = arg.get("channel")
    symbol = arg.get("instId")

    if not symbol or symbol not in subscribed_symbols:
        return

    data = payload.get("data")

    if not data:
        return

    if channel == "tickers":
        try:
            ticker = data[0]

            if isinstance(ticker, dict):
                process_price(
                    symbol,
                    safe_float(
                        ticker.get("last")
                    )
                )

        except Exception as e:
            print(
                f"TICKER ERROR "
                f"{symbol}: {e}"
            )

        return

    if channel == "candle15m":
        try:
            candle = candle_from_row(
                data[0]
            )

            if (
                candle is not None
                and candle["confirm"] == "1"
            ):
                process_completed_candle(
                    symbol,
                    candle
                )

        except Exception as e:
            print(
                f"CANDLE ERROR "
                f"{symbol}: {e}"
            )


def websocket_worker(
    group_number,
    group_symbols
):
    channels = []

    for symbol in group_symbols:
        channels.append({
            "channel": "candle15m",
            "instId": symbol,
        })

        channels.append({
            "channel": "tickers",
            "instId": symbol,
        })

    subscribed_symbols = set(
        group_symbols
    )

    while True:
        ws = None

        try:
            print(
                f"WS {group_number}: "
                f"connecting "
                f"({len(group_symbols)} symbols)"
            )

            ws = websocket.create_connection(
                BLOFIN_WS,
                timeout=WS_RECEIVE_TIMEOUT,
                enable_multithread=True,
            )

            print(
                f"WS {group_number}: "
                f"connected"
            )

            ws.send(
                json.dumps(
                    {
                        "op": "subscribe",
                        "args": channels,
                    },
                    separators=(",", ":"),
                )
            )

            print(
                f"WS {group_number}: "
                f"subscription sent "
                f"({len(channels)} channels)"
            )

            last_message_time = time.time()

            while True:
                try:
                    message = ws.recv()

                    if message:
                        last_message_time = (
                            time.time()
                        )

                        handle_ws_message(
                            message,
                            subscribed_symbols
                        )

                except websocket.WebSocketTimeoutException:
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

                            last_message_time = (
                                time.time()
                            )

                        except Exception as e:
                            print(
                                f"WS {group_number}: "
                                f"heartbeat failed: "
                                f"{e}"
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
            f"{WS_RECONNECT_DELAY} "
            f"seconds..."
        )

        time.sleep(
            WS_RECONNECT_DELAY
        )


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
            WS_GROUP_SIZE
        )
    ]

    print(
        f"WebSocket groups: "
        f"{len(groups)}"
    )

    print(
        f"Symbols per group: "
        f"{WS_GROUP_SIZE}"
    )

    for number, group in enumerate(
        groups,
        start=1
    ):
        print(
            f"Starting WS "
            f"{number}/{len(groups)} "
            f"({len(group)} symbols)"
        )

        thread = threading.Thread(
            target=websocket_worker,
            args=(number, group),
            daemon=True,
        )

        thread.start()

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
        "Mitigation: completed "
        "15M candle wick"
    )

    print(
        "Invalidation: completed "
        "15M candle close"
    )

    print(
        "EMA/VWAP/volume filters: NONE"
    )

    print(
        f"Live since: {now_utc()}"
    )

    print("=" * 70)


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


def main():
    print("=" * 70)
