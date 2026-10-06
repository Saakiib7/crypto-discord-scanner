import os
import time
import requests
from datetime import datetime, timezone


# ============================================================
# SETTINGS
# ============================================================

BLOFIN_BASE = "https://openapi.blofin.com"

DISCORD_WEBHOOK = os.environ.get("DISCORD_WEBHOOK_URL")

FAST_EMA = 9
SLOW_EMA = 16

VOLUME_LOOKBACK = 20
VOLUME_MULTIPLIER = 2.0

# Conservative rate to avoid BloFin 429 errors
MAX_REQUESTS_PER_MINUTE = 150
REQUEST_INTERVAL = 60.0 / MAX_REQUESTS_PER_MINUTE

# Number of candles requested for EMA calculation
CANDLE_LIMIT = 100

# Only accept signals from a recently closed 15m candle
MAX_SIGNAL_AGE_MINUTES = 20

# Discord message limit safety
DISCORD_MAX_LENGTH = 1900


# ============================================================
# HTTP SESSION + RATE LIMITER
# ============================================================

session = requests.Session()

last_request_time = 0.0


def wait_for_rate_limit():
    global last_request_time

    now = time.monotonic()
    elapsed = now - last_request_time

    if elapsed < REQUEST_INTERVAL:
        time.sleep(REQUEST_INTERVAL - elapsed)

    last_request_time = time.monotonic()


def get_json(path, params=None, retries=2):

    url = BLOFIN_BASE + path

    for attempt in range(retries + 1):

        wait_for_rate_limit()

        try:
            response = session.get(
                url,
                params=params,
                timeout=15
            )

            if response.status_code == 200:
                data = response.json()

                if str(data.get("code", "0")) != "0":
                    print(
                        f"BloFin API error: "
                        f"{data.get('msg', 'Unknown error')}"
                    )
                    return None

                return data

            # Rate limit
            if response.status_code == 429:

                if attempt < retries:
                    print(
                        "BloFin rate limit received. "
                        "Waiting 30 seconds..."
                    )
                    time.sleep(30)
                    continue

                print("BloFin rate limit. Skipping request.")
                return None

            # Temporary server error
            if response.status_code >= 500:

                if attempt < retries:
                    print(
                        f"BloFin server error {response.status_code}. "
                        "Waiting 10 seconds..."
                    )
                    time.sleep(10)
                    continue

                return None

            print(
                f"BloFin HTTP error: "
                f"{response.status_code}"
            )

            return None

        except requests.RequestException as e:

            if attempt < retries:
                print(
                    f"Request error: {e}. "
                    "Retrying in 5 seconds..."
                )
                time.sleep(5)
                continue

            print(f"Request failed: {e}")
            return None

    return None


# ============================================================
# GET LIVE BLOFIN USDT PERPETUALS
# ============================================================

def get_symbols():

    data = get_json(
        "/api/v1/market/instruments",
        {
            "instType": "SWAP"
        }
    )

    if not data:
        return []

    instruments = data.get("data", [])

    symbols = []

    for item in instruments:

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

        inst_id = item.get("instId")

        if inst_id:
            symbols.append(inst_id)

    symbols = sorted(set(symbols))

    return symbols


# ============================================================
# GET 15M CANDLES
# ============================================================

def get_candles(symbol):

    data = get_json(
        "/api/v1/market/candles",
        {
            "instId": symbol,
            "bar": "15m",
            "limit": CANDLE_LIMIT
        }
    )

    if not data:
        return []

    candles = data.get("data", [])

    if not candles:
        return []

    # BloFin returns newest candle first.
    # Reverse so oldest -> newest.
    candles = list(reversed(candles))

    return candles


# ============================================================
# EMA CALCULATION
# ============================================================

def calculate_ema(values, period):

    if len(values) < period:
        return []

    multiplier = 2.0 / (period + 1)

    ema = []

    # Initial SMA
    initial_sma = sum(values[:period]) / period

    ema.append(initial_sma)

    previous = initial_sma

    for value in values[period:]:

        current = (
            (value - previous) * multiplier
            + previous
        )

        ema.append(current)

        previous = current

    return ema


# ============================================================
# CHECK IF CANDLE IS COMPLETED
# ============================================================

def is_completed(candle):

    if len(candle) < 9:
        return False

    return str(candle[8]) == "1"


# ============================================================
# CHECK ONE SYMBOL
# ============================================================

def check_symbol(symbol):

    candles = get_candles(symbol)

    if len(candles) < 30:
        return None

    # Only completed candles
    completed = [
        candle
        for candle in candles
        if is_completed(candle)
    ]

    if len(completed) < 25:
        return None

    # Latest completed candle
    current = completed[-1]

    # Candle before it
    previous = completed[-2]

    try:

        closes = [
            float(candle[4])
            for candle in completed
        ]

        current_volume = float(current[5])

        previous_volumes = [
            float(candle[5])
            for candle in completed[-21:-1]
        ]

        if len(previous_volumes) != 20:
            return None

    except (ValueError, TypeError, IndexError):

        return None

    # --------------------------------------------------------
    # EMA CURRENT
    # --------------------------------------------------------

    ema9_series = calculate_ema(
        closes,
        FAST_EMA
    )

    ema16_series = calculate_ema(
        closes,
        SLOW_EMA
    )

    if not ema9_series or not ema16_series:
        return None

    current_ema9 = ema9_series[-1]
    current_ema16 = ema16_series[-1]

    # --------------------------------------------------------
    # EMA PREVIOUS
    # --------------------------------------------------------

    previous_closes = closes[:-1]

    previous_ema9_series = calculate_ema(
        previous_closes,
        FAST_EMA
    )

    previous_ema16_series = calculate_ema(
        previous_closes,
        SLOW_EMA
    )

    if not previous_ema9_series or not previous_ema16_series:
        return None

    previous_ema9 = previous_ema9_series[-1]
    previous_ema16 = previous_ema16_series[-1]

    # --------------------------------------------------------
    # CROSSOVER
    # --------------------------------------------------------

    bullish_cross = (
        previous_ema9 <= previous_ema16
        and current_ema9 > current_ema16
    )

    bearish_cross = (
        previous_ema9 >= previous_ema16
        and current_ema9 < current_ema16
    )

    if not bullish_cross and not bearish_cross:
        return None

    # --------------------------------------------------------
    # VOLUME
    # --------------------------------------------------------

    average_volume = (
        sum(previous_volumes)
        / len(previous_volumes)
    )

    if average_volume <= 0:
        return None

    volume_ratio = (
        current_volume
        / average_volume
    )

    if volume_ratio <= VOLUME_MULTIPLIER:
        return None

    # --------------------------------------------------------
    # CANDLE TIME
    # --------------------------------------------------------

    try:
        candle_timestamp = int(current[0])
    except (ValueError, TypeError, IndexError):
        return None

    # BloFin timestamp is milliseconds.
    # candle_timestamp = candle OPEN time.
    candle_close_timestamp = (
        candle_timestamp
        + (15 * 60 * 1000)
    )

    now_timestamp = int(
        datetime.now(timezone.utc).timestamp()
        * 1000
    )

    # Age measured from the actual candle CLOSE time
    age_minutes = (
        now_timestamp
        - candle_close_timestamp
    ) / 60000

    # Candle must already be closed
    if age_minutes < 0:
        return None

    # Ignore very old signals
    if age_minutes > MAX_SIGNAL_AGE_MINUTES:
        return None

    direction = (
        "BULLISH"
        if bullish_cross
        else "BEARISH"
    )

    price = float(current[4])

    return {
        "symbol": symbol,
        "direction": direction,
        "price": price,
        "ema9": current_ema9,
        "ema16": current_ema16,
        "volume_ratio": volume_ratio,
        "candle_timestamp": candle_timestamp,
        "candle_close_timestamp": candle_close_timestamp,
        "age_minutes": age_minutes
    }


# ============================================================
# DISCORD
# ============================================================

def send_discord_message(message):

    if not DISCORD_WEBHOOK:
        print("ERROR: DISCORD_WEBHOOK_URL is missing.")
        return False

    try:

        response = session.post(
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
            return True

        if response.status_code == 429:

            try:
                retry_after = response.json().get(
                    "retry_after",
                    2
                )
            except Exception:
                retry_after = 2

            print(
                f"Discord rate limit. "
                f"Waiting {retry_after} seconds..."
            )

            time.sleep(float(retry_after))

            response = session.post(
                DISCORD_WEBHOOK,
                json={
                    "content": message,
                    "allowed_mentions": {
                        "parse": []
                    }
                },
                timeout=15
            )

            return response.status_code in (200, 204)

        print(
            f"Discord error: "
            f"{response.status_code}"
        )

        return False

    except requests.RequestException as e:

        print(f"Discord request failed: {e}")

        return False


def send_alerts(results):

    if not results:
        return

    results = sorted(
        results,
        key=lambda x: (
            x["direction"],
            -x["volume_ratio"]
        )
    )

    header = (
        "⚡ **BLOFIN 15M EMA 9/16 ALERT**\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
    )

    messages = []
    current_message = header

    for result in results:

        emoji = (
            "🟢"
            if result["direction"] == "BULLISH"
            else "🔴"
        )

        # Candle OPEN time
        candle_open_time = datetime.fromtimestamp(
            result["candle_timestamp"] / 1000,
            tz=timezone.utc
        ).strftime(
            "%Y-%m-%d %H:%M UTC"
        )

        # Candle CLOSE time
        candle_close_time = datetime.fromtimestamp(
            result["candle_close_timestamp"] / 1000,
            tz=timezone.utc
        ).strftime(
            "%Y-%m-%d %H:%M UTC"
        )

        block = (
            f"{emoji} **{result['direction']}** "
            f"`{result['symbol']}`\n"
            f"Price: `{result['price']:.8g}`\n"
            f"EMA 9: `{result['ema9']:.8g}`\n"
            f"EMA 16: `{result['ema16']:.8g}`\n"
            f"Volume: **{result['volume_ratio']:.2f}x**\n"
            f"Candle: `{candle_open_time} → "
            f"{candle_close_time}`\n"
            f"✅ Closed: `{candle_close_time}`\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
        )

        if (
            len(current_message)
            + len(block)
            > DISCORD_MAX_LENGTH
        ):

            messages.append(current_message)
            current_message = block

        else:

            current_message += block

    if current_message.strip():
        messages.append(current_message)

    for index, message in enumerate(messages):

        success = send_discord_message(message)

        if success:
            print(
                f"Discord alert sent "
                f"({index + 1}/{len(messages)})"
            )
        else:
            print(
                f"Failed to send Discord alert "
                f"({index + 1}/{len(messages)})"
            )

        if index < len(messages) - 1:
            time.sleep(1)


# ============================================================
# MAIN
# ============================================================

def main():

    start_time = time.time()

    print("=" * 60)
    print("BLOFIN 15M EMA 9/16 SCANNER")
    print("=" * 60)

    print(
        "Strategy: 15M EMA 9/16 crossover + "
        ">2x volume"
    )

    print(
        "Only completed 15M candles are used."
    )

    print(
        f"API rate: {MAX_REQUESTS_PER_MINUTE} "
        "requests/minute"
    )

    print()

    # --------------------------------------------------------
    # GET SYMBOLS
    # --------------------------------------------------------

    symbols = get_symbols()

    if not symbols:

        print("No live BloFin USDT perpetuals found.")
        return

    print(
        f"Found {len(symbols)} live USDT perpetuals."
    )

    print("Starting scan...")
    print()

    # --------------------------------------------------------
    # SCAN
    # --------------------------------------------------------

    results = []

    total = len(symbols)

    for index, symbol in enumerate(symbols, start=1):

        result = check_symbol(symbol)

        if result:
            results.append(result)

            print(
                f"🚨 SIGNAL: "
                f"{result['direction']} "
                f"{symbol} "
                f"{result['volume_ratio']:.2f}x"
            )

        if index % 50 == 0 or index == total:

            elapsed = time.time() - start_time

            print(
                f"Progress: {index}/{total} "
                f"({index / total * 100:.1f}%) "
                f"- {elapsed:.1f}s"
            )

    # --------------------------------------------------------
    # RESULTS
    # --------------------------------------------------------

    print()
    print(
        f"EMA setups found: {len(results)}"
    )

    if results:

        for result in results:

            print(
                f"{result['symbol']} | "
                f"{result['direction']} | "
                f"Volume {result['volume_ratio']:.2f}x"
            )

        send_alerts(results)

    else:

        print(
            "No recent 9/16 EMA crossovers "
            "with >2x volume."
        )

    elapsed = time.time() - start_time

    print()
    print(
        f"Total scan time: {elapsed:.1f} seconds"
    )

    print("=" * 60)


if __name__ == "__main__":
    main()
