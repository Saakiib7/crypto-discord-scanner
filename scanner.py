import os
import time
import threading
import requests
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed


# ============================================================
# SETTINGS
# ============================================================

BLOFIN_BASE = "https://openapi.blofin.com"

DISCORD_WEBHOOK = os.environ.get(
    "DISCORD_WEBHOOK_URL"
)

FAST_EMA = 9
SLOW_EMA = 16

VOLUME_LOOKBACK = 20
VOLUME_MULTIPLIER = 2.0

MAX_WORKERS = 15

# BloFin public REST rate limit
# Stay safely below 500 requests/minute.
MAX_REQUESTS_PER_MINUTE = 450

REQUEST_INTERVAL = (
    60.0 / MAX_REQUESTS_PER_MINUTE
)


# ============================================================
# RATE LIMITER
# ============================================================

_last_request_time = 0.0

_rate_lock = threading.Lock()


def throttle():

    global _last_request_time

    with _rate_lock:

        now = time.monotonic()

        wait = (
            REQUEST_INTERVAL
            - (now - _last_request_time)
        )

        if wait > 0:

            time.sleep(wait)

        _last_request_time = (
            time.monotonic()
        )


# ============================================================
# THREAD LOCAL SESSION
# ============================================================

_thread_local = threading.local()


def get_session():

    if not hasattr(
        _thread_local,
        "session"
    ):

        session = requests.Session()

        session.headers.update({
            "User-Agent":
                "Crypto-Discord-EMA-Scanner/5.0"
        })

        _thread_local.session = session

    return _thread_local.session


# ============================================================
# BLOFIN API REQUEST
# ============================================================

def get_json(
    path,
    params=None,
    retries=4
):

    url = (
        f"{BLOFIN_BASE}{path}"
    )

    for attempt in range(retries):

        try:

            throttle()

            response = get_session().get(
                url,
                params=params,
                timeout=15
            )

            if response.status_code == 429:

                print(
                    "BloFin rate limit. "
                    "Waiting 10 seconds..."
                )

                time.sleep(10)

                continue

            if response.status_code == 403:

                print(
                    "BloFin HTTP 403. "
                    "Waiting 15 seconds..."
                )

                time.sleep(15)

                continue

            response.raise_for_status()

            data = response.json()

            if str(
                data.get("code")
            ) != "0":

                raise RuntimeError(
                    f"BloFin API error "
                    f"{data.get('code')}: "
                    f"{data.get('msg')}"
                )

            return data

        except Exception as e:

            if (
                attempt
                == retries - 1
            ):

                raise

            print(
                f"Request error: {e}. "
                f"Retrying..."
            )

            time.sleep(
                2 * (attempt + 1)
            )

    return None


# ============================================================
# GET LIVE USDT PERPETUALS
# ============================================================

def get_symbols():

    data = get_json(
        "/api/v1/market/instruments"
    )

    symbols = []

    for item in data["data"]:

        if (
            item.get("state")
            == "live"

            and item.get("instType")
            == "SWAP"

            and item.get("contractType")
            == "linear"

            and item.get("settleCurrency")
            == "USDT"
        ):

            inst_id = item.get(
                "instId"
            )

            if inst_id:

                symbols.append(
                    inst_id
                )

    return sorted(
        set(symbols)
    )


# ============================================================
# GET 15M CANDLES
# ============================================================

def get_candles(
    symbol,
    limit=100
):

    data = get_json(
        "/api/v1/market/candles",
        {
            "instId": symbol,
            "bar": "15m",
            "limit": str(limit)
        }
    )

    candles = data["data"]

    # BloFin returns newest first.
    # Convert to oldest -> newest.
    candles = list(
        reversed(candles)
    )

    return candles


# ============================================================
# EMA
# ============================================================

def calculate_ema(
    values,
    period
):

    if len(values) < period:

        return None

    multiplier = (
        2.0 / (period + 1)
    )

    # Initial SMA
    ema = (
        sum(values[:period])
        / period
    )

    for price in values[period:]:

        ema = (
            (price - ema)
            * multiplier
            + ema
        )

    return ema


# ============================================================
# CHECK WHETHER CANDLE IS COMPLETED
# ============================================================

def is_completed(candle):

    # BloFin candle structure:
    #
    # 0 = timestamp
    # 1 = open
    # 2 = high
    # 3 = low
    # 4 = close
    # 5 = volume
    # 6 = base volume
    # 7 = quote volume
    # 8 = confirm
    #
    # confirm = 1 means completed.

    return (
        len(candle) >= 9
        and str(candle[8]) == "1"
    )


# ============================================================
# CHECK ONE SYMBOL
# ============================================================

def check_symbol(symbol):

    try:

        candles = get_candles(
            symbol,
            100
        )

        if len(candles) < 30:

            return None

        # ----------------------------------------------------
        # USE ONLY COMPLETED 15M CANDLES
        # ----------------------------------------------------

        completed = [

            candle

            for candle
            in candles

            if is_completed(candle)

        ]

        if len(completed) < 25:

            return None

        # ----------------------------------------------------
        # LATEST CLOSED CANDLE
        # ----------------------------------------------------

        current = completed[-1]

        previous = completed[-2]

        current_close = float(
            current[4]
        )

        previous_close = float(
            previous[4]
        )

        # ----------------------------------------------------
        # 9 EMA
        # ----------------------------------------------------

        closes = [

            float(candle[4])

            for candle
            in completed

        ]

        ema9 = calculate_ema(
            closes,
            FAST_EMA
        )

        ema16 = calculate_ema(
            closes,
            SLOW_EMA
        )

        previous_closes = closes[:-1]

        previous_ema9 = calculate_ema(
            previous_closes,
            FAST_EMA
        )

        previous_ema16 = calculate_ema(
            previous_closes,
            SLOW_EMA
        )

        if (
            ema9 is None
            or ema16 is None
            or previous_ema9 is None
            or previous_ema16 is None
        ):

            return None

        # ----------------------------------------------------
        # CROSSOVER
        # ----------------------------------------------------

        bullish_cross = (

            previous_ema9
            <= previous_ema16

            and

            ema9
            > ema16

        )

        bearish_cross = (

            previous_ema9
            >= previous_ema16

            and

            ema9
            < ema16

        )

        if not (
            bullish_cross
            or bearish_cross
        ):

            return None

        # ----------------------------------------------------
        # VOLUME
        # ----------------------------------------------------

        # Previous 20 completed candles.
        # Do NOT include the signal candle.

        volume_candles = completed[
            -21:-1
        ]

        if len(
            volume_candles
        ) != VOLUME_LOOKBACK:

            return None

        previous_volumes = [

            float(candle[5])

            for candle
            in volume_candles

        ]

        current_volume = float(
            current[5]
        )

        average_volume = (
            sum(previous_volumes)
            / VOLUME_LOOKBACK
        )

        if average_volume <= 0:

            return None

        volume_ratio = (
            current_volume
            / average_volume
        )

        # Must be STRICTLY greater than 2x.
        if (
            volume_ratio
            <= VOLUME_MULTIPLIER
        ):

            return None

        # ----------------------------------------------------
        # CANDLE TIME
        # ----------------------------------------------------

        candle_timestamp = int(
            current[0]
        )

        candle_time = (
            datetime.fromtimestamp(
                candle_timestamp / 1000,
                timezone.utc
            )
        )

        return {

            "symbol":
                symbol,

            "direction":
                (
                    "BULLISH"
                    if bullish_cross
                    else "BEARISH"
                ),

            "price":
                current_close,

            "ema9":
                ema9,

            "ema16":
                ema16,

            "volume_ratio":
                volume_ratio,

            "candle_time":
                candle_time.strftime(
                    "%Y-%m-%d %H:%M UTC"
                ),

            "candle_timestamp":
                candle_timestamp

        }

    except Exception as e:

        print(
            f"Error checking "
            f"{symbol}: {e}"
        )

        return None


# ============================================================
# CHECK WHETHER SIGNAL CANDLE IS RECENT
# ============================================================

def is_recent_closed_candle(
    result
):

    candle_timestamp = (
        result["candle_timestamp"]
    )

    now = time.time()

    age = (
        now
        - candle_timestamp / 1000
    )

    # Only alert for a recently closed candle.
    #
    # This prevents the same crossover from
    # triggering again on the next 5-minute scan.
    #
    # Maximum 9 minutes old.

    return (
        0
        <= age
        < 9 * 60
    )


# ============================================================
# DISCORD MESSAGE SPLITTER
# ============================================================

def split_message(
    message,
    max_len=1900
):

    chunks = []

    while len(message) > max_len:

        split_at = message.rfind(
            "\n\n",
            0,
            max_len
        )

        if split_at == -1:

            split_at = message.rfind(
                "\n",
                0,
                max_len
            )

        if split_at == -1:

            split_at = max_len

        chunks.append(
            message[:split_at]
        )

        message = (
            message[split_at:]
            .lstrip()
        )

    if message:

        chunks.append(
            message
        )

    return chunks


# ============================================================
# SEND DISCORD ALERT
# ============================================================

def send_discord_alert(
    results
):

    if not DISCORD_WEBHOOK:

        raise RuntimeError(
            "DISCORD_WEBHOOK_URL "
            "is missing."
        )

    lines = [

        "⚡ **BLOFIN 9/16 EMA ALERT**",

        f"**{len(results)} setup(s)**",

        ""
    ]

    for result in results:

        if (
            result["direction"]
            == "BULLISH"
        ):

            icon = "🟢"

            cross_text = (
                "9 EMA crossed "
                "**ABOVE** 16 EMA"
            )

        else:

            icon = "🔴"

            cross_text = (
                "9 EMA crossed "
                "**BELOW** 16 EMA"
            )

        lines += [

            f"{icon} **{result['symbol']}**",

            f"📍 Direction: "
            f"**{result['direction']}**",

            f"💰 Price: "
            f"`{result['price']:.8g}`",

            f"9 EMA: "
            f"`{result['ema9']:.8g}`",

            f"16 EMA: "
            f"`{result['ema16']:.8g}`",

            f"🔥 15M Volume: "
            f"`{result['volume_ratio']:.2f}x`",

            f"📈 {cross_text}",

            f"🕐 Candle closed: "
            f"`{result['candle_time']}`",

            ""
        ]

    message = "\n".join(
        lines
    )

    chunks = split_message(
        message
    )

    for chunk in chunks:

        for attempt in range(4):

            response = (
                get_session().post(
                    DISCORD_WEBHOOK,
                    json={
                        "content": chunk
                    },
                    timeout=15
                )
            )

            if (
                response.status_code
                != 429
            ):

                response.raise_for_status()

                break

            try:

                retry_after = float(
                    response.json().get(
                        "retry_after",
                        2
                    )
                )

            except Exception:

                retry_after = 2

            print(
                "Discord rate limit. "
                f"Waiting "
                f"{retry_after:.2f}s..."
            )

            time.sleep(
                retry_after + 1
            )

        time.sleep(0.75)


# ============================================================
# MAIN
# ============================================================

def main():

    start_time = time.time()

    print("=" * 60)

    print(
        "BLOFIN 15M 9 EMA x 16 EMA "
        "+ VOLUME SCANNER"
    )

    print("=" * 60)

    if not DISCORD_WEBHOOK:

        raise RuntimeError(
            "DISCORD_WEBHOOK_URL "
            "is missing."
        )

    # --------------------------------------------------------
    # GET SYMBOLS
    # --------------------------------------------------------

    print(
        "Getting BloFin USDT "
        "perpetual symbols..."
    )

    symbols = get_symbols()

    print(
        f"Found {len(symbols)} "
        "USDT perpetual symbols."
    )

    results = []

    completed_count = 0

    # --------------------------------------------------------
    # SCAN
    # --------------------------------------------------------

    with ThreadPoolExecutor(
        max_workers=MAX_WORKERS
    ) as executor:

        futures = {

            executor.submit(
                check_symbol,
                symbol
            ): symbol

            for symbol in symbols

        }

        for future in as_completed(
            futures
        ):

            symbol = futures[
                future
            ]

            try:

                result = (
                    future.result()
                )

                if result:

                    # Only recent closed
                    # candle signals.

                    if is_recent_closed_candle(
                        result
                    ):

                        results.append(
                            result
                        )

            except Exception as e:

                print(
                    f"Worker error "
                    f"{symbol}: {e}"
                )

            completed_count += 1

            if (
                completed_count % 50
                == 0
            ):

                print(
                    f"Progress: "
                    f"{completed_count}/"
                    f"{len(symbols)}"
                )

    # --------------------------------------------------------
    # REMOVE DUPLICATES
    # --------------------------------------------------------

    unique = {}

    for result in results:

        key = (
            result["symbol"],
            result["candle_timestamp"],
            result["direction"]
        )

        unique[key] = result

    results = list(
        unique.values()
    )

    results.sort(
        key=lambda x:
            x["symbol"]
    )

    # --------------------------------------------------------
    # RESULTS
    # --------------------------------------------------------

    print()

    print(
        f"EMA setups found: "
        f"{len(results)}"
    )

    # --------------------------------------------------------
    # DISCORD
    # --------------------------------------------------------

    if results:

        send_discord_alert(
            results
        )

        for result in results:

            print(
                f"ALERT: "
                f"{result['symbol']} | "
                f"{result['direction']} | "
                f"{result['volume_ratio']:.2f}x"
            )

    else:

        print(
            "No recent 9/16 EMA "
            "crossovers with >2x volume."
        )

    # --------------------------------------------------------
    # TIME
    # --------------------------------------------------------

    elapsed = (
        time.time()
        - start_time
    )

    print()

    print(
        f"Total scan time: "
        f"{elapsed:.1f} seconds"
    )

    print("=" * 60)


# ============================================================
# START
# ============================================================

if __name__ == "__main__":

    main()
