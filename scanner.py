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

EMA_PERIOD = 200

VOLUME_LOOKBACK = 20

VOLUME_MULTIPLIER = 2.0

MAX_WORKERS = 15

# BloFin REST limit is 500 requests/minute.
# Stay safely below it.
MAX_REQUESTS_PER_MINUTE = 450

REQUEST_INTERVAL = (
    60.0 / MAX_REQUESTS_PER_MINUTE
)


# ============================================================
# GLOBAL RATE LIMITER
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
                "Crypto-Discord-Scanner/4.0"
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

            # BloFin firewall / rate limit
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

            if str(data.get("code")) != "0":

                raise RuntimeError(
                    f"BloFin API error "
                    f"{data.get('code')}: "
                    f"{data.get('msg')}"
                )

            return data

        except Exception as e:

            if attempt == retries - 1:

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
# GET CANDLES
# ============================================================

def get_candles(
    symbol,
    timeframe,
    limit
):

    data = get_json(
        "/api/v1/market/candles",
        {
            "instId": symbol,
            "bar": timeframe,
            "limit": str(limit)
        }
    )

    candles = data["data"]

    # BloFin returns newest first.
    # Convert to oldest -> newest.
    return list(
        reversed(candles)
    )


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
# DAILY VWAP
# ============================================================

def calculate_daily_vwap(
    candles
):

    today = datetime.now(
        timezone.utc
    ).date()

    cumulative_pv = 0.0

    cumulative_volume = 0.0

    for candle in candles:

        candle_time = (
            datetime.fromtimestamp(
                int(candle[0]) / 1000,
                timezone.utc
            )
        )

        if (
            candle_time.date()
            != today
        ):

            continue

        high = float(candle[2])

        low = float(candle[3])

        close = float(candle[4])

        volume = float(candle[5])

        typical_price = (
            high
            + low
            + close
        ) / 3.0

        cumulative_pv += (
            typical_price
            * volume
        )

        cumulative_volume += (
            volume
        )

    if cumulative_volume <= 0:

        return None

    return (
        cumulative_pv
        / cumulative_volume
    )


# ============================================================
# CHECK ONE SYMBOL
# ============================================================

def check_symbol(symbol):

    try:

        # ----------------------------------------------------
        # 15M DATA
        # ----------------------------------------------------

        candles = get_candles(
            symbol,
            "15m",
            100
        )

        if len(candles) < 30:

            return None

        # Ignore current incomplete candle
        completed = candles[:-1]

        if len(completed) < 22:

            return None

        current = completed[-1]

        previous = completed[-2]

        current_close = float(
            current[4]
        )

        previous_close = float(
            previous[4]
        )

        current_volume = float(
            current[5]
        )

        # ----------------------------------------------------
        # VOLUME SURGE
        # ----------------------------------------------------

        previous_volumes = [

            float(candle[5])

            for candle
            in completed[-21:-1]

        ]

        if len(
            previous_volumes
        ) != VOLUME_LOOKBACK:

            return None

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

        if (
            volume_ratio
            <= VOLUME_MULTIPLIER
        ):

            return None

        # ----------------------------------------------------
        # DAILY VWAP
        # ----------------------------------------------------

        current_vwap = (
            calculate_daily_vwap(
                completed
            )
        )

        previous_vwap = (
            calculate_daily_vwap(
                completed[:-1]
            )
        )

        if (
            current_vwap is None
            or previous_vwap is None
        ):

            return None

        above_vwap = (
            current_close
            > current_vwap
        )

        previous_above_vwap = (
            previous_close
            > previous_vwap
        )

        if not above_vwap:

            return None

        crossed_vwap = (
            not previous_above_vwap
            and above_vwap
        )

        # ----------------------------------------------------
        # ONLY NOW REQUEST 1H DATA
        # ----------------------------------------------------

        candles_1h = get_candles(
            symbol,
            "1H",
            210
        )

        if len(candles_1h) < 202:

            return None

        completed_1h = (
            candles_1h[:-1]
        )

        closes_1h = [

            float(candle[4])

            for candle
            in completed_1h

        ]

        ema200 = calculate_ema(
            closes_1h,
            EMA_PERIOD
        )

        ema200_previous = (
            calculate_ema(
                closes_1h[:-1],
                EMA_PERIOD
            )
        )

        if (
            ema200 is None
            or ema200_previous is None
        ):

            return None

        above_ema = (
            current_close
            > ema200
        )

        if not above_ema:

            return None

        previous_above_ema = (
            previous_close
            > ema200_previous
        )

        crossed_ema = (
            not previous_above_ema
            and above_ema
        )

        # ----------------------------------------------------
        # FINAL SIGNAL
        # ----------------------------------------------------

        if not (
            crossed_vwap
            or crossed_ema
        ):

            return None

        return {

            "symbol":
                symbol,

            "price":
                current_close,

            "vwap":
                current_vwap,

            "ema200":
                ema200,

            "volume_ratio":
                volume_ratio,

            "crossed_vwap":
                crossed_vwap,

            "crossed_ema":
                crossed_ema
        }

    except Exception as e:

        print(
            f"Error checking "
            f"{symbol}: {e}"
        )

        return None


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
# DISCORD ALERT
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

        "🚨 **BLOFIN CRYPTO ALERTS** 🚨",

        f"**{len(results)} coins "
        f"meet ALL 3 conditions**",

        ""
    ]

    for result in results:

        crossed = []

        if result[
            "crossed_vwap"
        ]:

            crossed.append(
                "VWAP"
            )

        if result[
            "crossed_ema"
        ]:

            crossed.append(
                "1H EMA 200"
            )

        lines += [

            f"🔔 **{result['symbol']}**",

            f"💰 Price: "
            f"`{result['price']:.8g}`",

            f"📊 Daily VWAP: "
            f"`{result['vwap']:.8g}`",

            f"📈 1H EMA 200: "
            f"`{result['ema200']:.8g}`",

            f"🔥 15M Volume: "
            f"`{result['volume_ratio']:.2f}x`",

            "✅ VWAP | "
            "✅ EMA 200 | "
            "✅ Volume >2x",

            f"📍 Crossed: "
            f"**{' + '.join(crossed)}**",

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
        "BLOFIN VWAP + EMA200 + "
        "VOLUME SCANNER"
    )

    print("=" * 60)

    if not DISCORD_WEBHOOK:

        raise RuntimeError(
            "DISCORD_WEBHOOK_URL "
            "is missing."
        )

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
    # PARALLEL SCANNING
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
    # SORT
    # --------------------------------------------------------

    results.sort(
        key=lambda x:
            x["symbol"]
    )

    print()

    print(
        f"Setups found: "
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
                f"{result['volume_ratio']:.2f}x"
            )

    else:

        print(
            "No coins currently "
            "meet all conditions."
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
# RUN
# ============================================================

if __name__ == "__main__":

    main()
