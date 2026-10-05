import os
import time
import threading
import requests
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed


# ============================================================
# SETTINGS
# ============================================================

OKX_BASE = "https://www.okx.com"

DISCORD_WEBHOOK = os.environ.get(
    "DISCORD_WEBHOOK_URL"
)

EMA_PERIOD = 200

VOLUME_LOOKBACK = 20

VOLUME_MULTIPLIER = 2.0

# Number of simultaneous workers
MAX_WORKERS = 15

# Stay safely below OKX public candle API limit
# OKX candle endpoint: 40 requests / 2 seconds
REQUESTS_PER_2S = 35

REQUEST_INTERVAL = 2.0 / REQUESTS_PER_2S


# ============================================================
# RATE LIMITER
# ============================================================

_last_request_time = 0.0

_rate_lock = threading.Lock()


# ============================================================
# THREAD LOCAL HTTP SESSIONS
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
                "Crypto-Discord-Scanner/3.0"
        })

        _thread_local.session = session

    return _thread_local.session


# ============================================================
# OKX REQUEST THROTTLE
# ============================================================

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
# OKX API REQUEST
# ============================================================

def get_json(
    path,
    params=None,
    retries=4
):

    url = (
        f"{OKX_BASE}{path}"
    )

    for attempt in range(retries):

        try:

            throttle()

            response = get_session().get(
                url,
                params=params,
                timeout=15
            )

            # OKX HTTP rate limit
            if response.status_code == 429:

                retry_after = (
                    response.headers.get(
                        "Retry-After"
                    )
                )

                if retry_after:

                    wait = float(
                        retry_after
                    )

                else:

                    wait = 2.0

                print(
                    f"OKX rate limit. "
                    f"Waiting {wait:.1f}s..."
                )

                time.sleep(
                    min(
                        wait + 0.5,
                        10
                    )
                )

                continue

            response.raise_for_status()

            data = response.json()

            if data.get("code") != "0":

                raise RuntimeError(
                    f"OKX API error "
                    f"{data.get('code')}: "
                    f"{data.get('msg')}"
                )

            return data

        except Exception:

            if (
                attempt
                == retries - 1
            ):

                raise

            time.sleep(
                1.0 * (attempt + 1)
            )

    return None


# ============================================================
# GET ALL OKX USDT PERPETUALS
# ============================================================

def get_symbols():

    data = get_json(
        "/api/v5/public/instruments",
        {
            "instType": "SWAP"
        }
    )

    symbols = []

    for item in data["data"]:

        inst_id = item.get(
            "instId",
            ""
        )

        if (
            item.get("state")
            == "live"

            and item.get("settleCcy")
            == "USDT"

            and item.get("ctType")
            == "linear"

            and inst_id.endswith(
                "-USDT-SWAP"
            )
        ):

            symbols.append(
                inst_id
            )

    return sorted(
        set(symbols)
    )


# ============================================================
# GET OKX CANDLES
# ============================================================

def get_candles(
    inst_id,
    bar,
    limit
):

    data = get_json(
        "/api/v5/market/candles",
        {
            "instId": inst_id,
            "bar": bar,
            "limit": str(limit)
        }
    )

    candles = data["data"]

    # OKX returns newest first.
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
        2 / (period + 1)
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

        high = float(
            candle[2]
        )

        low = float(
            candle[3]
        )

        close = float(
            candle[4]
        )

        volume = float(
            candle[5]
        )

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

    if (
        cumulative_volume
        <= 0
    ):

        return None

    return (
        cumulative_pv
        / cumulative_volume
    )


# ============================================================
# CHECK ONE COIN
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

        # Ignore currently forming candle
        completed = candles[:-1]

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
        # VOLUME > 2X
        # ----------------------------------------------------

        previous_volumes = [
            float(candle[5])
            for candle
            in completed[-21:-1]
        ]

        if (
            len(previous_volumes)
            != VOLUME_LOOKBACK
        ):

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
        # 1H EMA 200
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
        # ALL 3 CONDITIONS
        # ----------------------------------------------------

        # Alert only when price newly crosses
        # VWAP or 1H EMA 200.

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
# SPLIT DISCORD MESSAGE
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

        "🚨 **CRYPTO ALERTS** 🚨",

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

            f"📊 VWAP: "
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

        # Small pause between Discord messages
        time.sleep(0.75)


# ============================================================
# MAIN
# ============================================================

def main():

    start_time = time.time()

    print("=" * 60)

    print(
        "OKX VWAP + EMA200 + "
        "VOLUME SCANNER"
    )

    print("=" * 60)

    if not DISCORD_WEBHOOK:

        raise RuntimeError(
            "DISCORD_WEBHOOK_URL "
            "is missing."
        )

    print(
        "Getting OKX USDT "
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
                completed_count % 100
                == 0
            ):

                print(
                    f"Progress: "
                    f"{completed_count}/"
                    f"{len(symbols)}"
                )

    # --------------------------------------------------------
    # SORT RESULTS
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
    # SEND DISCORD
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
# START
# ============================================================

if __name__ == "__main__":

    main()
