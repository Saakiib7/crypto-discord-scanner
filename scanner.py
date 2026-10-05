import os
import time
import requests
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed


# ============================================================
# SETTINGS
# ============================================================

BYBIT_BASE = "https://api.bybit.com"

TIMEFRAME_15M = "15"
TIMEFRAME_1H = "60"

EMA_PERIOD = 200
VOLUME_LOOKBACK = 20
VOLUME_MULTIPLIER = 2.0

# Number of simultaneous market-data requests.
# 15 is deliberately conservative.
MAX_WORKERS = 15

DISCORD_WEBHOOK = os.environ.get(
    "DISCORD_WEBHOOK_URL"
)


# ============================================================
# HTTP SESSION
# ============================================================

session = requests.Session()

session.headers.update({
    "User-Agent": "Crypto-Discord-Scanner/2.0"
})


# ============================================================
# BYBIT REQUEST
# ============================================================

def get_json(endpoint, params=None, retries=4):

    url = f"{BYBIT_BASE}{endpoint}"

    for attempt in range(retries):

        try:

            response = session.get(
                url,
                params=params,
                timeout=15
            )

            response.raise_for_status()

            data = response.json()

            if data.get("retCode") != 0:

                raise RuntimeError(
                    f"Bybit API error "
                    f"{data.get('retCode')}: "
                    f"{data.get('retMsg')}"
                )

            return data

        except Exception as e:

            if attempt == retries - 1:
                raise

            time.sleep(
                1.5 * (attempt + 1)
            )

    return None


# ============================================================
# GET ALL BYBIT USDT PERPETUALS
# ============================================================

def get_symbols():

    symbols = []
    cursor = None

    while True:

        params = {
            "category": "linear",
            "limit": 1000
        }

        if cursor:
            params["cursor"] = cursor

        data = get_json(
            "/v5/market/instruments-info",
            params
        )

        items = data["result"]["list"]

        for item in items:

            if (
                item.get("status") == "Trading"
                and item.get("contractType")
                == "LinearPerpetual"
                and item.get("quoteCoin") == "USDT"
            ):
                symbols.append(
                    item["symbol"]
                )

        cursor = data["result"].get(
            "nextPageCursor"
        )

        if not cursor:
            break

    return sorted(set(symbols))


# ============================================================
# GET KLINES
# ============================================================

def get_klines(
    symbol,
    interval,
    limit
):

    data = get_json(
        "/v5/market/kline",
        {
            "category": "linear",
            "symbol": symbol,
            "interval": interval,
            "limit": limit
        }
    )

    candles = data["result"]["list"]

    # Bybit returns newest first.
    # Convert to oldest → newest.
    return list(reversed(candles))


# ============================================================
# EMA
# ============================================================

def calculate_ema(values, period):

    if len(values) < period:
        return None

    multiplier = 2 / (period + 1)

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

def calculate_daily_vwap(candles):

    today = datetime.now(
        timezone.utc
    ).date()

    cumulative_pv = 0.0
    cumulative_volume = 0.0

    for candle in candles:

        candle_time = datetime.fromtimestamp(
            int(candle[0]) / 1000,
            timezone.utc
        )

        if candle_time.date() != today:
            continue

        high = float(candle[2])
        low = float(candle[3])
        close = float(candle[4])
        volume = float(candle[5])

        typical_price = (
            high + low + close
        ) / 3

        cumulative_pv += (
            typical_price * volume
        )

        cumulative_volume += volume

    if cumulative_volume <= 0:
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

        candles = get_klines(
            symbol,
            TIMEFRAME_15M,
            100
        )

        if len(candles) < 30:
            return None

        # Ignore currently forming candle.
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
            for candle in completed[
                -21:-1
            ]
        ]

        if len(previous_volumes) != 20:
            return None

        average_volume = (
            sum(previous_volumes)
            / 20
        )

        if average_volume <= 0:
            return None

        volume_ratio = (
            current_volume
            / average_volume
        )

        if volume_ratio <= 2.0:
            return None

        # ----------------------------------------------------
        # VWAP
        # ----------------------------------------------------

        current_vwap = calculate_daily_vwap(
            completed
        )

        previous_vwap = calculate_daily_vwap(
            completed[:-1]
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
        # ONLY AFTER 15M QUALIFIES:
        # GET 1H EMA 200
        # ----------------------------------------------------

        candles_1h = get_klines(
            symbol,
            TIMEFRAME_1H,
            210
        )

        if len(candles_1h) < 202:
            return None

        completed_1h = candles_1h[:-1]

        closes_1h = [
            float(candle[4])
            for candle in completed_1h
        ]

        ema200 = calculate_ema(
            closes_1h,
            200
        )

        ema200_previous = calculate_ema(
            closes_1h[:-1],
            200
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
        # ALL 3 CONDITIONS ARE TRUE
        # ----------------------------------------------------

        # Alert when price newly crosses either VWAP
        # or the 1H EMA 200.
        if not (
            crossed_vwap
            or crossed_ema
        ):
            return None

        return {
            "symbol": symbol,
            "price": current_close,
            "vwap": current_vwap,
            "ema200": ema200,
            "volume_ratio": volume_ratio,
            "crossed_vwap": crossed_vwap,
            "crossed_ema": crossed_ema
        }

    except Exception as e:

        print(
            f"Error checking {symbol}: {e}"
        )

        return None


# ============================================================
# DISCORD ALERT
# ============================================================

def send_discord_alert(results):

    if not DISCORD_WEBHOOK:

        raise RuntimeError(
            "DISCORD_WEBHOOK_URL is missing."
        )

    if not results:
        return

    lines = []

    lines.append(
        "🚨 **CRYPTO ALERTS** 🚨"
    )

    lines.append(
        f"**{len(results)} coins meet ALL 3 conditions**"
    )

    lines.append("")

    for result in results:

        crossed = []

        if result["crossed_vwap"]:
            crossed.append("VWAP")

        if result["crossed_ema"]:
            crossed.append("1H EMA 200")

        lines.append(
            f"🔔 **{result['symbol']}**"
        )

        lines.append(
            f"💰 Price: `{result['price']:.8g}`"
        )

        lines.append(
            f"📊 VWAP: `{result['vwap']:.8g}`"
        )

        lines.append(
            f"📈 1H EMA 200: "
            f"`{result['ema200']:.8g}`"
        )

        lines.append(
            f"🔥 15M Volume: "
            f"`{result['volume_ratio']:.2f}x`"
        )

        lines.append(
            "✅ VWAP | "
            "✅ EMA 200 | "
            "✅ Volume >2x"
        )

        lines.append(
            f"📍 Crossed: "
            f"**{' + '.join(crossed)}**"
        )

        lines.append("")

    message = "\n".join(lines)

    # Discord content limit is 2000 characters.
    # Split safely if necessary.
    chunks = []

    while len(message) > 1900:

        split_at = message.rfind(
            "\n\n",
            0,
            1900
        )

        if split_at == -1:
            split_at = 1900

        chunks.append(
            message[:split_at]
        )

        message = message[
            split_at:
        ].lstrip()

    if message:
        chunks.append(message)

    for chunk in chunks:

        payload = {
            "content": chunk
        }

        response = session.post(
            DISCORD_WEBHOOK,
            json=payload,
            timeout=15
        )

        # Discord rate-limit protection.
        if response.status_code == 429:

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
                f"Discord rate limit. "
                f"Waiting {retry_after}s..."
            )

            time.sleep(
                retry_after + 1
            )

            response = session.post(
                DISCORD_WEBHOOK,
                json=payload,
                timeout=15
            )

        response.raise_for_status()


# ============================================================
# MAIN
# ============================================================

def main():

    start_time = time.time()

    print("=" * 60)
    print(
        "BYBIT VWAP + EMA200 + VOLUME SCANNER"
    )
    print("=" * 60)

    if not DISCORD_WEBHOOK:

        raise RuntimeError(
            "DISCORD_WEBHOOK_URL is missing."
        )

    print(
        "Getting Bybit USDT perpetual symbols..."
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

            symbol = futures[future]

            try:

                result = future.result()

                if result:
                    results.append(result)

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

    # Keep alerts in alphabetical order.
    results.sort(
        key=lambda x: x["symbol"]
    )

    print()
    print(
        f"Setups found: "
        f"{len(results)}"
    )

    # --------------------------------------------------------
    # SEND ONE CONSOLIDATED DISCORD ALERT
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
            "No coins currently meet "
            "all conditions."
        )

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


if __name__ == "__main__":
    main()
