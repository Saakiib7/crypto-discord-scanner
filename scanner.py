import os
import time
import requests
from datetime import datetime, timezone


# ============================================================
# SETTINGS
# ============================================================

BYBIT_BASE = "https://api.bybit.com"

TIMEFRAME_15M = "15"
TIMEFRAME_1H = "60"

EMA_PERIOD = 200
VOLUME_LOOKBACK = 20
VOLUME_MULTIPLIER = 2.0

DISCORD_WEBHOOK = os.environ.get("DISCORD_WEBHOOK_URL")


# ============================================================
# HTTP SESSION
# ============================================================

session = requests.Session()

session.headers.update({
    "User-Agent": "Crypto-Discord-Scanner/1.0"
})


# ============================================================
# GET JSON FROM BYBIT
# ============================================================

def get_json(endpoint, params=None, retries=3):

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
                    f"Bybit API error: "
                    f"{data.get('retCode')} - "
                    f"{data.get('retMsg')}"
                )

            return data

        except Exception as e:

            if attempt == retries - 1:
                raise

            print(
                f"Request error: {e}. "
                f"Retrying..."
            )

            time.sleep(2)

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

    # Bybit returns newest candle first.
    candles = data["result"]["list"]

    return list(reversed(candles))


# ============================================================
# EMA
# ============================================================

def calculate_ema(
    values,
    period
):

    if len(values) < period:
        return None

    multiplier = 2 / (period + 1)

    ema = sum(
        values[:period]
    ) / period

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
# CHECK 15M CONDITIONS
# ============================================================

def check_15m_conditions(symbol):

    candles = get_klines(
        symbol,
        TIMEFRAME_15M,
        100
    )

    # Remove the currently forming candle.
    if len(candles) < 30:
        return None

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

    # --------------------------------------------------------
    # VOLUME
    # Current completed 15m candle must be > 2x
    # average volume of the previous 20 candles.
    # --------------------------------------------------------

    previous_volumes = [
        float(candle[5])
        for candle in completed[
            -21:-1
        ]
    ]

    if len(previous_volumes) < VOLUME_LOOKBACK:
        return None

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
    # VWAP
    # --------------------------------------------------------

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

    current_above_vwap = (
        current_close > current_vwap
    )

    previous_above_vwap = (
        previous_close > previous_vwap
    )

    # Must be above VWAP now.
    if not current_above_vwap:
        return None

    # --------------------------------------------------------
    # 15M VWAP CROSS
    # --------------------------------------------------------

    crossed_vwap = (
        not previous_above_vwap
        and current_above_vwap
    )

    return {
        "symbol": symbol,
        "price": current_close,
        "vwap": current_vwap,
        "volume_ratio": volume_ratio,
        "crossed_vwap": crossed_vwap
    }


# ============================================================
# CHECK 1H EMA 200
# ============================================================

def check_ema200(symbol):

    candles = get_klines(
        symbol,
        TIMEFRAME_1H,
        210
    )

    if len(candles) < EMA_PERIOD + 2:
        return None

    # Remove current unfinished 1H candle.
    completed = candles[:-1]

    closes = [
        float(candle[4])
        for candle in completed
    ]

    current_price = closes[-1]

    ema_current = calculate_ema(
        closes,
        EMA_PERIOD
    )

    ema_previous = calculate_ema(
        closes[:-1],
        EMA_PERIOD
    )

    if (
        ema_current is None
        or ema_previous is None
    ):
        return None

    above_ema = (
        current_price > ema_current
    )

    previous_above_ema = (
        closes[-2] > ema_previous
    )

    if not above_ema:
        return None

    crossed_ema = (
        not previous_above_ema
        and above_ema
    )

    return {
        "ema200": ema_current,
        "crossed_ema": crossed_ema
    }


# ============================================================
# FULL SYMBOL CHECK
# ============================================================

def check_symbol(symbol):

    try:

        # First check 15m conditions.
        data_15m = check_15m_conditions(
            symbol
        )

        if data_15m is None:
            return None

        # Then check 1H EMA 200.
        data_ema = check_ema200(
            symbol
        )

        if data_ema is None:
            return None

        # ----------------------------------------------------
        # ALL 3 CONDITIONS ARE NOW TRUE:
        #
        # 1. Above VWAP
        # 2. Above 1H EMA 200
        # 3. Volume > 2x average
        # ----------------------------------------------------

        # Alert only when price has newly crossed
        # VWAP or EMA 200.
        if not (
            data_15m["crossed_vwap"]
            or data_ema["crossed_ema"]
        ):
            return None

        return {
            "symbol": symbol,
            "price": data_15m["price"],
            "vwap": data_15m["vwap"],
            "ema200": data_ema["ema200"],
            "volume_ratio": data_15m[
                "volume_ratio"
            ],
            "crossed_vwap":
                data_15m["crossed_vwap"],
            "crossed_ema":
                data_ema["crossed_ema"]
        }

    except Exception as e:

        print(
            f"Error checking {symbol}: {e}"
        )

        return None


# ============================================================
# DISCORD
# ============================================================

def send_discord_alert(results):

    if not DISCORD_WEBHOOK:

        raise RuntimeError(
            "DISCORD_WEBHOOK_URL secret "
            "is missing."
        )

    for result in results:

        crossed = []

        if result["crossed_vwap"]:
            crossed.append("VWAP")

        if result["crossed_ema"]:
            crossed.append(
                "1H EMA 200"
            )

        message = (
            "🚨 **CRYPTO ALERT** 🚨\n\n"

            f"**{result['symbol']}**\n\n"

            f"💰 Price: "
            f"`{result['price']:.8g}`\n"

            f"📊 VWAP: "
            f"`{result['vwap']:.8g}`\n"

            f"📈 1H EMA 200: "
            f"`{result['ema200']:.8g}`\n"

            f"🔥 15M Volume: "
            f"`{result['volume_ratio']:.2f}x`\n\n"

            "✅ Above VWAP\n"
            "✅ Above 1H EMA 200\n"
            "✅ Volume > 2x average\n\n"

            f"🔔 Crossed: "
            f"**{' + '.join(crossed)}**"
        )

        response = session.post(
            DISCORD_WEBHOOK,
            json={
                "content": message
            },
            timeout=15
        )

        response.raise_for_status()

        print(
            f"Discord alert sent: "
            f"{result['symbol']}"
        )


# ============================================================
# MAIN
# ============================================================

def main():

    print("=" * 60)
    print(
        "BYBIT VWAP + EMA200 + VOLUME SCANNER"
    )
    print("=" * 60)

    if not DISCORD_WEBHOOK:

        print(
            "ERROR: "
            "DISCORD_WEBHOOK_URL is missing."
        )

        return

    print(
        "Getting Bybit USDT perpetual symbols..."
    )

    symbols = get_symbols()

    print(
        f"Found {len(symbols)} "
        "USDT perpetual symbols."
    )

    results = []

    for index, symbol in enumerate(
        symbols,
        start=1
    ):

        print(
            f"[{index}/{len(symbols)}] "
            f"Checking {symbol}"
        )

        result = check_symbol(
            symbol
        )

        if result:
            results.append(result)

    print()
    print(
        f"Setups found: "
        f"{len(results)}"
    )

    if results:

        send_discord_alert(
            results
        )

        for result in results:

            print(
                f"ALERT: "
                f"{result['symbol']} | "
                f"Volume "
                f"{result['volume_ratio']:.2f}x"
            )

    else:

        print(
            "No coins currently meet "
            "all conditions."
        )

    print("=" * 60)


if __name__ == "__main__":
    main()
