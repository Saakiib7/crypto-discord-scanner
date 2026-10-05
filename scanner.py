import os
import time
import requests
from datetime import datetime, timezone

# ============================================================
# SETTINGS
# ============================================================

BINANCE_BASE = "https://fapi.binance.com"

TIMEFRAME_15M = "15m"
TIMEFRAME_1H = "1h"

EMA_PERIOD = 200
VOLUME_LOOKBACK = 20
VOLUME_MULTIPLIER = 2.0

# Discord webhook stored safely in GitHub Secrets
DISCORD_WEBHOOK = os.environ.get("DISCORD_WEBHOOK_URL")

# ============================================================
# HTTP SESSION
# ============================================================

session = requests.Session()
session.headers.update({
    "User-Agent": "Crypto-Discord-Scanner/1.0"
})


def get_json(url, params=None, retries=3):
    for attempt in range(retries):
        try:
            response = session.get(
                url,
                params=params,
                timeout=15
            )

            response.raise_for_status()
            return response.json()

        except Exception as e:
            if attempt == retries - 1:
                raise

            time.sleep(2)

    return None


# ============================================================
# GET ALL USDT-M PERPETUAL SYMBOLS
# ============================================================

def get_symbols():

    data = get_json(
        f"{BINANCE_BASE}/fapi/v1/exchangeInfo"
    )

    symbols = []

    for item in data["symbols"]:

        if (
            item["status"] == "TRADING"
            and item["contractType"] == "PERPETUAL"
            and item["quoteAsset"] == "USDT"
        ):
            symbols.append(item["symbol"])

    return symbols


# ============================================================
# GET KLINES
# ============================================================

def get_klines(symbol, interval, limit):

    return get_json(
        f"{BINANCE_BASE}/fapi/v1/klines",
        {
            "symbol": symbol,
            "interval": interval,
            "limit": limit
        }
    )


# ============================================================
# EMA CALCULATION
# ============================================================

def calculate_ema(values, period):

    if len(values) < period:
        return None

    multiplier = 2 / (period + 1)

    # Initial SMA
    ema = sum(values[:period]) / period

    # Continue EMA
    for price in values[period:]:
        ema = (price - ema) * multiplier + ema

    return ema


# ============================================================
# DAILY VWAP
# ============================================================

def calculate_daily_vwap(klines):

    now = datetime.now(timezone.utc)

    cumulative_pv = 0.0
    cumulative_volume = 0.0

    for candle in klines:

        open_time = datetime.fromtimestamp(
            candle[0] / 1000,
            timezone.utc
        )

        # Only today's candles
        if open_time.date() != now.date():
            continue

        high = float(candle[2])
        low = float(candle[3])
        close = float(candle[4])
        volume = float(candle[5])

        typical_price = (high + low + close) / 3

        cumulative_pv += typical_price * volume
        cumulative_volume += volume

    if cumulative_volume == 0:
        return None

    return cumulative_pv / cumulative_volume


# ============================================================
# CHECK ONE SYMBOL
# ============================================================

def check_symbol(symbol):

    try:

        # ----------------------------------------------------
        # 15 MINUTE DATA
        # Need enough candles for VWAP + volume comparison
        # ----------------------------------------------------

        candles_15m = get_klines(
            symbol,
            TIMEFRAME_15M,
            100
        )

        if len(candles_15m) < 30:
            return None

        # Binance's last candle may still be forming.
        # Remove it and use only completed candles.
        completed_15m = candles_15m[:-1]

        current = completed_15m[-1]
        previous = completed_15m[-2]

        current_close = float(current[4])
        previous_close = float(previous[4])

        current_volume = float(current[5])

        # ----------------------------------------------------
        # VOLUME CONDITION
        # Current completed candle volume must be > 2x
        # average of previous 20 completed candles
        # ----------------------------------------------------

        previous_volumes = [
            float(candle[5])
            for candle in completed_15m[-21:-1]
        ]

        average_volume = (
            sum(previous_volumes)
            / len(previous_volumes)
        )

        volume_ratio = (
            current_volume / average_volume
            if average_volume > 0
            else 0
        )

        volume_condition = (
            volume_ratio > VOLUME_MULTIPLIER
        )

        if not volume_condition:
            return None

        # ----------------------------------------------------
        # DAILY VWAP
        # ----------------------------------------------------

        vwap = calculate_daily_vwap(completed_15m)

        if vwap is None:
            return None

        current_above_vwap = current_close > vwap
        previous_vwap = calculate_daily_vwap(
            completed_15m[:-1]
        )

        if previous_vwap is None:
            return None

        previous_above_vwap = previous_close > previous_vwap

        # ----------------------------------------------------
        # 1H EMA 200
        # ----------------------------------------------------

        candles_1h = get_klines(
            symbol,
            TIMEFRAME_1H,
            210
        )

        if len(candles_1h) < EMA_PERIOD + 2:
            return None

        # Remove currently forming 1H candle
        completed_1h = candles_1h[:-1]

        closes_1h = [
            float(candle[4])
            for candle in completed_1h
        ]

        ema200_current = calculate_ema(
            closes_1h,
            EMA_PERIOD
        )

        ema200_previous = calculate_ema(
            closes_1h[:-1],
            EMA_PERIOD
        )

        if (
            ema200_current is None
            or ema200_previous is None
        ):
            return None

        current_above_ema = (
            current_close > ema200_current
        )

        previous_above_ema = (
            previous_close > ema200_previous
        )

        # ----------------------------------------------------
        # ALL THREE CONDITIONS
        # ----------------------------------------------------

        all_conditions = (
            current_above_vwap
            and current_above_ema
            and volume_condition
        )

        if not all_conditions:
            return None

        # ----------------------------------------------------
        # CROSSING CONDITION
        #
        # Alert when the coin has just moved above VWAP
        # OR just moved above the 1H EMA 200.
        #
        # This prevents repeated alerts while price remains
        # above both levels.
        # ----------------------------------------------------

        crossed_vwap = (
            not previous_above_vwap
            and current_above_vwap
        )

        crossed_ema = (
            not previous_above_ema
            and current_above_ema
        )

        if not (crossed_vwap or crossed_ema):
            return None

        # ----------------------------------------------------
        # RESULT
        # ----------------------------------------------------

        return {
            "symbol": symbol,
            "price": current_close,
            "vwap": vwap,
            "ema200": ema200_current,
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
            "DISCORD_WEBHOOK_URL secret is missing."
        )

    for result in results:

        symbol = result["symbol"]
        price = result["price"]
        vwap = result["vwap"]
        ema200 = result["ema200"]
        volume_ratio = result["volume_ratio"]

        crossed = []

        if result["crossed_vwap"]:
            crossed.append("VWAP")

        if result["crossed_ema"]:
            crossed.append("1H EMA 200")

        crossed_text = " + ".join(crossed)

        message = (
            f"🚨 **CRYPTO BREAKOUT ALERT** 🚨\n\n"
            f"**{symbol}**\n\n"
            f"💰 Price: `{price:.8g}`\n"
            f"📊 VWAP: `{vwap:.8g}`\n"
            f"📈 1H EMA 200: `{ema200:.8g}`\n"
            f"🔥 15M Volume: `{volume_ratio:.2f}x`\n\n"
            f"✅ Above VWAP\n"
            f"✅ Above 1H EMA 200\n"
            f"✅ Volume > 2x average\n"
            f"🔔 Crossed: **{crossed_text}**"
        )

        payload = {
            "content": message
        }

        response = session.post(
            DISCORD_WEBHOOK,
            json=payload,
            timeout=15
        )

        response.raise_for_status()

        print(
            f"Discord alert sent: {symbol}"
        )


# ============================================================
# MAIN
# ============================================================

def main():

    print("=" * 60)
    print("CRYPTO VWAP + EMA200 + VOLUME SCANNER")
    print("=" * 60)

    if not DISCORD_WEBHOOK:
        print(
            "ERROR: DISCORD_WEBHOOK_URL is not configured."
        )
        return

    print("Getting Binance USDT perpetual symbols...")

    symbols = get_symbols()

    print(
        f"Found {len(symbols)} USDT perpetual symbols."
    )

    results = []

    for index, symbol in enumerate(symbols, start=1):

        print(
            f"[{index}/{len(symbols)}] Checking {symbol}"
        )

        result = check_symbol(symbol)

        if result:
            results.append(result)

    print()
    print(
        f"Setups found: {len(results)}"
    )

    if results:
        send_discord_alert(results)

        for result in results:
            print(
                f"ALERT: {result['symbol']} "
                f"| Volume {result['volume_ratio']:.2f}x"
            )

    else:
        print(
            "No coins currently meet all conditions."
        )

    print("=" * 60)


if __name__ == "__main__":
    main()
