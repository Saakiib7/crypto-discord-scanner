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
        SW
