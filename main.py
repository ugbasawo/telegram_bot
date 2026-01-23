import asyncio
import logging
import time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import numpy as np
from deriv_api import DerivAPI
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes

# ========================= CONFIG =========================
DEMO_TOKEN = "tIrfitLjqeBxCOM"
REAL_TOKEN = "2hsJzopRHG5w"
APP_ID = 1089

MARKETS = ["R_10", "R_25", "R_50"]

COOLDOWN_SEC = 120
MAX_TRADES_PER_DAY = 60
MAX_CONSEC_LOSSES = 10

TELEGRAM_TOKEN = "8253450930:AAHUhPk9TML-8kZlA9UaHZZvTUGdurN9MVY"
TELEGRAM_CHAT_ID = "7634818949"

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# ========================= STRATEGY SETTINGS =========================
TF_SEC = 60  # M1 candles
CANDLES_COUNT = 150
SCAN_SLEEP_SEC = 2

RSI_PERIOD = 14
DURATION_MIN = 1

# ========================= EMA50 SLOPE + MARTINGALE + DAILY TARGET =========================
EMA_SLOPE_LOOKBACK = 10
EMA_SLOPE_MIN = 0.0

MARTINGALE_MULT = 2.0
MARTINGALE_MAX_STEPS = 4
MARTINGALE_MAX_PAYOUT = 8.0  # cap payout, not stake

DAILY_PROFIT_TARGET = 5.0

# ========================= PAYOUT MODE SETTINGS =========================
# ✅ Deriv will calculate the stake automatically from payout.
USE_PAYOUT_MODE_DEFAULT = True
BASE_PAYOUT = 0.50           # ✅ what you requested
MIN_PAYOUT = 0.01            # Deriv rule (your error was because payout wasn't sent / was 0)

# ========================= INDICATOR MATH =========================
def calculate_ema(values, period: int):
    values = np.array(values, dtype=float)
    if len(values) < period:
        return np.array([])
    k = 2.0 / (period + 1.0)
    ema = np.zeros_like(values)
    ema[0] = values[0]
    for i in range(1, len(values)):
        ema[i] = values[i] * k + ema[i - 1] * (1 - k)
    return ema

def calculate_rsi(values, period=14):
    values = np.array(values, dtype=float)
    n = len(values)
    if n < period + 2:
        return np.array([])

    deltas = np.diff(values)
    gains = np.where(deltas > 0, deltas, 0.0)
    losses = np.where(deltas < 0, -deltas, 0.0)

    rsi = np.full(n, np.nan, dtype=float)

    avg_gain = np.mean(gains[:period])
    avg_loss = np.mean(losses[:period])
    rs = avg_gain / (avg_loss + 1e-12)
    rsi[period] = 100.0 - (100.0 / (1.0 + rs))

    for i in range(period + 1, n):
        gain = gains[i - 1]
        loss = losses[i - 1]
        avg_gain = (avg_gain * (period - 1) + gain) / period
        avg_loss = (avg_loss * (period - 1) + loss) / period
        rs = avg_gain / (avg_loss + 1e-12)
        rsi[i] = 100.0 - (100.0 / (1.0 + rs))

    return rsi

def build_candles_from_deriv(candles_raw):
    out = []
    for x in candles_raw:
        out.append({
            "t0": int(x.get("epoch", 0)),
            "o": float(x.get("open", 0)),
            "h": float(x.get("high", 0)),
            "l": float(x.get("low", 0)),
            "c": float(x.get("close", 0)),
        })
    return out

def fmt_time_hhmmss(epoch):
    try:
        return datetime.fromtimestamp(epoch, ZoneInfo("Africa/Lagos")).strftime("%H:%M:%S")
    except:
        return "—"


# ========================= BOT CORE =========================
class DerivSniperBot:
    def __init__(self):
        self.api = None
        self.app = None
        self.active_token = None
        self.account_type = "None"

        self.is_scanning = False
        self.scanner_task = None
        self.market_tasks = {}

        self.active_trade_info = None
        self.active_market = "None"
        self.trade_start_time = 0.0

        self.cooldown_until = 0.0
        self.trades_today = 0
        self.total_losses_today = 0
        self.consecutive_losses = 0
        self.total_profit_today = 0.0
        self.balance = "0.00"

        self.trade_lock = asyncio.Lock()

        self.market_debug = {m: {} for m in MARKETS}
        self.last_processed_closed_t0 = {m: 0 for m in MARKETS}

        # Daily controls
        self.tz = ZoneInfo("Africa/Lagos")
        self.current_day = datetime.now(self.tz).date()
        self.pause_until = 0.0

        # ✅ Payout mode + martingale payout
        self.use_payout_mode = USE_PAYOUT_MODE_DEFAULT
        self.base_payout = max(float(BASE_PAYOUT), MIN_PAYOUT)
        self.current_payout = self.base_payout
        self.martingale_step = 0

    async def connect(self) -> bool:
        try:
            if not self.active_token:
                return False
            self.api = DerivAPI(app_id=APP_ID)
            await self.api.authorize(self.active_token)
            await self.fetch_balance()
            return True
        except Exception as e:
            logger.error(f"Connect error: {e}")
            return False

    async def safe_reconnect(self) -> bool:
        try:
            if self.api:
                try:
                    await self.api.disconnect()
                except:
                    pass
        except:
            pass
        self.api = None
        return await self.connect()

    async def safe_ticks_history(self, payload: dict, retries: int = 4):
        last_err = None
        for attempt in range(1, retries + 1):
            try:
                if not self.api:
                    ok = await self.safe_reconnect()
                    if not ok:
                        raise RuntimeError("Reconnect failed")
                return await self.api.ticks_history(payload)
            except Exception as e:
                last_err = e
                msg = str(e).lower()

                if "rate limit" in msg or "too many" in msg or "throttle" in msg:
                    await asyncio.sleep(2.5 * attempt)
                else:
                    await asyncio.sleep(1.0 * attempt)

                if any(k in msg for k in ["disconnect", "connection", "websocket", "not connected", "authorize", "auth"]):
                    await self.safe_reconnect()

        raise last_err

    async def fetch_balance(self):
        if not self.api:
            return
        try:
            bal = await self.api.balance({"balance": 1})
            self.balance = f"{float(bal['balance']['balance']):.2f} {bal['balance']['currency']}"
        except:
            pass

    # ========================= DAILY RESET / PAUSE =========================
    def _next_midnight_epoch(self) -> float:
        now = datetime.now(self.tz)
        next_midnight = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        return next_midnight.timestamp()

    def _daily_reset_if_needed(self):
        today = datetime.now(self.tz).date()
        if today != self.current_day:
            self.current_day = today
            self.trades_today = 0
            self.total_losses_today = 0
            self.consecutive_losses = 0
            self.total_profit_today = 0.0
            self.cooldown_until = 0.0
            self.pause_until = 0.0
            self.martingale_step = 0
            self.current_payout = self.base_payout
    # ========================= END DAILY RESET / PAUSE =========================

    def can_auto_trade(self) -> tuple[bool, str]:
        self._daily_reset_if_needed()

        if time.time() < self.pause_until:
            left = int(self.pause_until - time.time())
            return False, f"Paused until 12:00am WAT ({left}s)"

        if self.total_profit_today >= DAILY_PROFIT_TARGET:
            self.pause_until = self._next_midnight_epoch()
            return False, f"Daily target reached (+${self.total_profit_today:.2f})"

        if self.consecutive_losses >= MAX_CONSEC_LOSSES:
            return False, "Stopped: max loss streak reached"
        if self.trades_today >= MAX_TRADES_PER_DAY:
            return False, "Stopped: daily trade limit reached"
        if time.time() < self.cooldown_until:
            return False, f"Cooldown {int(self.cooldown_until - time.time())}s"
        if self.active_trade_info:
            return False, "Trade in progress"
        if not self.api:
            return False, "Not connected"
        return True, "OK"

    async def background_scanner(self):
        if not self.api:
            return
        self.market_tasks = {sym: asyncio.create_task(self.scan_market(sym)) for sym in MARKETS}
        try:
            while self.is_scanning:
                if self.active_trade_info and (time.time() - self.trade_start_time > (DURATION_MIN * 60 + 90)):
                    self.active_trade_info = None
                await asyncio.sleep(1)
        finally:
            for t in self.market_tasks.values():
                t.cancel()
            self.market_tasks.clear()

    async def fetch_real_m1_candles(self, symbol: str):
        payload = {
            "ticks_history": symbol,
            "end": "latest",
            "count": CANDLES_COUNT,
            "style": "candles",
            "granularity": TF_SEC
        }
        data = await self.safe_ticks_history(payload, retries=4)
        return build_candles_from_deriv(data.get("candles", []))

    async def _sleep_until(self, epoch_ts: float, buffer_s: float = 0.15):
        wait = (epoch_ts - time.time()) + buffer_s
        if wait > 0:
            await asyncio.sleep(wait)
        else:
            await asyncio.sleep(0.10)

    async def _sync_to_next_candle_open(self, last_closed_t0: int):
        next_open = int(last_closed_t0) + int(TF_SEC)
        await self._sleep_until(next_open, buffer_s=0.20)

    async def scan_market(self, symbol: str):
        while self.is_scanning:
            try:
                if self.consecutive_losses >= MAX_CONSEC_LOSSES or self.trades_today >= MAX_TRADES_PER_DAY:
                    self.is_scanning = False
                    break

                ok_gate, gate = self.can_auto_trade()

                candles = await self.fetch_real_m1_candles(symbol)
                if len(candles) < 70:
                    self.market_debug[symbol] = {
                        "time": time.time(),
                        "gate": "Waiting for more candles",
                        "why": [f"Not enough candle history yet (need ~70, have {len(candles)})."],
                    }
                    await asyncio.sleep(SCAN_SLEEP_SEC)
                    continue

                pullback = candles[-3]
                confirm = candles[-2]
                confirm_t0 = int(confirm["t0"])

                # Rate-limit fix: if no new candle, wait
                if self.last_processed_closed_t0[symbol] == confirm_t0:
                    await self._sync_to_next_candle_open(confirm_t0)
                    continue

                closes = [x["c"] for x in candles]

                ema20_arr = calculate_ema(closes, 20)
                ema50_arr = calculate_ema(closes, 50)
                if len(ema20_arr) < 60 or len(ema50_arr) < 60:
                    self.market_debug[symbol] = {"time": time.time(), "gate": "Indicators", "why": ["EMA20/EMA50 not ready yet."]}
                    await asyncio.sleep(SCAN_SLEEP_SEC)
                    continue

                ema20_pullback = float(ema20_arr[-3])
                ema20_confirm = float(ema20_arr[-2])
                ema50_confirm = float(ema50_arr[-2])

                # EMA50 slope
                slope_ok = False
                ema50_slope = 0.0
                ema50_rising = False
                ema50_falling = False
                if len(ema50_arr) >= (EMA_SLOPE_LOOKBACK + 3):
                    ema50_prev = float(ema50_arr[-(EMA_SLOPE_LOOKBACK + 2)])
                    ema50_slope = ema50_confirm - ema50_prev
                    ema50_rising = ema50_slope > EMA_SLOPE_MIN
                    ema50_falling = ema50_slope < -EMA_SLOPE_MIN
                    slope_ok = True

                rsi_arr = calculate_rsi(closes, RSI_PERIOD)
                if len(rsi_arr) < 60 or np.isnan(rsi_arr[-2]):
                    self.market_debug[symbol] = {"time": time.time(), "gate": "Indicators", "why": ["RSI not ready yet."]}
                    await asyncio.sleep(SCAN_SLEEP_SEC)
                    continue
                rsi_now = float(rsi_arr[-2])

                # Pullback touch EMA20 (aligned)
                pb_high = float(pullback["h"])
                pb_low = float(pullback["l"])
                touched_ema20 = (pb_low <= ema20_pullback <= pb_high)

                # Confirm candle color + close vs EMA20
                c_open = float(confirm["o"])
                c_close = float(confirm["c"])
                bull_confirm = c_close > c_open
                bear_confirm = c_close < c_open

                close_above_ema20 = c_close > ema20_confirm
                close_below_ema20 = c_close < ema20_confirm

                # Spike filter
                bodies = [abs(float(candles[i]["c"]) - float(candles[i]["o"])) for i in range(-22, -2)]
                avg_body = float(np.mean(bodies)) if len(bodies) >= 10 else float(np.mean([abs(float(c["c"]) - float(c["o"])) for c in candles[-60:-2]]))
                last_body = abs(c_close - c_open)

                if symbol == "R_50":
                    spike_mult = 1.2
                    rsi_call_min, rsi_call_max = 53.0, 62.0
                    rsi_put_min, rsi_put_max = 38.0, 47.0
                    ema_diff_min = 0.30
                else:
                    spike_mult = 1.5
                    rsi_call_min, rsi_call_max = 52.0, 60.0
                    rsi_put_min, rsi_put_max = 40.0, 48.0
                    ema_diff_min = 0.20

                spike_block = (avg_body > 0 and last_body > spike_mult * avg_body)

                ema_diff = abs(ema20_confirm - ema50_confirm)
                flat_block = ema_diff < ema_diff_min

                uptrend = ema20_confirm > ema50_confirm
                downtrend = ema20_confirm < ema50_confirm

                call_rsi_ok = (rsi_call_min <= rsi_now <= rsi_call_max)
                put_rsi_ok = (rsi_put_min <= rsi_now <= rsi_put_max)

                call_ready = (
                    uptrend
                    and slope_ok and ema50_rising
                    and touched_ema20
                    and bull_confirm
                    and close_above_ema20
                    and call_rsi_ok
                    and not spike_block
                    and not flat_block
                )

                put_ready = (
                    downtrend
                    and slope_ok and ema50_falling
                    and touched_ema20
                    and bear_confirm
                    and close_below_ema20
                    and put_rsi_ok
                    and not spike_block
                    and not flat_block
                )

                signal = "CALL" if call_ready else "PUT" if put_ready else None

                trend_label = "UPTREND" if uptrend else "DOWNTREND" if downtrend else "SIDEWAYS"
                ema_label = "EMA20 ABOVE EMA50" if uptrend else "EMA20 BELOW EMA50" if downtrend else "EMA20 = EMA50"
                trend_strength = "STRONG" if not flat_block else "WEAK"
                pullback_label = "PULLBACK TOUCHED ✅" if touched_ema20 else "WAITING PULLBACK…"
                confirm_close_label = (
                    "CONFIRM CLOSE > EMA20 ✅" if close_above_ema20 else
                    "CONFIRM CLOSE < EMA20 ✅" if close_below_ema20 else
                    "CONFIRM CLOSE ON EMA20"
                )
                slope_label = "EMA50 SLOPE ↑" if ema50_rising else "EMA50 SLOPE ↓" if ema50_falling else "EMA50 SLOPE FLAT"

                block_label = []
                if spike_block:
                    block_label.append("SPIKE BLOCK")
                if flat_block:
                    block_label.append("WEAK/FLAT TREND")
                if not slope_ok:
                    block_label.append("SLOPE N/A")
                block_label = " | ".join(block_label) if block_label else "OK"

                why = []
                if not ok_gate:
                    why.append(f"Gate blocked: {gate}")
                if signal:
                    why.append(f"READY: {signal} (enter next candle)")
                else:
                    why.append("Waiting: setup")

                self.market_debug[symbol] = {
                    "time": time.time(),
                    "gate": gate,
                    "last_closed": confirm_t0,
                    "signal": signal,
                    "trend_label": trend_label,
                    "ema_label": ema_label,
                    "trend_strength": trend_strength,
                    "pullback_label": pullback_label,
                    "block_label": block_label,
                    "confirm_close_label": confirm_close_label,
                    "slope_label": slope_label,
                    "ema50_slope": ema50_slope,
                    "why": why[:10],
                }

                self.last_processed_closed_t0[symbol] = confirm_t0

                if not ok_gate:
                    await self._sync_to_next_candle_open(confirm_t0)
                    continue

                if call_ready or put_ready:
                    await self._sync_to_next_candle_open(confirm_t0)

                if call_ready:
                    await self.execute_trade("CALL", symbol, reason="AUTO Signal", source="AUTO")
                elif put_ready:
                    await self.execute_trade("PUT", symbol, reason="AUTO Signal", source="AUTO")

                await asyncio.sleep(0.25)

            except asyncio.CancelledError:
                break
            except Exception as e:
                msg = str(e)
                logger.error(f"Scanner Error ({symbol}): {msg}")
                self.market_debug[symbol] = {"time": time.time(), "gate": "Error", "why": [msg[:160]]}

                low = msg.lower()
                if "rate limit" in low or "too many" in low or "throttle" in low:
                    await asyncio.sleep(6)
                elif any(k in low for k in ["disconnect", "connection", "websocket", "not connected"]):
                    await self.safe_reconnect()
                    await asyncio.sleep(2)
                else:
                    await asyncio.sleep(2)

            await asyncio.sleep(SCAN_SLEEP_SEC)

    async def execute_trade(self, side: str, symbol: str, reason="MANUAL", source="MANUAL"):
        if not self.api or self.active_trade_info:
            return

        async with self.trade_lock:
            ok, _gate = self.can_auto_trade()
            if not ok:
                return

            try:
                # ✅ payout selection (AUTO uses martingale payout, MANUAL uses base payout)
                payout = float(self.current_payout if source == "AUTO" else self.base_payout)
                payout = max(payout, MIN_PAYOUT)
                payout = round(payout, 2)

                # ✅ Deriv proposal in payout mode
                prop = await self.api.proposal({
                    "proposal": 1,
                    "amount": payout,
                    "basis": "payout",
                    "contract_type": side,
                    "currency": "USD",
                    "duration": int(DURATION_MIN),
                    "duration_unit": "m",
                    "symbol": symbol
                })

                # ask_price = stake Deriv will charge for that payout
                stake_used = float(prop["proposal"]["ask_price"])

                buy = await self.api.buy({
                    "buy": prop["proposal"]["id"],
                    "price": stake_used
                })

                self.active_trade_info = int(buy["buy"]["contract_id"])
                self.active_market = symbol
                self.trade_start_time = time.time()

                if source == "AUTO":
                    self.trades_today += 1

                safe_symbol = str(symbol).replace("_", " ")
                mode_label = "PAYOUT" if self.use_payout_mode else "STAKE"
                msg = (
                    f"🚀 {side} TRADE OPENED\n"
                    f"🛒 Market: {safe_symbol}\n"
                    f"⏱ Expiry: {DURATION_MIN}m\n"
                    f"🎯 Mode: {mode_label}\n"
                    f"🎯 Payout: ${payout:.2f}\n"
                    f"💵 Stake Used (Deriv): ${stake_used:.2f}\n"
                    f"🧠 Reason: {reason}\n"
                    f"🤖 Source: {source}\n"
                    f"💵 Today PnL: {self.total_profit_today:+.2f} / +{DAILY_PROFIT_TARGET:.2f}\n"
                    f"🧪 Martingale(step): {self.martingale_step}/{MARTINGALE_MAX_STEPS}"
                )
                await self.app.bot.send_message(TELEGRAM_CHAT_ID, msg)

                asyncio.create_task(self.check_result(self.active_trade_info, source, payout_used=payout))

            except Exception as e:
                logger.error(f"Trade error: {e}")

    async def check_result(self, cid: int, source: str, payout_used: float):
        await asyncio.sleep(int(DURATION_MIN) * 60 + 5)
        try:
            res = await self.api.proposal_open_contract({"proposal_open_contract": 1, "contract_id": cid})
            profit = float(res["proposal_open_contract"].get("profit", 0))

            if source == "AUTO":
                self.total_profit_today += profit

                if profit <= 0:
                    self.consecutive_losses += 1
                    self.total_losses_today += 1

                    # ✅ Martingale on PAYOUT (2x payout next time)
                    self.martingale_step = min(self.martingale_step + 1, MARTINGALE_MAX_STEPS)
                    next_payout = float(payout_used) * float(MARTINGALE_MULT)
                    self.current_payout = min(next_payout, MARTINGALE_MAX_PAYOUT)
                else:
                    self.consecutive_losses = 0
                    self.martingale_step = 0
                    self.current_payout = self.base_payout

                if self.total_profit_today >= DAILY_PROFIT_TARGET:
                    self.pause_until = self._next_midnight_epoch()

            await self.fetch_balance()

            pause_note = ""
            if time.time() < self.pause_until:
                pause_note = f"\n⏸ Paused until 12:00am WAT"

            await self.app.bot.send_message(
                TELEGRAM_CHAT_ID,
                (
                    f"🏁 FINISH: {'WIN' if profit > 0 else 'LOSS'} ({profit:+.2f})\n"
                    f"📊 Today: {self.trades_today}/{MAX_TRADES_PER_DAY} | ❌ Losses: {self.total_losses_today} | Streak: {self.consecutive_losses}/{MAX_CONSEC_LOSSES}\n"
                    f"💵 Today PnL: {self.total_profit_today:+.2f} / +{DAILY_PROFIT_TARGET:.2f}\n"
                    f"🎯 Next Payout: ${self.current_payout:.2f} (step {self.martingale_step}/{MARTINGALE_MAX_STEPS})\n"
                    f"💰 Balance: {self.balance}"
                    f"{pause_note}"
                )
            )
        finally:
            self.active_trade_info = None
            self.cooldown_until = time.time() + COOLDOWN_SEC


# ========================= UI =========================
bot_logic = DerivSniperBot()

def main_keyboard():
    payout_status = "✅ PAYOUT" if bot_logic.use_payout_mode else "❌ PAYOUT"
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("▶️ START", callback_data="START_SCAN"),
         InlineKeyboardButton("⏹️ STOP", callback_data="STOP_SCAN")],
        [InlineKeyboardButton("📊 STATUS", callback_data="STATUS"),
         InlineKeyboardButton("🔄 REFRESH", callback_data="STATUS")],
        [InlineKeyboardButton("🧪 TEST BUY", callback_data="TEST_BUY")],
        [InlineKeyboardButton("🧪 DEMO", callback_data="SET_DEMO"),
         InlineKeyboardButton("💰 LIVE", callback_data="SET_REAL")],
        [InlineKeyboardButton(payout_status, callback_data="TOGGLE_PAYOUT")]
    ])

def format_market_detail(sym: str, d: dict) -> str:
    if not d:
        return f"📍 {sym.replace('_',' ')}\n⏳ No scan data yet"

    age = int(time.time() - d.get("time", time.time()))
    gate = d.get("gate", "—")
    last_closed = d.get("last_closed", 0)
    signal = d.get("signal") or "—"

    trend_label = d.get("trend_label", "—")
    ema_label = d.get("ema_label", "—")
    trend_strength = d.get("trend_strength", "—")
    pullback_label = d.get("pullback_label", "—")
    block_label = d.get("block_label", "—")
    confirm_close_label = d.get("confirm_close_label", "—")
    slope_label = d.get("slope_label", "—")

    return (
        f"📍 {sym.replace('_',' ')} ({age}s)\n"
        f"Gate: {gate}\n"
        f"Last closed: {fmt_time_hhmmss(last_closed)}\n"
        f"────────────────\n"
        f"Trend: {trend_label} ({trend_strength})\n"
        f"{ema_label}\n"
        f"{slope_label}\n"
        f"{pullback_label}\n"
        f"{confirm_close_label}\n"
        f"Filters: {block_label}\n"
        f"Signal: {signal}\n"
    )

async def btn_handler(u: Update, c: ContextTypes.DEFAULT_TYPE):
    q = u.callback_query
    await q.answer()

    if q.data == "SET_DEMO":
        bot_logic.active_token, bot_logic.account_type = DEMO_TOKEN, "DEMO"
        ok = await bot_logic.connect()
        await q.edit_message_text("✅ Connected to DEMO" if ok else "❌ DEMO Failed", reply_markup=main_keyboard())

    elif q.data == "SET_REAL":
        bot_logic.active_token, bot_logic.account_type = REAL_TOKEN, "LIVE"
        ok = await bot_logic.connect()
        await q.edit_message_text("⚠️ LIVE CONNECTED" if ok else "❌ LIVE Failed", reply_markup=main_keyboard())

    elif q.data == "TOGGLE_PAYOUT":
        bot_logic.use_payout_mode = not bot_logic.use_payout_mode
        # In this build, we always trade payout-mode logic.
        # The toggle is mainly for UI confirmation, and future expansion.
        await q.edit_message_text(
            f"✅ Payout mode is now {'ON' if bot_logic.use_payout_mode else 'OFF'}",
            reply_markup=main_keyboard()
        )

    elif q.data == "START_SCAN":
        if not bot_logic.api:
            await q.edit_message_text("❌ Connect first.", reply_markup=main_keyboard())
            return
        bot_logic.is_scanning = True
        bot_logic.scanner_task = asyncio.create_task(bot_logic.background_scanner())
        await q.edit_message_text(
            f"🔍 SCANNER ACTIVE\n"
            f"🎯 MODE: PAYOUT\n"
            f"🎯 Base Payout: ${bot_logic.base_payout:.2f}\n"
            f"🧪 Martingale: {MARTINGALE_MULT}x payout (max steps {MARTINGALE_MAX_STEPS}, cap payout ${MARTINGALE_MAX_PAYOUT:.2f})\n"
            f"🎯 Daily Target: +${DAILY_PROFIT_TARGET:.2f} (pause till 12am WAT)\n"
            f"🕯 Timeframe: M1 | ⏱ Expiry: {DURATION_MIN}m",
            reply_markup=main_keyboard()
        )

    elif q.data == "STOP_SCAN":
        bot_logic.is_scanning = False
        await q.edit_message_text("⏹️ Scanner stopped.", reply_markup=main_keyboard())

    elif q.data == "TEST_BUY":
        await bot_logic.execute_trade("CALL", "R_10", "Manual Test", source="MANUAL")

    elif q.data == "STATUS":
        await bot_logic.fetch_balance()
        now_time = datetime.now(ZoneInfo("Africa/Lagos")).strftime("%Y-%m-%d %H:%M:%S")
        _ok, gate = bot_logic.can_auto_trade()

        trade_status = "No Active Trade"
        if bot_logic.active_trade_info and bot_logic.api:
            try:
                res = await bot_logic.api.proposal_open_contract({"proposal_open_contract": 1, "contract_id": bot_logic.active_trade_info})
                pnl = float(res["proposal_open_contract"].get("profit", 0))
                rem = max(0, int(DURATION_MIN * 60) - int(time.time() - bot_logic.trade_start_time))
                icon = "✅ PROFIT" if pnl > 0 else "❌ LOSS" if pnl < 0 else "➖ FLAT"
                mkt_clean = str(bot_logic.active_market).replace("_", " ")
                trade_status = f"🚀 Active Trade ({mkt_clean})\n📈 PnL: {icon} ({pnl:+.2f})\n⏳ Left: {rem}s"
            except:
                trade_status = "🚀 Active Trade: Syncing..."

        pause_line = ""
        if time.time() < bot_logic.pause_until:
            pause_line = f"⏸ Paused until 12:00am WAT\n"

        header = (
            f"🕒 Time (WAT): {now_time}\n"
            f"🤖 Bot: {'ACTIVE' if bot_logic.is_scanning else 'OFFLINE'} ({bot_logic.account_type})\n"
            f"{pause_line}"
            f"🎯 MODE: PAYOUT\n"
            f"🎯 Base Payout: ${bot_logic.base_payout:.2f} | Next Payout: ${bot_logic.current_payout:.2f}\n"
            f"⏱ Expiry: {DURATION_MIN}m | Cooldown: {COOLDOWN_SEC}s\n"
            f"🎯 Daily Target: +${DAILY_PROFIT_TARGET:.2f}\n"
            f"📡 Markets: {', '.join(MARKETS).replace('_',' ')}\n"
            f"━━━━━━━━━━━━━━━\n{trade_status}\n━━━━━━━━━━━━━━━\n"
            f"💵 Total Profit Today: {bot_logic.total_profit_today:+.2f}\n"
            f"🎯 Trades: {bot_logic.trades_today}/{MAX_TRADES_PER_DAY} | ❌ Losses: {bot_logic.total_losses_today}\n"
            f"📉 Loss Streak: {bot_logic.consecutive_losses}/{MAX_CONSEC_LOSSES} | 🧪 Martingale Step: {bot_logic.martingale_step}/{MARTINGALE_MAX_STEPS}\n"
            f"🚦 Gate: {gate}\n"
            f"💰 Balance: {bot_logic.balance}\n"
        )

        details = "\n\n📌 LIVE SCAN (Simple)\n\n" + "\n\n".join(
            [format_market_detail(sym, bot_logic.market_debug.get(sym, {})) for sym in MARKETS]
        )

        await q.edit_message_text(header + details, reply_markup=main_keyboard())

async def start_cmd(u: Update, c: ContextTypes.DEFAULT_TYPE):
    await u.message.reply_text(
        "💎 Deriv Bot (PAYOUT MODE)\n"
        "🎯 Payout: $0.50 (Deriv calculates stake)\n"
        f"🧪 Martingale: {MARTINGALE_MULT}x payout (max steps {MARTINGALE_MAX_STEPS})\n"
        f"🎯 Daily Target: +${DAILY_PROFIT_TARGET:.2f} (pause till 12am WAT)\n",
        reply_markup=main_keyboard()
    )

if __name__ == "__main__":
    app = Application.builder().token(TELEGRAM_TOKEN).build()
    bot_logic.app = app
    app.add_handler(CommandHandler("start", start_cmd))
    app.add_handler(CallbackQueryHandler(btn_handler))
    app.run_polling(drop_pending_updates=True)
