import asyncio
import os
import json
import logging
from datetime import datetime, timedelta
import pandas as pd
import pandas_ta as ta
import ccxt
import aiohttp
from aiogram import Bot, Dispatcher
from aiogram.filters import Command
from aiogram.types import Message
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from dotenv import load_dotenv

# ===================== CONFIG =====================
load_dotenv()
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
if not TELEGRAM_TOKEN:
    raise ValueError("❌ TELEGRAM_TOKEN missing")

CHECK_INTERVAL = 5  # minutes — интервал сканирования рынка
COOLDOWN_MINUTES = 30  # cooldown между сигналами для одного символа
DATA_FILE = "data.json"
BYBIT_DELAY = 0.5   # seconds — задержка между запросами

# ===================== LOGGING =====================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger(__name__)

# ===================== BOT =====================
bot = Bot(token=TELEGRAM_TOKEN)
dp = Dispatcher()
subscribers = set()
last_signals = {}

# ===================== STORAGE =====================
def load_data():
    global subscribers
    if os.path.exists(DATA_FILE):
        try:
            with open(DATA_FILE, "r") as f:
                data = json.load(f)
                subcribers = set(data.get("subcribers", []))
                logger.info(f"Loaded {len(subcribers)} subscribers")
        except Exception as e:
            logger.error(f"Failed to load data file: {e}")

def save_data():
    try:
        with open(DATA_FILE, "w") as f:
            json.dump({"subcribers": list(subcribers)}, f)
    except Exception as e:
        logger.error(f"Failed to save data file: {e}")

# ===================== EXCHANGE =====================
async def init_exchange():
    connector = aiohttp.TCPConnector(limit=100, limit_per_host=30)
    session = aiohttp.ClientSession(connector=connector)
    exchange = ccxt.bybit({
        "enableRateLimit": True,
        "options": {
            "defaultType": "future",
            "http": {"aiohttp_session": session}
        }
    })
    return exchange, session

# ===================== HELPER FUNCTIONS =====================
async def fetch_ohlcv_async(exchange, symbol):
    try:
        return await exchange.fetch_ohlcv(symbol, timeframe="5m", limit=100, params={"category": "linear"})
    except Exception as e:
        logger.error(f"fetch_ohlcv error for {symbol}: {e}")
        return None

async def fetch_funding(exchange, symbol):
    try:
        info = await exchange.fetch_funding_rate(symbol)
        return info.get('fundingRate', 0)
    except Exception as e:
        logger.error(f"fetch_funding error for {symbol}: {e}")
        return 0

async def fetch_oi(exchange, symbol):
    try:
        ticker = await exchange.fetch_ticker(symbol)
        return ticker.get('quoteVolume', 0)  # Используем volume как прокси для OI
    except Exception as e:
        logger.error(f"fetch_oi error for {symbol}: {e}")
        return 0

def calculate_indicators(df):
    df["rsi"] = ta.rsi(df["close"], length=14)
    df["ema50"] = ta.ema(df["close"], length=50)
    df["atr"] = ta.atr(df["high"], df["low"], df["close"], length=14)
    return df

def get_trend_filter(df):
    ema200 = ta.ema(df["close"], length=200)
    ema50 = ta.ema(df["close"], length=50)
    if len(ema200) < 2 or len(ema50) < 2:
        return True
    return ema200.iloc[-1] < ema50.iloc[-1]  # Нисходящий тренд


def normalize_data(series):
    mean = series.mean()
    std = series.std()
    if std == 0:
        return 0
    return (series.iloc[-1] - mean) / std


def get_signal(df, funding_rate, open_interest):
    price = df["close"].iloc[-1]
    ema = df["ema50"].iloc[-1]
    rsi = df["rsi"].iloc[-1]
    atr = df["atr"].iloc[-1] if "atr" in df.columns else 0.01

    z_funding = normalize_data(df["funding"]) if "funding" in df else 0
    z_oi = normalize_data(df["oi"]) if "oi" in df else 0

    score = 0
    reasons = []

    if rsi > 75:
        score += 3
        reasons.append("RSI перекуплен (>75)")
    volume_spike = df["volume"].iloc[-1] > df["volume"].rolling(20).mean().iloc[-2] * 1.8
    if volume_spike:
        score += 2
        reasons.append("Всплеск объёма (>1.8× среднего)")
    ema_deviation = abs(price - ema) / ema
    if ema_deviation > atr * 2:
        score += 2
        reasons.append(f"Сильное отклонение от EMA ({ema_deviation:.2%})")
    if z_funding > 1:
        score += 1
        reasons.append("Высокая ставка финансирования (Z>1)")
    if z_oi > 1:
        score += 1
        reasons.append("Рост открытого интереса (Z>1)")

    dynamic_threshold = 8 if atr < 0.01 else 6
    trend_ok = get_trend_filter(df)

    stop_loss = price * 0.98
    take_profit = price * 0.94

    return score >= dynamic_threshold and trend_ok, score, reasons, {
        "stop_loss": stop_loss,
        "take_profit": take_profit,
        "atr": atr
    }

# ===================== CORE FUNCTIONS =====================
async def process_symbol(exchange, symbol):
    try:
        await asyncio.sleep(BYBIT_DELAY)

        now = datetime.now()
        if symbol in last_signals and now - last_signals[symbol] < timedelta(minutes=COOLDOWN_MINUTES):
                        return None

        ohlcv = await fetch_ohlcv_async(exchange, symbol)
        if not ohlcv:
            return None

        df = pd.DataFrame(ohlcv, columns=["ts", "open", "high", "low", "close", "volume"])
        df["close"] = pd.to_numeric(df["close"], errors='coerce')
        df["volume"] = pd.to_numeric(df["volume"], errors='coerce')
        df = calculate_indicators(df)

        # Добавляем данные финансирования и OI в DataFrame для нормализации
        funding_rate = await fetch_funding(exchange, symbol)
        open_interest = await fetch_oi(exchange, symbol)
        df["funding"] = [funding_rate] * len(df)
        df["oi"] = [open_interest] * len(df)

        signal_triggered, score, reasons, risk_management = get_signal(df, funding_rate, open_interest)

        if signal_triggered:
            last_signals[symbol] = now
            return symbol, score, reasons, risk_management
    except Exception as e:
        logger.error(f"process_symbol error for {symbol}: {e}")
    return None


async def scan_market(exchange):
    logger.info("🔍 Scan start")
    try:
        markets = await exchange.load_markets()
        symbols = [
            s for s, i in markets.items()
            if i.get("linear") and i.get("quote") == "USDT" and i.get("active", True)
        ]

        # Ограничиваем количество символов для сканирования
        symbols_to_scan = symbols[:50]
        tasks = [process_symbol(exchange, s) for s in symbols_to_scan]
        results = await asyncio.gather(*tasks)

        signals = [r for r in results if r]

        for symbol, score, reasons, rm in signals:
            token = symbol.replace("USDT", "")
            reasons_text = "\n".join([f"• {r}" for r in reasons])
            text = f"""
🚨 <b>SHORT SIGNAL</b> — ${token}
🔥 Score: <b>{score}/10</b>
📋 Причины:
{reasons_text}
🛡️ Управление рисками:
• Stop Loss: {rm['stop_loss']:.4f}
• Take Profit: {rm['take_profit']:.4f}
• ATR: {rm['atr']:.4f}
🕒 {datetime.now().strftime('%H:%M:%S')}
🔗 https://www.bybit.com/trade/perpetual/{symbol}
"""
            for user in subscribers:
                try:
                    await bot.send_message(user, text, parse_mode="HTML", disable_web_page_preview=True)
                except Exception as e:
                    logger.warning(f"Failed to send message to {user}: {e}")

        logger.info(f"✅ Signals sent: {len(signals)}")
    except Exception as e:
        logger.error(f"Scan error: {e}")

# ===================== MAIN =====================
async def main():
    load_data()
    logger.info("🚀 Bot started")
    await bot.delete_webhook(drop_pending_updates=True)

    exchange, session = await init_exchange()

    scheduler = AsyncIOScheduler()
    scheduler.add_job(scan_market, "interval", minutes=CHECK_INTERVAL, args=[exchange])
    scheduler.start()

    # Запускаем первое сканирование с задержкой
    async def delayed_scan():
        await asyncio.sleep(2)
        await scan_market(exchange)
    asyncio.create_task(delayed_scan())

    try:
        await dp.start_polling(bot)
    finally:
        await session.close()

if __name__ == "__main__":
    asyncio.run(main())

