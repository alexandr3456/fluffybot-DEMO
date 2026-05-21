import asyncio
import ccxt
import pandas as pd
from datetime import datetime
from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties
from dotenv import load_dotenv
import os
import logging

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger(__name__)

load_dotenv()

# ========================= НАСТРОЙКИ =========================
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")

SCAN_INTERVAL = int(os.getenv("SCAN_INTERVAL", 300))

# Проверка обязательных переменных
if not TELEGRAM_TOKEN:
    raise ValueError("❌ TELEGRAM_TOKEN missing")


# Параметры качества сигналов
MIN_24H_VOLUME_USD = 800_000
PRICE_PUMP_5M = 7.0
PRICE_PUMP_15M = 12.0
VOLUME_SPIKE = 3.8
RSI_THRESHOLD = 73
RSI_THRESHOLD_15M = 70
MIN_FUNDING_RATE = 0.0001

# Инициализация биржи с асинхронной поддержкой
exchange = ccxt.bybit({
    'enableRateLimit': True,
    'options': {'defaultType': 'future'}
})

# Новый способ задания parse_mode в aiogram 3.7+
bot = Bot(
    token=TELEGRAM_TOKEN,
    default=DefaultBotProperties(parse_mode="HTML")
)
dp = Dispatcher()  # Инициализация Dispatcher

def rsi(series, period=14):
    """Простой расчёт RSI"""
    delta = series.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    rs = gain / loss
    return 100 - (100 / (1 + rs))

async def get_symbols():
    try:
        markets = await exchange.load_markets()  # Асинхронный вызов
        symbols = [s for s, m in markets.items()
                   if m.get('active') and m.get('quote') == 'USDT' and m.get('type') == 'swap']
        return symbols[:350]
    except Exception as e:
        logger.error(f"Error loading markets: {e}")
        return []

async def fetch_ohlcv(symbol, timeframe, limit=100):
    try:
        ohlcv = await exchange.fetch_ohlcv(symbol, timeframe, limit=limit)
        df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
        return df
    except Exception as e:
        logger.error(f"fetch_ohlcv error for {symbol}: {e}")
        return None

async def get_funding_rate(symbol):
    try:
        funding = await exchange.fetch_funding_rate(symbol)
        return funding.get('fundingRate', 0)
    except Exception as e:
        logger.error(f"get_funding_rate error for {symbol}: {e}")
        return 0

async def check_symbol(symbol):
    try:
        # Добавляем задержку между запросами
        await asyncio.sleep(0.1)

        df5 = await fetch_ohlcv(symbol, '5m', 80)
        df15 = await fetch_ohlcv(symbol, '15m', 60)

        if df5 is None or df15 is None or len(df5) < 40:
            return None

        price = df5['close'].iloc[-1]
        price_5m_ago = df5['close'].iloc[-2]
        price_15m_ago = df15['close'].iloc[-2]

        change_5m = (price - price_5m_ago) / price_5m_ago * 100
        change_15m = (price - price_15m_ago) / price_15m_ago * 100

        # Volume spike
        avg_vol = df5['volume'].rolling(20).mean().iloc[-1]
        current_vol = df5['volume'].iloc[-1]
        vol_ratio = current_vol / avg_vol if avg_vol > 0 else 0

        # RSI
        rsi5 = rsi(df5['close']).iloc[-1]
        rsi15 = rsi(df15['close']).iloc[-1]

        # 24h volume
        ticker = await exchange.fetch_ticker(symbol)  # Асинхронный вызов
        volume_24h = ticker.get('quoteVolume', 0)

        funding = await get_funding_rate(symbol)

        if (change_5m >= PRICE_PUMP_5M and
            change_15m >= PRICE_PUMP_15M and
            vol_ratio >= VOLUME_SPIKE and
            rsi5 >= RSI_THRESHOLD and
            rsi15 >= RSI_THRESHOLD_15M and
            volume_24h >= MIN_24H_VOLUME_USD and
            funding >= MIN_FUNDING_RATE):

            return {
                'symbol': symbol.replace('/USDT', '').replace('USDT', ''),
                'price': price,
                'pump_5m': round(change_5m, 2),
                'pump_15m': round(change_15m, 2),
                'vol_ratio': round(vol_ratio, 2),
                'rsi5': round(rsi5, 1),
                'funding': round(funding * 10000, 2),  # Конвертируем в базисные пункты
                'volume_24h': f"{volume_24h/1_000_000:.1f}M",
                'time': datetime.now().strftime("%H:%M")
            }
    except Exception as e:
        logger.error(f"check_symbol error for {symbol}: {e}")
    return None

async def scanner():
    print("🚀 Bybit Short Pump Scanner запущен...")
    while True:
        try:
            symbols = await get_symbols()
            tasks = [check_symbol(sym) for sym in symbols]
            results = await asyncio.gather(*tasks)

            for signal in [r for r in results if r]:
                text = f"""<b>🔴 ШОРТ СИГНАЛ — СИЛЬНЫЙ ПАМП</b>

🔹 <b>{signal['symbol']}USDT</b>
💰 Цена: <b>${signal['price']:.4f}</b>
📈 Рост 5м: <b>+{signal['pump_5m']}%</b>
📈 Рост 15м: <b>+{signal['pump_15m']}%</b>
📊 Volume spike: <b>x{signal['vol_ratio']}</b>
📉 RSI 5m: <b>{signal['rsi5']}</b>

💸 Funding: <b>+{signal['funding']}</b> ‱
📊 24h Vol: <b>${signal['volume_24h']}</b>

🕒 {signal['time']} | Bybit Perpetual"""

                

        except Exception as e:
            logger.error(f"Ошибка сканирования: {e}")

        logger.info(f"🔄 Следующий скан через {SCAN_INTERVAL} секунд...")
        await asyncio.sleep(SCAN_INTERVAL)

if __name__ == "__main__":
    try:
        asyncio.run(scanner())
    except KeyboardInterrupt:
        logger.info("🛑 Сканер остановлен пользователем")
    except Exception as e:
        logger.critical(f"Критическая ошибка: {e}")
