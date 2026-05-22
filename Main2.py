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

# Инициализация биржи с настройками таймаутов
exchange = ccxt.bybit({
    'enableRateLimit': True,
    'options': {'defaultType': 'swap'},
    'timeout': 30000,  # 30 секунд таймаут
    'rateLimit': 2000,   # 2 секунды между запросами
})

# Новый способ задания parse_mode в aiogram 3.7+
bot = Bot(
    token=TELEGRAM_TOKEN,
    default=DefaultBotProperties(parse_mode="HTML")
)
dp = Dispatcher()  # Инициализация Dispatcher
@dp.message()
async def handle_start(message):
    """Обрабатываем команду /start и запоминаем ID чата"""
    if message.text == '/start':
        # Сохраняем ID чата пользователя
        global TELEGRAM_CHAT_ID
        TELEGRAM_CHAT_ID = message.chat.id
        await message.answer(
            "✅ Бот запущен! Теперь вы будете получать сигналы о сильных пампах.\n\n"
            "Сканер запустится автоматически."
        )
        logger.info(f"Запомнен ID чата: {TELEGRAM_CHAT_ID}")

TELEGRAM_CHAT_ID = None
def rsi(series, period=14):
    """Простой расчёт RSI"""
    delta = series.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    rs = gain / loss
    return 100 - (100 / (1 + rs))

async def get_symbols():
    try:
        markets = exchange.load_markets()  # Синхронный вызов
        symbols = [
            s for s, m in markets.items()
            if m.get('active') and m.get('quote') == 'USDT' and m.get('type') == 'swap'
        ]
        return symbols[:350]
    except Exception as e:
        logger.error(f"Error loading markets: {e}")
        return []

def fetch_ohlcv(symbol, timeframe, limit=100):
    try:
        ohlcv = exchange.fetch_ohlcv(symbol, timeframe, limit=limit)
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

        # Синхронные вызовы fetch_ohlcv
        df5 = fetch_ohlcv(symbol, '5m', 80)
        df15 = fetch_ohlcv(symbol, '15m', 60)

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

        # RSI — синхронные вызовы
        rsi5_series = rsi(df5['close'], period=14)
        rsi15_series = rsi(df15['close'], period=14)
        rsi5 = rsi5_series.iloc[-1] if not rsi5_series.empty else 0
        rsi15 = rsi15_series.iloc[-1] if not rsi15_series.empty else 0

        # 24h volume — синхронный вызов (убрали await)
        ticker = exchange.fetch_ticker(symbol)  # Без await!
        volume_24h = ticker.get('quoteVolume', 0)

        # Асинхронный вызов для funding rate
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
            # Ждём, пока не будет установлен ID чата
            if TELEGRAM_CHAT_ID is None:
                logger.info("Ожидание команды /start...")
                await asyncio.sleep(5)
                continue

            symbols = await get_symbols()
            logger.info(f"Найдено {len(symbols)} символов для сканирования")

            batch_size = 20
            all_signals = []

            for i in range(0, len(symbols), batch_size):
                batch = symbols[i:i + batch_size]
                logger.info(f"Обрабатывается батч {i//batch_size + 1} из {(len(symbols) + batch_size - 1) // batch_size}")

                tasks = [check_symbol(sym) for sym in batch]
                results = await asyncio.gather(*tasks, return_exceptions=True)

                valid_results = [
                    result for result in results
            if isinstance(result, dict) and result is not None
                ]
                all_signals.extend(valid_results)
                await asyncio.sleep(2)

            sent_count = 0
            for signal in all_signals:
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

                try:
                    await bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=text)
                    sent_count += 1
            logger.info(f"✅ Сигнал отправлен для {signal['symbol']}")
                except Exception as e:
                    logger.error(f"❌ Ошибка отправки в Telegram для {signal['symbol']}: {e}")

            logger.info(f"📊 Найдено сигналов: {len(all_signals)}, отправлено: {sent_count}")

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
