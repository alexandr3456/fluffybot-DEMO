import asyncio
import ccxt.async_support as ccxt
import pandas as pd
from datetime import datetime
from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties
from dotenv import load_dotenv
import os
import logging

# ========================= НАСТРОЙКИ =========================
load_dotenv()

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
SCAN_INTERVAL = int(os.getenv("SCAN_INTERVAL", 300))

MIN_24H_VOLUME_USD = 800_000
PRICE_PUMP_5M = 7.0
PRICE_PUMP_15M = 12.0
VOLUME_SPIKE = 3.8
RSI_THRESHOLD = 73
RSI_THRESHOLD_15M = 70
MIN_FUNDING_RATE = 0.0001

# ========================= ЛОГИРОВАНИЕ =========================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger(__name__)

if not TELEGRAM_TOKEN:
    raise ValueError("❌ TELEGRAM_TOKEN missing")

# ========================= ИНИЦИАЛИЗАЦИЯ =========================
bot = Bot(
    token=TELEGRAM_TOKEN,
    default=DefaultBotProperties(parse_mode="HTML")
)
dp = Dispatcher()

semaphore = asyncio.Semaphore(10)

exchange = ccxt.bybit({
    'enableRateLimit': True,
    'options': {'defaultType': 'swap'},
    'timeout': 30000,
})

TELEGRAM_CHAT_ID = None
SUBSCRIPTION_ACTIVE = False  # Флаг активности подписки

@dp.message()
async def handle_messages(message):
    global TELEGRAM_CHAT_ID, SUBSCRIPTION_ACTIVE

    if message.text.lower() == '/start':
        TELEGRAM_CHAT_ID = message.chat.id
        SUBSCRIPTION_ACTIVE = True
        await message.answer(
            "✅ <b>Bybit Short Pump Scanner успешно запущен!</b>\n\n"
            "Теперь вы будете получать сигналы."
        )
        logger.info(f"✅ Чат ID сохранён: {TELEGRAM_CHAT_ID}. Подписка активна")

    elif message.text.lower() == '/stop':
        SUBSCRIPTION_ACTIVE = False
        TELEGRAM_CHAT_ID = None
        await message.answer(
            "🛑 <b>Подписка отключена</b>\n\n"
            "Вы больше не будете получать сигналы. Для возобновления отправьте /start"
        )
        logger.info("🛑 Подписка отключена по команде /stop")

    else:
        await message.answer(
            "ℹ️ <b>Неизвестная команда</b>\n\n"
            "Доступные команды:\n"
            "/start — запустить подписку на сигналы\n"
            "/stop — отключить подписку"
        )
@dp.message()
async def handle_start(message):
    global TELEGRAM_CHAT_ID
    if message.text.lower() == '/start':
        TELEGRAM_CHAT_ID = message.chat.id
        await message.answer(
            "✅ <b>Bybit Short Pump Scanner успешно запущен!</b>\n\n"
            "Теперь вы будете получать сигналы."
        )
        logger.info(f"✅ Чат ID сохранён: {TELEGRAM_CHAT_ID}")


def calculate_rsi(series, period=14):
    delta = series.diff()
    gain = delta.where(delta > 0, 0).rolling(window=period).mean()
    loss = -delta.where(delta < 0, 0).rolling(window=period).mean()
    rs = gain / loss
    return 100 - (100 / (1 + rs))


async def get_symbols():
    try:
        markets = await exchange.load_markets()
        return [s for s, m in markets.items() 
                if m.get('active') and m.get('quote') == 'USDT' and m.get('type') == 'swap'][:400]
    except Exception as e:
        logger.error(f"Ошибка рынков: {e}")
        return []


async def fetch_ohlcv(symbol, timeframe, limit=100):
    async with semaphore:
        try:
            ohlcv = await exchange.fetch_ohlcv(symbol, timeframe, limit=limit)
            df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
            return df
        except:
            return None


async def check_symbol(symbol):
    async with semaphore:
        try:
            df5 = await fetch_ohlcv(symbol, '5m', 80)
            df15 = await fetch_ohlcv(symbol, '15m', 60)

            if not all([df5, df15]) or len(df5) < 40 or len(df15) < 30:
                return None

            price = df5['close'].iloc[-1]
            change_5m = (price - df5['close'].iloc[-2]) / df5['close'].iloc[-2] * 100
            change_15m = (price - df15['close'].iloc[-2]) / df15['close'].iloc[-2] * 100

            avg_vol = df5['volume'].rolling(20).mean().iloc[-1]
            vol_ratio = df5['volume'].iloc[-1] / avg_vol if avg_vol > 0 else 0

            rsi5 = calculate_rsi(df5['close']).iloc[-1]
            rsi15 = calculate_rsi(df15['close']).iloc[-1]

            ticker = await exchange.fetch_ticker(symbol)
            funding = await exchange.fetch_funding_rate(symbol)

            if (change_5m >= PRICE_PUMP_5M and change_15m >= PRICE_PUMP_15M and
                vol_ratio >= VOLUME_SPIKE and rsi5 >= RSI_THRESHOLD and
                rsi15 >= RSI_THRESHOLD_15M and ticker.get('quoteVolume', 0) >= MIN_24H_VOLUME_USD and
                funding.get('fundingRate', 0) >= MIN_FUNDING_RATE):

                return {
                    'symbol': symbol.replace('/USDT', ''),
                    'price': price,
                    'pump_5m': round(change_5m, 2),
                    'pump_15m': round(change_15m, 2),
                    'vol_ratio': round(vol_ratio, 2),
                    'rsi5': round(rsi5, 1),
                    'funding': round(funding.get('fundingRate', 0) * 10000, 2),
                    'volume_24h': f"{ticker.get('quoteVolume', 0)/1_000_000:.1f}M",
                    'time': datetime.now().strftime("%H:%M")
                }
        except:
            pass
        return None


async def scanner():
    global TELEGRAM_CHAT_ID
    logger.info("🚀 Scanner запущен...")

    while True:
        if TELEGRAM_CHAT_ID is None:
            await asyncio.sleep(5)
            continue

        try:
            symbols = await get_symbols()
            results = await asyncio.gather(*[check_symbol(s) for s in symbols], return_exceptions=True)
            signals = [r for r in results if isinstance(r, dict)]

            for signal in signals:
                text = f"""<b>🔴 ШОРТ СИГНАЛ — СИЛЬНЫЙ ПАМП</b>
🔹 <b>{signal['symbol']}USDT</b>
💰 Цена: <b>${signal['price']:.4f}</b>
📈 5м: <b>+{signal['pump_5m']}%</b> | 15м: <b>+{signal['pump_15m']}%</b>
📊 Volume: <b>x{signal['vol_ratio']}</b> | RSI5: <b>{signal['rsi5']}</b>
💸 Funding: <b>+{signal['funding']}</b> ‱ | Vol24: <b>${signal['volume_24h']}</b>
🕒 {signal['time']}"""

                await bot.send_message(TELEGRAM_CHAT_ID, text)
                logger.info(f"✅ Сигнал: {signal['symbol']}")

            logger.info(f"Сигналов найдено: {len(signals)}")
        except Exception as e:
            logger.error(f"Ошибка scanner: {e}")

        await asyncio.sleep(SCAN_INTERVAL)


async def main():
    """Запускаем и поллинг, и сканер одновременно"""
    logger.info("Запуск бота...")
    
    # Запускаем scanner в фоне
    asyncio.create_task(scanner())

    # Запускаем получение сообщений от Telegram
    await dp.start_polling(bot)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("🛑 Бот остановлен")
    except Exception as e:
        logger.critical(f"Критическая ошибка: {e}")
    finally:
        asyncio.run(exchange.close())
