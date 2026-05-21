import asyncio
import ccxt
import pandas as pd
from datetime import datetime
from aiogram import Bot
from dotenv import load_dotenv
import os

load_dotenv()

# ========================= НАСТРОЙКИ =========================
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
SCAN_INTERVAL = int(os.getenv("SCAN_INTERVAL", 300))

# Параметры качества сигналов
MIN_24H_VOLUME_USD = 800_000
PRICE_PUMP_5M = 7.0
PRICE_PUMP_15M = 12.0
VOLUME_SPIKE = 3.8
RSI_THRESHOLD = 73
RSI_THRESHOLD_15M = 70
MIN_FUNDING_RATE = 0.0001

exchange = ccxt.bybit({
    'enableRateLimit': True,
    'options': {'defaultType': 'future'}
})

bot = Bot(token=TELEGRAM_TOKEN, parse_mode="HTML")


def rsi(series, period=14):
    """Простой расчёт RSI без дополнительных библиотек"""
    delta = series.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    rs = gain / loss
    return 100 - (100 / (1 + rs))


async def get_symbols():
    markets = exchange.load_markets()
    symbols = [s for s, m in markets.items() 
               if m.get('active') and m.get('quote') == 'USDT' and m.get('type') == 'swap']
    return symbols[:350]


def fetch_ohlcv(symbol, timeframe, limit=100):
    try:
        ohlcv = exchange.fetch_ohlcv(symbol, timeframe, limit=limit)
        df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
        return df
    except:
        return None


async def get_funding_rate(symbol):
    try:
        funding = exchange.fetch_funding_rate(symbol)
        return funding.get('fundingRate', 0)
    except:
        return 0


async def check_symbol(symbol):
    try:
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

        # RSI
        rsi5 = rsi(df5['close']).iloc[-1]
        rsi15 = rsi(df15['close']).iloc[-1]

        # 24h volume
        ticker = exchange.fetch_ticker(symbol)
        volume_24h = ticker.get('quoteVolume', 0)

        funding = await get_funding_rate(symbol)

        # === КАЧЕСТВЕННЫЙ СИГНАЛ ===
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
                'funding': round(funding * 10000, 2),
                'volume_24h': f"{volume_24h/1_000_000:.1f}M",
                'time': datetime.now().strftime("%H:%M")
            }
    except:
        pass
    return None


async def scanner():
    print("🚀 Качественный Bybit Short Pump Scanner запущен...")
    while True:
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

💸 Funding: <b>+{signal['funding']}</b>
📊 24h Vol: <b>${signal['volume_24h']}</b>

🕒 {signal['time']} | Bybit"""

            await bot.send_message(TELEGRAM_CHAT_ID, text)
            print(f"✅ Сигнал: {signal['symbol']}")

        await asyncio.sleep(SCAN_INTERVAL)


if __name__ == "__main__":
    asyncio.run(scanner())
