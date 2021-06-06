#!/usr/bin/env python
# pylint: disable=C0116

# TODO help
# TODO stem chart
# TODO 72hr
# TODO 1h 24h 7d
# TODO persistent

from decimal import Decimal
import argparse
import datetime
import json
import logging
import sqlite3
import time

from attrdict import AttrDict
from bscscan import BscScan
from telegram import Update, ForceReply
from telegram.ext import Updater, CommandHandler, MessageHandler, Filters, CallbackContext, PicklePersistence
import ccxt
import requests
import telegram

def ad(obj={}, **kw):
    if kw:
        obj.update(kw)
    if isinstance(obj, dict):
        return AttrDict(obj)
    elif isinstance(obj, list):
        return [AttrDict(i) for i in obj]
    else:
        return obj

def req(url):
    time.sleep(0.1)
    r = requests.get(url)
    logger.info(f"GET {url} {r.status_code}")
    return ad(r.json())

def yesterday():
    return datetime.datetime.now() - datetime.timedelta(days=1)


logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)
config = ad(json.load(open("config.json")))
bsc = BscScan(config.bscscan.token)


def ftx_funding(symbol, whitelist):
    if symbol:
        r = req(f"https://ftx.com/api/funding_rates?future={symbol.upper()}-PERP")
        if r.success:
            rate = sum(float(i.rate) for i in r.result[:24]) * 365
            return [ad(exchange='ftx', symbol=symbol, rate=rate)]
        else:
            return []
    else:
        r = req(f"https://ftx.com/api/funding_rates")
        if r.success:
            samples = {}
            t = yesterday().isoformat()
            for i in r.result:
                if i.time > t and i.future.endswith('-PERP'):
                    symbol = i.future.replace('-PERP', '').lower()
                    samples.setdefault(symbol, []).append(i.rate)
            rates = []
            for symbol in samples:
                if symbol not in whitelist:
                    logger.info(f"skip {symbol}")
                    continue
                rate = sum(samples[symbol]) / len(samples[symbol]) * 24 * 365
                rates.append(ad(exchange='ftx', symbol=symbol, rate=rate))
            top = sorted(rates, key=lambda i:-i.rate)[:10]
            rates = []
            for i in top:
                rates += ftx_funding(i.symbol, whitelist)
            return rates
        else:
            return []

def binance_funding(symbol, whitelist):
    if symbol:
        r = req(f"https://fapi.binance.com/fapi/v1/fundingRate?symbol={symbol.upper()}USDT")
        if r:
            rate = sum(float(i['fundingRate']) for i in r[-3:]) * 365
            return [ad(exchange='binance', symbol=symbol, rate=rate)]
        else:
            return []
    else:
        r = req(f"https://fapi.binance.com/fapi/v1/fundingRate?limit=1000")
        if r:
            samples = {}
            t = yesterday().timestamp() * 1000
            for i in r:
                if i.fundingTime > t and i.symbol.endswith('USDT'):
                    symbol = i.symbol.replace('USDT', '').lower()
                    samples.setdefault(symbol, []).append(float(i.fundingRate))
            rates = []
            for symbol in samples:
                if symbol not in whitelist:
                    logger.info(f"skip {symbol}")
                    continue
                rate = sum(samples[symbol]) / len(samples[symbol]) * 3 * 365
                rates.append(ad(exchange='binance', symbol=symbol, rate=rate))
            top = sorted(rates, key=lambda i:-i.rate)[:10]
            rates = []
            for i in top:
                rates += binance_funding(i.symbol, whitelist)
            return rates
        else:
            return []


DYDXL1_MARKETS = {'btc':'PBTC-USDC', 'eth':'WETH-PUSD'}
def dydxL1_funding(symbol, whitelist):
    if symbol:
        symbols = [symbol] if symbol in DYDXL1_MARKETS else []
    else:
        symbols = ['btc', 'eth']
    r = req(f"https://api.dydx.exchange/v1/historical-funding-rates")
    rates = []
    for symbol in symbols:
        market = DYDXL1_MARKETS[symbol]
        rate = sum(float(i.fundingRate8Hr) for i in ad(r[market]).history[:24])/24*3 * 365
        rates.append(ad(exchange='dydxL1', symbol=symbol, rate=rate))
    return rates


def dydx_funding(symbol, whitelist):
    r = req(f"https://api.dydx.exchange/v3/markets")
    markets = {i['baseAsset'].lower(): m for m, i in r.markets.items()}
    if symbol:
        symbols = [symbol] if symbol in markets else []
    else:
        symbols = list(markets.keys())
    rates = []
    for symbol in symbols:
        r = req(f"https://api.dydx.exchange/v3/historical-funding/{markets[symbol]}")
        rate = sum(float(i.rate) for i in r.historicalFunding[:24]) * 365
        rates.append(ad(exchange='dydx', symbol=symbol, rate=rate))
    return rates


EXCHANGES = {
    'ftx': ftx_funding,
    'binance': binance_funding,
    'dydx': dydx_funding,
    #'dydxL1': dydxL1_funding,
}


def funding_command(update: Update, ctx: CallbackContext) -> None:
    logger.info(update.message.text)
    args = update.message.text.split()[1:]
    exchanges = [i for i in args if i in EXCHANGES] or EXCHANGES.keys()
    symbols = [i for i in args if i not in EXCHANGES] or [None]
    rates = []
    for exchange in exchanges:
        for symbol in symbols:
            rates += EXCHANGES[exchange](symbol, ctx.bot_data['whitelist'])
    rates = sorted(rates, key=lambda r:-r.rate)[:10]
    rows = []
    for r in rates:
        rows.append((r.symbol.upper(), r.exchange, f"{round(r.rate * 100, 2)}%"))
    w = [max(len(r) for r in c) for c in zip(*rows)]
    message = '24hr funding rates: \(APR\)\n'
    message += '\n'.join(f'`{r[0]:{w[0]}} {r[1]:{w[1]}} {r[2]:>{w[2]}}`' for r in rows)
    update.message.reply_text(message, parse_mode=telegram.ParseMode.MARKDOWN_V2)


def apy_command(update: Update, _: CallbackContext) -> None:
    logger.info(f"{update.message.chat.id} {update.message.chat.username} {update.message.text}")
    apy = float(update.message.text.split()[1].replace('%','')) / 100
    apr = ((apy+1)**(1/365)-1)*365
    update.message.reply_text(f"{round(apy*100,2)}% APY = {round(apr*100,2)}% APR compound daily")


def update_markets(ctx: CallbackContext):
    markets = req('https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&per_page=50')
    ctx.bot_data['whitelist'] = {i.symbol.lower() for i in markets}


def price_alert(ctx: CallbackContext):
    con = sqlite3.connect('prices.sqlite3') # XXX
    cur = con.cursor()
    try:
        cur.execute('create table prices (time timestamp, symbol text, price real)')
        con.commit()
    except:
        pass

    # XXX
    symbol = 'alpacasalpaca'
    alpaca = int(bsc.get_acc_balance_by_token_contract_address('0x8f0528ce5ef7b51152a59745befdd91d97091d2f', '0x3Ee4de968E47877F432226d6a9A0DAD6EAc6001b')) / 10**18
    salpaca = int(bsc.get_acc_balance_by_token_contract_address('0x6f695bd5ffd25149176629f8491a5099426ce7a7', '0x3Ee4de968E47877F432226d6a9A0DAD6EAc6001b')) / 10**18
    price = alpaca / salpaca

    now = datetime.datetime.now()
    values = (now, symbol, price)
    cur.execute('insert into prices values (?, ?, ?)', values)
    con.commit()

    intv = datetime.timedelta(seconds=3600) # XXX
    values = (now - intv,)
    cur.execute('select price from prices where time > ? order by time limit 1', values)
    (oldprice,) = cur.fetchone()
    pricechange = (price - oldprice) / price
    msg = f'{symbol} price changed {pricechange*100:.2f}% to {price:.4f} in 1h' # XXX
    logger.info(msg)
    if abs(pricechange) > 0.01: # XXX
        chat_id = config.telegram.chat_id
        lastnotified = ctx.bot_data.setdefault('price_alert', {}).setdefault('lastnotified', {}).setdefault(chat_id, {}).setdefault(symbol, now - datetime.timedelta(days=1))
        if now > lastnotified + intv:
            ctx.bot.send_message(chat_id, msg)
            ctx.bot_data['price_alert']['lastnotified'][chat_id][symbol] = now


def get_funding_binanceu(start=None, end=None):
    for data in req(f"https://fapi.binance.com/fapi/v1/fundingRate?limit=1000"):
        if not data['symbol'].endswith('USDT'):
            continue
        symbol = data['symbol'].replace('USDT', '')
        timestamp = datetime.datetime.fromtimestamp(data['fundingTime'] // 1000)
        apr = float(data['fundingRate']) * 3 * 365
        yield (symbol, timestamp, apr)

def get_funding_binancec(start=None, end=None):
    for info in req('https://dapi.binance.com/dapi/v1/exchangeInfo')['symbols']:
        if not info['symbol'].endswith('USD_PERP'):
            continue
        symbol = info['symbol'].replace('USD_PERP', '')
        for data in req(f"https://dapi.binance.com/dapi/v1/fundingRate?symbol={info['symbol']}&limit=1000"):
            timestamp = datetime.datetime.fromtimestamp(data['fundingTime'] // 1000)
            apr = float(data['fundingRate']) * 3 * 365
            yield (symbol, timestamp, apr)

def get_funding_ftx(start=None, end=None):
    for data in req(f"https://ftx.com/api/funding_rates")['result']:
        if not data['future'].endswith('-PERP'):
            continue
        symbol = data['future'].replace('-PERP', '')
        timestamp = data['time']
        apr = float(data['rate']) * 24 * 365
        yield (symbol, timestamp, apr)

def get_funding_dydx(start=None, end=None):
    for market in req(f"https://api.dydx.exchange/v3/markets").markets:
        if not market.endswith('-USD'):
            continue
        symbol = market.replace('-USD', '')
        for data in req(f"https://api.dydx.exchange/v3/historical-funding/{market}").historicalFunding:
            timestamp = data.effectiveAt
            apr = float(data.rate) * 24 * 365
            yield (symbol, timestamp, apr)

FUNDING_EXCHANGES = {
    'binanceu': get_funding_binanceu,
    'binancec': get_funding_binancec,
    'ftx': get_funding_ftx,
    'dydx': get_funding_dydx,
}

def update_funding(ctx: CallbackContext):
    con = sqlite3.connect('apebot.sqlite3')
    cur = con.cursor()
    try:
        cur.execute('create table funding (exchange text, symbol text, time timestamp, apr real, unique(exchange, symbol, time))')
        con.commit()
    except:
        pass
    for exchange, get_funding in FUNDING_EXCHANGES.items():
        try:
            for symbol, timestamp, apr in get_funding():
                try:
                    values = (exchange, symbol, timestamp, apr)
                    cur.execute('insert into funding values (?, ?, ?, ?)', values)
                    con.commit()
                except:
                    pass
        except:
            pass


BINANCE_ORDERS_CONFIG = [
    {'market': 'spot',            'url': 'https://www.binance.com/bapi/capital/v1/private/streamer/order/get-trade-orders', 'headers': {'Referer': 'https://www.binance.com/en/my/orders/exchange/tradeorder'},               'data':{}},
    {'market': 'future usd-m',    'url': 'https://www.binance.com/bapi/futures/v1/private/future/order/order-history',      'headers': {'Referer': 'https://www.binance.com/en/my/orders/futures/orderhistory'},              'data':{}},
    {'market': 'future coin-m',   'url': 'https://www.binance.com/bapi/futures/v1/private/delivery/order/order-history',    'headers': {'Referer': 'https://www.binance.com/en/my/orders/futures/orderhistory'},              'data':{}},
    {'market': 'margin cross',    'url': 'https://www.binance.com/bapi/capital/v1/private/streamer/order/get-trade-orders', 'headers': {'Referer': 'https://www.binance.com/en/my/orders/margin/tradeorder/margin'},          'data':{"accountType":"MARGIN"}},
    {'market': 'margin isolated', 'url': 'https://www.binance.com/bapi/capital/v1/private/streamer/order/get-trade-orders', 'headers': {'Referer': 'https://www.binance.com/en/my/orders/margin/tradeorder/isolated_margin'}, 'data':{"accountType":"ISOLATED_MARGIN"}},
]

def binance_fetch_orders(since):
    raw_headers = open('binance_headers.txt').read()
    headers = dict(l.split(': ') for l in raw_headers.strip().splitlines())
    now = int(datetime.datetime.now().timestamp() * 1000)
    data = {'page': 1, 'rows': 15, 'startTime': since, 'endTime': now}
    results = []
    for c in BINANCE_ORDERS_CONFIG:
        r = requests.post(c['url'], headers=dict(headers, **c['headers']), json=dict(data, **c['data']))
        for order in r.json()['data']:
            order['_market'] = c['market']
            order['status'] = order['status'].lower()
            if order['status'] == 'filled':
                order['status'] = 'closed'
            order.setdefault('average', order.get('avgPrice', order.get('price')))
            order.setdefault('amount', order.get('origQty'))
            order.setdefault('filled', order.get('executedQty'))
            results.append(order)
    return results


def dec(num):
    return Decimal(num).quantize(Decimal('0.0001')).normalize()


# TODO retry
def order_alert(ctx: CallbackContext):
    chat_id = config.telegram.chat_id
    ftx = ccxt.ftx(config['ftx'])
    for exchange, fetch_orders in [('binance', binance_fetch_orders), ('ftx', ftx.fetch_orders)]:
        lastnotified = ctx.bot_data.setdefault('order_alert', {}).setdefault('lastnotified', {}).setdefault(chat_id, {}).setdefault(exchange, int(datetime.datetime.now().timestamp() * 1000) - 86400000)
        #lastnotified = int(datetime.datetime.now().timestamp() * 1000) - 86400000*4
        orders = fetch_orders(since=lastnotified)
        for order in orders:
            order = ad(order)
            if order.status == 'closed':
                logger.info(order)
                msg = f'{exchange} {order.symbol} {order.type} {order.side} {float(order.filled):.4f} @ {float(order.average):.4f}'
                ctx.bot.send_message(config.telegram.chat_id, msg)
        now = int(datetime.datetime.now().timestamp() * 1000)
        ctx.bot_data['order_alert']['lastnotified'][chat_id][exchange] = now


def main() -> None:
    persistence = PicklePersistence('apebot.pickle')
    updater = Updater(config.telegram.token, persistence=persistence)
    dispatcher = updater.dispatcher
    dispatcher.add_handler(CommandHandler("funding", funding_command))
    dispatcher.add_handler(CommandHandler("f", funding_command))
    dispatcher.add_handler(CommandHandler("apy", apy_command))
    updater.job_queue.run_repeating(update_markets, interval=3600, first=1) # 1h
    #updater.job_queue.run_repeating(price_alert, interval=300, first=1) # 5m
    updater.job_queue.run_repeating(update_funding, interval=300, first=1) # 5m
    #updater.job_queue.run_repeating(order_alert, interval=60, first=1) # 1m
    updater.start_polling()
    updater.idle()


if __name__ == '__main__':
    main()

