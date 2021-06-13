#!/usr/bin/env python
# pylint: disable=C0116

# TODO help
# TODO stem chart
# TODO 72hr
# TODO 1h 24h 7d
# TODO persistent

from decimal import Decimal
import datetime
import functools
import json
import logging
import pprint
import pytz
import re
import sqlite3
import time
import traceback

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

def req(url, **kw):
    time.sleep(0.1)
    r = requests.get(url, **kw)
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
    filters = {'key': 'momo-key', 'bond': 'barnbridge', 'bunny': 'pancake-bunny', 'ust': 'terrausd'}
    coins = req('https://api.coingecko.com/api/v3/coins/list')
    coins = [c for c in coins if c['id'] == filters.get(c['symbol'], c['id'])]
    ctx.bot_data['coins'] = coins


def get_coin_id(symbol, ctx):
    coins = [c for c in ctx.bot_data['coins'] if c['symbol'] == symbol]
    if len(coins) > 1:
        raise ValueError(f"more than 1 coin with symbol {symbol} {coins}")
    elif len(coins) == 0:
        raise ValueError(f"coin not found with symbol {symbol}")
    return coins[0]['id']

def get_token_address(symbol, ctx):
    # XXX fixed protocol
    if symbol in ctx.bot_data.setdefault('token_address', {}):
        return ctx.bot_data['token_address'][symbol]
    coin_id = get_coin_id(symbol, ctx)
    address = req(f'https://api.coingecko.com/api/v3/coins/{coin_id}')['platforms']['ethereum']
    ctx.bot_data['token_address'][symbol] = address
    return address

@functools.lru_cache()
def get_1inch_price(address, ttl=None):
    usdc_address = '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'
    usdc_amount = '10000000000'
    r = req(f"https://api.1inch.exchange/v3.0/1/quote?fromTokenAddress={usdc_address}&toTokenAddress={address}&amount={usdc_amount}")
    fromAmount = float(r.fromTokenAmount)*10**-r.fromToken.decimals
    toAmount = float(r.toTokenAmount)*10**-r.toToken.decimals
    return fromAmount / toAmount

def get_price(exchange, symbol, ctx):
    if exchange == '1inch':
        address = get_token_address(symbol, ctx)
        ttl = int(time.time() / 60)
        return get_1inch_price(address, ttl)
    else:
        raise ValueError(f"unsupported exchange {exchange}")

def price_alert(ctx: CallbackContext):
    for k, v in ctx.bot_data['price_alert'].items():
        logger.info(f"{k} {v}")
        try:
            exchange, symbol, direction0, trigger_price, chat_id = k
        except:
            continue
        now = normtz(datetime.datetime.utcnow())
        intv = datetime.timedelta(seconds=3600)
        price = get_price(exchange, symbol, ctx)
        direction = -1 if direction0 == '<' else 1
        trigger_price = float(trigger_price)
        last_notified = normtz(datetime.datetime.fromisoformat(v.setdefault('last_notified', (now-intv*2).isoformat())))
        last_price = v.setdefault('last_price', price)
        ctx.bot_data['price_alert'][k]['last_price'] = price
        logger.info(f"{exchange} {symbol} {direction} {trigger_price} {chat_id} {price} {last_notified} {last_price}")
        if now < last_notified + intv:
            logger.info(f"skip last notified too close")
            continue
        if (price - trigger_price) * direction > 0:
            msg = f'{symbol} on {exchange} price {price:.4f} {direction0} {trigger_price}'
            logger.info(msg)
            ctx.bot.send_message(chat_id, msg)
            ctx.bot_data['price_alert'][k]['last_notified'] = now.isoformat()

def price_alert_command(update: Update, ctx: CallbackContext) -> None:
    logger.info(f"{update.message.chat.id} {update.message.chat.username} {update.message.text}")
    try:
        exchange, symbol, direction, trigger_price = update.message.text.split()[1:]
        chat_id = update.message.chat_id
        ctx.bot_data['price_alert'][(exchange, symbol, direction, trigger_price, chat_id)] = {}
        update.message.reply_text('ok')
    except:
        update.message.reply_text('error')
        traceback.print_exc()

def list_price_alert_command(update: Update, ctx: CallbackContext) -> None:
    logger.info(f"{update.message.chat.id} {update.message.chat.username} {update.message.text}")
    try:
        update.message.reply_text(pprint.pformat(ctx.bot_data['price_alert'], width=40))
    except:
        update.message.reply_text('error')
        traceback.print_exc()

def clear_price_alert_command(update: Update, ctx: CallbackContext) -> None:
    logger.info(f"{update.message.chat.id} {update.message.chat.username} {update.message.text}")
    try:
        ctx.bot_data['price_alert'] = {}
        update.message.reply_text('ok')
    except:
        update.message.reply_text('error')
        traceback.print_exc()

def delete_price_alert_command(update: Update, ctx: CallbackContext) -> None:
    logger.info(f"{update.message.chat.id} {update.message.chat.username} {update.message.text}")
    try:
        exchange, symbol, direction, trigger_price = update.message.text.split()[1:]
        chat_id = update.message.chat_id
        ctx.bot_data['price_alert'].pop((exchange, symbol, direction, trigger_price, chat_id))
        update.message.reply_text('ok')
    except:
        update.message.reply_text('error')
        traceback.print_exc()


def get_price_command(update: Update, ctx: CallbackContext) -> None:
    logger.info(f"{update.message.chat.id} {update.message.chat.username} {update.message.text}")
    try:
        symbol = update.message.text.split()[1]
        exchange = '1inch' # XXX
        price = get_price(exchange, symbol, ctx)
        update.message.reply_text(f'price {exchange} {symbol} {price:.4f}')
    except:
        update.message.reply_text('error')
        logger.error(traceback.format_exc())


def get_binanceu_funding(start=None, end=None):
    params = {'limit': 1000}
    if start:
        params['startTime'] = int(start.timestamp()) * 1000
    if end:
        params['endTime'] = int(end.timestamp()) * 1000
    for data in req(f"https://fapi.binance.com/fapi/v1/fundingRate", params=params):
        if not data['symbol'].endswith('USDT'):
            continue
        symbol = data['symbol'].replace('USDT', '')
        timestamp = normtz(datetime.datetime.fromtimestamp(data['fundingTime'] // 1000))
        apr = float(data['fundingRate']) * 3 * 365
        yield (symbol, timestamp, apr)

def get_binancec_funding(start=None, end=None):
    for info in req('https://dapi.binance.com/dapi/v1/exchangeInfo')['symbols']:
        if not info['symbol'].endswith('USD_PERP'):
            continue
        symbol = info['symbol'].replace('USD_PERP', '')
        params = {'limit': 1000}
        if start:
            params['startTime'] = int(start.timestamp()) * 1000
        if end:
            params['endTime'] = int(end.timestamp()) * 1000
        for data in req(f"https://dapi.binance.com/dapi/v1/fundingRate?symbol={info['symbol']}", params=params):
            timestamp = normtz(datetime.datetime.fromtimestamp(data['fundingTime'] // 1000))
            apr = float(data['fundingRate']) * 3 * 365
            yield (symbol, timestamp, apr)

def get_ftx_funding(start=None, end=None):
    params = {}
    if start:
        params['start_time'] = int(start.timestamp())
    if end:
        params['end_time'] = int(end.timestamp())
    for data in req(f"https://ftx.com/api/funding_rates", params=params)['result']:
        if not data['future'].endswith('-PERP'):
            continue
        symbol = data['future'].replace('-PERP', '')
        timestamp = normtz(datetime.datetime.fromisoformat(data['time']))
        apr = float(data['rate']) * 24 * 365
        yield (symbol, timestamp, apr)

def get_dydx_funding(start=None, end=None):
    for market in req(f"https://api.dydx.exchange/v3/markets").markets:
        if not market.endswith('-USD'):
            continue
        symbol = market.replace('-USD', '')
        params = {}
        if end:
            params['effectiveBeforeOrAt'] = end.isoformat()
        for data in req(f"https://api.dydx.exchange/v3/historical-funding/{market}", params=params).historicalFunding:
            timestamp = normtz(datetime.datetime.fromisoformat(data.effectiveAt.replace('Z', '+00:00')))
            apr = float(data.rate) * 24 * 365
            yield (symbol, timestamp, apr)

FUNDING_EXCHANGES = {
    'binanceu': get_binanceu_funding,
    'binancec': get_binancec_funding,
    'ftx': get_ftx_funding,
    'dydx': get_dydx_funding,
}

def normtz(dt):
    if dt and not dt.tzinfo:
        return dt.replace(tzinfo=pytz.utc)
    return dt

def update_exchange_funding(exchange, start=None, end=None):
    ts_min = None
    ts_max = None
    count = 0
    total = 0
    con = sqlite3.connect('apebot.sqlite3')
    cur = con.cursor()
    try:
        cur.execute('create table funding (exchange text, symbol text, time timestamp, apr real, unique(exchange, symbol, time))')
        con.commit()
    except:
        pass
    while 1:
        logger.info(f"updating funding {exchange} start {start} end {end}")
        ts_min0 = ts_min
        ts_max0 = ts_max
        count = 0
        for symbol, timestamp, apr in FUNDING_EXCHANGES[exchange](start, end):
            ts_min = min(ts_min, timestamp) if ts_min else timestamp
            ts_max = max(ts_max, timestamp) if ts_max else timestamp
            try:
                values = (exchange, symbol, timestamp.isoformat(), apr)
                cur.execute('insert into funding values (?, ?, ?, ?)', values)
                count += 1
            except sqlite3.IntegrityError:
                pass
            except:
                traceback.print_exc()
        con.commit()
        logger.info(f"updated funding {exchange} {count} entries min timestamp {ts_min} max timestamp {ts_max}")
        total += count
        ts_min0 = normtz(ts_min0)
        ts_max0 = normtz(ts_max0)
        ts_min = normtz(ts_min)
        ts_max = normtz(ts_max)
        start = normtz(start)
        end = normtz(end)
        if ts_min0 == ts_min and ts_max0 == ts_max:
            break
        elif ts_min <= start and end <= ts_max:
            break
        elif ts_min <= start and ts_max < end:
            start = ts_max
        elif start < ts_min and end <= ts_max:
            end = ts_min
        else:
            break
    return total, ts_min, ts_max

def update_funding(ctx: CallbackContext):
    for exchange in FUNDING_EXCHANGES:
        try:
            update_exchange_funding(exchange)
        except:
            pass

def escape(s, chars):
    for c in chars:
        s = s.replace(c, '\\' + c)
    return s

def update_funding_command(update: Update, ctx: CallbackContext) -> None:
    logger.info(update.message.text)
    args = update.message.text.split()[1:]
    exchanges = [i for i in args if i in FUNDING_EXCHANGES] or FUNDING_EXCHANGES.keys()
    start = None
    end = None
    iter_args = iter(args)
    for arg in iter_args:
        try:
            start = normtz(datetime.datetime.fromisoformat(arg))
            break
        except:
            pass
    for arg in iter_args:
        try:
            end = normtz(datetime.datetime.fromisoformat(arg))
            break
        except:
            pass
    for exchange in exchanges:
        count, ts_min, ts_max = update_exchange_funding(exchange, start, end)
        ts_min = escape(str(ts_min), '-.+')
        ts_max = escape(str(ts_max), '-.+')
        message = f"total updated funding {exchange} {count} entries min timestamp {ts_min} max timestamp {ts_max}"
        logger.info(message)
        update.message.reply_text(message, parse_mode=telegram.ParseMode.MARKDOWN_V2)

def funding_command2(update: Update, ctx: CallbackContext) -> None:
    logger.info(update.message.text)
    whitelist = [i.upper() for i in ctx.bot_data['whitelist']]
    args = update.message.text.split()[1:]
    exchanges = [i for i in args if i in FUNDING_EXCHANGES] or FUNDING_EXCHANGES.keys()
    symbols = [i.upper() for i in args if i not in FUNDING_EXCHANGES and not re.match(r'\d', i)] or whitelist
    days = [i.replace('d', ' day') for i in args if re.match(r'\d+d', i)] or ['1 day']
    rates = []
    con = sqlite3.connect('apebot.sqlite3')
    cur = con.cursor()
    for day in days:
        E = ("exchange in (%s) and " % ','.join('\''+i+'\'' for i in exchanges)) if exchanges else ''
        S = ("symbol in (%s) and " % ','.join('\''+i+'\'' for i in symbols)) if symbols else ''
        sql = f"select exchange, symbol, avg(apr) from funding where {E} {S} time >= datetime('now', '{'-'+day}') group by exchange, symbol order by avg(apr) desc limit 10"
        logger.info(sql)
        for exchange, symbol, rate in cur.execute(sql).fetchall():
            rates.append(ad(exchange=exchange, symbol=symbol, rate=rate))
    rows = []
    for r in rates:
        rows.append((r.symbol.upper(), r.exchange, f"{round(r.rate * 100, 2)}%"))
    w = [max(len(r) for r in c) for c in zip(*rows)]
    message = f'{days[0]} funding rates: \(APR\)\n'
    message += '\n'.join(f'`{r[0]:{w[0]}} {r[1]:{w[1]}} {r[2]:>{w[2]}}`' for r in rows)
    update.message.reply_text(message, parse_mode=telegram.ParseMode.MARKDOWN_V2)


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
    dispatcher.add_handler(CommandHandler("update_funding", update_funding_command))
    dispatcher.add_handler(CommandHandler("ff", funding_command2))
    dispatcher.add_handler(CommandHandler("price_alert", price_alert_command))
    dispatcher.add_handler(CommandHandler("p", get_price_command))
    dispatcher.add_handler(CommandHandler("list_price_alert", list_price_alert_command))
    dispatcher.add_handler(CommandHandler("clear_price_alert", clear_price_alert_command))
    dispatcher.add_handler(CommandHandler("delete_price_alert", delete_price_alert_command))
    updater.job_queue.run_repeating(update_markets, interval=3600, first=1) # 1h
    updater.job_queue.run_repeating(update_funding, interval=300, first=1) # 5m
    updater.job_queue.run_repeating(price_alert, interval=60, first=1) # 1m
    #disabled
    #updater.job_queue.run_repeating(order_alert, interval=60, first=1) # 1m
    updater.start_polling()
    updater.idle()


if __name__ == '__main__':
    main()

