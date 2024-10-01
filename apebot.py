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
import os
import pprint
import pytz
import re
import sqlite3
import time
import traceback

from attrdict import AttrDict
from bscscan import BscScan
from telegram import Update, ForceReply
from telegram.ext import Updater, CommandHandler, MessageHandler, Filters, CallbackContext, PicklePersistence, RegexHandler
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

def req(url, method='get', sleep=0.1, **kw):
    time.sleep(sleep)
    r = requests.request(method, url, **kw)
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
    #'ftx': ftx_funding,
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


def greed_command(update: Update, _: CallbackContext) -> None:
    logger.info(f"{update.message.chat.id} {update.message.chat.username} {update.message.text}")
    update.message.reply_photo(requests.get("https://alternative.me/crypto/fear-and-greed-index.png").content)


def update_markets(ctx: CallbackContext):
    markets = req('https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&per_page=100')
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
    if (symbol in ctx.bot_data.setdefault('token_address', {}) and
        symbol in ctx.bot_data.setdefault('token_decimals', {})):
        return ctx.bot_data['token_address'][symbol]
    coin_id = get_coin_id(symbol, ctx)
    address = req(f'https://api.coingecko.com/api/v3/coins/{coin_id}')['platforms']['ethereum']
    ctx.bot_data['token_address'][symbol] = address
    decimals = req(f"https://api.ethplorer.io/getTokenInfo/{address}?apiKey=freekey")['decimals']
    ctx.bot_data['token_decimals'][symbol] = int(decimals)
    return address

@functools.lru_cache()
def get_1inch_price(from_address, to_address, from_amount, ttl=None):
    #usdc_address = '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'
    r = req(f"https://api.1inch.exchange/v3.0/1/quote?fromTokenAddress={from_address}&toTokenAddress={to_address}&amount={from_amount}")
    fromAmount = float(r.fromTokenAmount)*10**-r.fromToken.decimals
    toAmount = float(r.toTokenAmount)*10**-r.toToken.decimals
    return fromAmount / toAmount

def get_price(exchange, to_symbol, ctx, from_symbol='usdc', from_amount=10000):
    if exchange == '1inch':
        to_address = get_token_address(to_symbol, ctx)
        from_address = get_token_address(from_symbol, ctx)
        from_amount *= 10**ctx.bot_data['token_decimals'][from_symbol]
        ttl = int(time.time() / 60)
        return get_1inch_price(from_address, to_address, from_amount, ttl)
    else:
        raise ValueError(f"unsupported exchange {exchange}")

def price_alert(ctx: CallbackContext):
    for k, v in ctx.bot_data.get('price_alert', {}).items():
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


def float_command(update: Update, ctx: CallbackContext) -> None:
    logger.info(f"{update.message.chat.id} {update.message.chat.username} {update.message.text}")
    try:
        exchange = '1inch' # XXX
        symbol = 'float'
        buy_price = get_price(exchange, symbol, ctx, 'usdc', 10000)
        sell_price = 1 / get_price(exchange, 'usdc', ctx, symbol, 6666)
        msg = '\n'.join([f'buy price {exchange} {symbol} {buy_price:.4f}',
                         f'sell price {exchange} {symbol} {sell_price:.4f}'])
        update.message.reply_text(msg)
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

def get_dydxv4_funding(start=None, end=None):
    for market in req(f"https://indexer.dydx.trade/v4/perpetualMarkets").markets:
        if not market.endswith('-USD'):
            continue
        symbol = market.replace('-USD', '')
        params = {}
        if end:
            params['effectiveBeforeOrAt'] = end.isoformat()
        for data in req(f"https://indexer.dydx.trade/v4/historicalFunding/{market}", params=params).historicalFunding:
            timestamp = normtz(datetime.datetime.fromisoformat(data.effectiveAt.replace('Z', '+00:00')))
            apr = float(data.rate) * 24 * 365
            yield (symbol, timestamp, apr)

def get_hyperliquid_funding(start=None, end=None):
    url = 'https://api.hyperliquid.xyz/info'
    start_time = int((datetime.datetime.now() - datetime.timedelta(days=20)).timestamp() * 1000)
    for coin in req(url, method='post', json={'type': 'meta'}).universe:
        symbol = coin.name
        for data in req(url, method='post', sleep=1, json={'type': 'fundingHistory', 'coin': symbol, 'startTime': start_time}):
            timestamp = normtz(datetime.datetime.utcfromtimestamp(data.time / 1000))
            apr = float(data.fundingRate) * 24 * 365
            yield (symbol, timestamp, apr)

def get_aevo_funding(start=None, end=None):
    # XXX not support start/end time
    get = lambda api, **args: req('https://api.aevo.xyz/' + api, sleep=0.5, method='get', params=args)
    for i in get('markets', instrument_type='PERPETUAL'):
        r = get('funding-history', instrument_name=i.instrument_name, limit=50)
        if 'funding_history' not in r:
            logger.error(f"{i.instrument_name} {r}")
            continue
        for s, t, f, p in r.funding_history:
            symbol = s.replace('-PERP', '')
            timestamp = normtz(datetime.datetime.utcfromtimestamp(int(t) / 1000000000))
            apr = float(f) * 24 * 365
            yield (symbol, timestamp, apr)

FUNDING_EXCHANGES = {
    'binanceu': get_binanceu_funding,
    'binancec': get_binancec_funding,
    #'ftx': get_ftx_funding,
    'dydx': get_dydx_funding,
    'dydxv4': get_dydxv4_funding,
    'hyperliquid': get_hyperliquid_funding,
    'aevo': get_aevo_funding,
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
        cur.execute('create table if not exists funding (exchange text, symbol text, time timestamp, apr real, unique(exchange, symbol, time))')
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
        start = start or ts_min
        end = end or ts_max
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
            traceback.print_exc()

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
    symbols = [i.upper().replace('-PERP', '') for i in args if i not in FUNDING_EXCHANGES and not re.match(r'\d', i)] or whitelist
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


def farb_command(update: Update, ctx: CallbackContext) -> None:
    # TODO return top 10 max(abs(funding diff by exchange)) group by symbol
    logger.info(update.message.text)


def get_el():
    return requests.get("https://api.dune.com/api/v1/query/3411506/results?limit=1000", headers={"X-Dune-API-Key": config.dune.apiKey}).json()['result']['rows'][0]['total_restaked_points']

def get_ef():
    #return requests.get('https://www.etherfi.bid/api/etherfi/points').json()['loyaltyPoints'] * 10 - 9 * 37100695710
    #return requests.get('https://www.ether.fi/api/points').json()['loyaltyPoints']
    return requests.get('https://app.ether.fi/api/points').json()['loyaltyPoints']

def get_el_price():
    #return requests.get('https://api-v2.whales.market/v2/tokens/detail/EigenLayer').json()['data']['last_price']
    return 0.05

def get_ef_price():
    return float(requests.get('https://fapi.binance.com/fapi/v1/ticker/price?symbol=ETHFIUSDT').json()['price'])

def points_command(update: Update, ctx: CallbackContext) -> None:
    update.message.reply_text(
f"""
`EL points {f"{int(get_el()):,}".rjust(16)}`
`EF points {f"{int(get_ef()):,}".rjust(16)}`
""".strip(), parse_mode=telegram.ParseMode.MARKDOWN_V2)

def etherfi_command(update: Update, ctx: CallbackContext) -> None:
    chat_id = update.message.chat_id
    address = config.wallet[str(chat_id)]
    d = requests.get(f"https://app.ether.fi/api/portfolio/v3/{address}").json()
    logger.info(f"https://app.ether.fi/api/portfolio/v3/{address}")
    logger.info(d)
    el = d['totalIntegrationEigenLayerPoints']
    ef = d['totalIntegrationLoyaltyPoints']
    global_el = get_el()
    global_ef = get_ef()
    el_price = get_el_price()
    ef_price = get_ef_price()
    ef_airdrop = 50000000
    update.message.reply_text(
f"""
your EL points {int(el):,}
your EF points {int(ef):,}
global EL points {int(global_el):,}
global EF points {int(global_ef):,}
EL point price ${el_price:.3f}
EF token price ${ef_price:.3f}
your EL airdrop ${el * el_price:.3f}
your EF airdrop ${ef / global_ef * ef_airdrop * ef_price:.3f}
your EF airdrop {ef / global_ef * ef_airdrop:.3f}
""".strip())


def get_lp_tokens_history(contract, address):
    con = sqlite3.connect('lp.sqlite3')
    return con.execute('select token0 * my_lp / total_lp, token1 * my_lp / total_lp from lp where contract = ? and address = ? order by timestamp desc limit 25', [contract, address]).fetchall()

def get_aave_debt(chain, token, address):
    chain = ad(config.aave[chain])
    token = ad(chain[token])
    w = Web3(Web3.HTTPProvider(chain.rpc))
    c = w.eth.contract(address=token.contract, abi=chain.abi)
    return c.functions.balanceOf(address).call() * 10**-token.decimals

def get_pnl(tokens, debts):
    bal = [t - d for t, d in zip(tokens, debts)]
    return (bal[1]/tokens[1]*tokens[0]+bal[0], bal[0]/tokens[0]*tokens[1]+bal[1])

def get_lp_output(lp, address):
    hist = get_lp_tokens_history(lp.contract, address)
    debts = [get_aave_debt(lp.aave, t.token, address) for t in lp.tokens]
    d0, d1 = debts
    t0, t1 = hist[0][0], hist[0][1]
    dt0 = d1/t1*t0+d0
    dt1 = d0/t0*t1+d1
    s0 = lp.tokens[0]["token"]
    s1 = lp.tokens[1]["token"]
    pnl_t = get_pnl(hist[0], debts)
    pnl_0 = get_pnl(hist[-1], debts)
    pnl_1d = [pnl_t[0]-pnl_0[0], pnl_t[1] - pnl_0[1]]
    apy_1d = pnl_1d[0] / dt0 * 365
    days = (datetime.datetime.now(datetime.timezone.utc) - datetime.datetime.fromisoformat(lp.start)).total_seconds() / 86400
    apy_all = pnl_t[0] / dt0 / days * 365
    k0 = hist[-1][0]*hist[-1][1]
    kt = t0*t1
    kapy_1d = ((kt/k0)**0.5-1)*365
    return f"""
lp {t0:.3f} {s0} + {t1:.3f} {s1} = {t0*2:.3f} {s0} ({t1*2:.3f} {s1})
debt {d0:.3f} {s0} + {d1:.3f} {s0} = {dt0:.3f} {s0} ({dt1:.3f} {s1})
24h pnl {pnl_1d[0]:.3f} {s0} ({pnl_1d[1]:.3f} {s1}) apy {apy_1d*100:.3f}%
all pnl {pnl_t[0]:.3f} {s0} ({pnl_t[1]:.3f} {s1}) apy {apy_all*100:.3f}%
k apy 24h {kapy_1d*100:.3f}%
""".strip()

def lp_command(update: Update, ctx: CallbackContext) -> None:
    chat_id = update.message.chat_id
    address = config.wallet[str(chat_id)]
    for lp in config.lp:
        text = get_lp_output(lp, address)
        update.message.reply_text(text)


def twitter_command(update: Update, _: CallbackContext) -> None:
    logger.info(update.message.text)
    url = "http://192.168.0.119:8000/get_tweet?url=" + update.message.text
    r = requests.get(url)
    update.message.reply_photo(r.content)


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


def ftt_alert(ctx: CallbackContext):
    ftt(ctx.bot)

def get_bot():
    return telegram.Bot(config.telegram.token)

def ftt(bot=get_bot()):
    #chat_id = config.telegram.chat_id
    chat_id = -608131165
    #exchange = ccxt.ftx(config['ftx'])
    exchange = ccxt.binance(dict(config['binance'], options={'defaultType':'future'}))
    symbol = 'FTT/BUSD'
    intv = '1m'
    limit = 30
    n = 3
    candles = exchange.fetch_ohlcv(symbol, '1m', limit=limit)[:-1]
    avgVol = sum(c[5] for c in candles) / len(candles)
    changes = ' '.join(f'{(c[4]/c[1]-1)*100:.2f}%' for c in candles[-n:])
    vols = ' '.join(f'{c[5]}' for c in candles[-n:])
    logger.info(f"{exchange.name} {symbol} avg vol {avgVol} changes {changes} vols {vols}")
    if (all(c[4] > c[1] and c[5] > avgVol for c in candles[-n:]) or
        all(c[4] < c[1] and c[5] > avgVol for c in candles[-n:])):
        msg = f'{exchange.name} {symbol} last {n} {intv} candles {changes}'
        bot.send_message(chat_id, msg)


def launchpool_alert(ctx: CallbackContext):
    r = req('https://www.binance.com/bapi/composite/v1/public/cms/article/all/query?type=1&pageNo=1&pageSize=10&queryKeywords=launchpool&sortBy=2&apiVersion=V2')
    con = sqlite3.connect('cache.sqlite3')
    cur = con.cursor()
    try:
        cur.execute('create table if not exists cache(key string primary key, value string)')
        con.commit()
    except:
        pass
    for i in r.data.articles:
        code, title = i.code, i.title
        logger.info(f"launchpool {code} {title}")
        url = f'https://www.binance.com/en/support/announcement/{code}'
        text = f'{title}\n{url}'
        if cur.execute('select * from cache where key=?', [code]).fetchone():
            continue
        ctx.bot.send_message(config.telegram.chat_id, text)
        cur.execute('insert into cache values (?, ?)', [code, title])
        con.commit()


from web3 import Web3
def update_lp(ctx):
    con = sqlite3.connect('lp.sqlite3')
    cur = con.cursor()
    try:
        cur.execute('create table if not exists lp(timestamp datetime default current_timestamp, contract string, address, string, total_lp real, my_lp real, token0 real, token1 real)')
        con.commit()
    except:
        pass
    address = config.wallet[config.telegram.chat_id]
    for lp in config.lp:
        contract = lp.contract
        w = Web3(Web3.HTTPProvider(lp.rpc))
        c = w.eth.contract(address=contract, abi=lp.abi)
        my_lp = c.functions.balanceOf(address).call() * 10**-lp.decimals
        total_lp = c.functions.totalSupply().call() * 10**-lp.decimals
        reserves = c.functions.getReserves().call()
        tokens = [reserves[i] * 10**-lp.tokens[i].decimals for i in [0, 1]]
        my_tokens = [tokens[i] * my_lp / total_lp for i in [0, 1]]
        logger.info(f"contract {contract} address {address} total_lp {total_lp} my_lp {my_lp} token0 {tokens[0]} token1 {tokens[1]} my {lp.tokens[0].token} {my_tokens[0]} my {lp.tokens[1].token} {my_tokens[1]} k {my_tokens[0] * my_tokens[1]}")
        #ctx.bot.send_message(config.telegram.chat_id, text)
        args = [contract, address, total_lp, my_lp, tokens[0], tokens[1]]
        cur.execute('insert into lp (contract, address, total_lp, my_lp, token0, token1) values (?, ?, ?, ?, ?, ?)', args)
        con.commit()


def kelp_withdraw():
    result = {}
    for k, c in config.kelp_withdraw.items():
        r = req(c['url'], method='post', json=c['json'], headers={'origin':c['origin']})
        s = r.result[c['result_range'][0]:c['result_range'][1]]
        result[k] = int(s, 16)*10**(-c['decimals'])
    return result

def check_kelp_withdraw(ctx):
    last = ctx.bot_data.get('kelp_withdraw_last_notified', datetime.datetime.now())
    if (datetime.datetime.now() - last).seconds < 3600:
        return
    for k, n in kelp_withdraw().items():
        if n > 0.1:
            text = f"kelp {k} {n}"
            ctx.bot.send_message(config.telegram.chat_id, text)
            ctx.bot_data['kelp_withdraw_last_notified'] = datetime.datetime.now()


def lst_eth(names):
    result = {}
    for name in names:
        if name in config.oracle.base:
            w = Web3(Web3.HTTPProvider(config.oracle.rpc.base))
        else:
            w = Web3(Web3.HTTPProvider(config.oracle.rpc.arbitrum))
        c = w.eth.contract(address=config.oracle[name], abi=config.oracle.abi)
        result[name] = c.functions.latestAnswer().call() * 1e-18
    return result

def lst_sol(names):
    url = 'https://sanctum-extra-api.ngrok.dev/v1/sol-value/current'
    params = [('lst', name) for name in names]
    r = requests.get(url, params=params)
    return {k: int(v) * 1e-9 for k, v in r.json()['solValues'].items()}

def lst_command(update: Update, ctx: CallbackContext) -> None:
    logger.info(update.message.text)
    names = re.split(r'[ /]', update.message.text[1:])[1:]
    if 0 < len(names) < 3 and all('eth' in name for name in names):
        exchange_rates = lst_eth(names)
    elif 0 < len(names) < 3 and all(name == 'inf' or 'sol' in name for name in names):
        exchange_rates = lst_sol(names)
    else:
        update.message.reply_text('ngmi')
        return
    if len(names) == 1:
        text = f"{exchange_rates[names[0]]:.4f}"
    else:
        text = f"{exchange_rates[names[0]] / exchange_rates[names[1]]:.4f}"
    update.message.reply_text(text)


def quote_cowswap(name, amount=1):
    payload = dict(config.cowswap[name], sellAmountBeforeFee=str(int(amount * 1e18)))
    r = requests.post('https://api.cow.fi/mainnet/api/v1/quote', json=payload)
    return int(r.json()['quote']['buyAmount']) * 1e-18

def peg_command(update: Update, ctx: CallbackContext) -> None:
    logger.info(update.message.text)
    try:
        names = update.message.text.split()[1:] or config.cowswap.keys()
        for name in names:
            rate = lst_eth([name])[name]
            quote = {i: quote_cowswap(name, i) for i in [1, 10, 32]}
            text = f"""`{name}`
`exchange rate: {rate:>6.4f}`
`quote:`
`{quote[1]:>6.4f} {quote[10]:>6.3f} {quote[32]:6.3f}`
`receive:`
`{quote[1] * rate:>6.4f} {quote[10] * rate:>6.3f} {quote[32] * rate:6.3f}`"""
            update.message.reply_text(text, parse_mode=telegram.ParseMode.MARKDOWN_V2)
    except:
        traceback.print_exc()
        update.message.reply_text('ngmi')


def get_gas():
    params = {'module': 'gastracker', 'action': 'gasoracle', 'apikey': config.etherscan.token}
    return requests.get('https://api.etherscan.io/api', params=params).json()['result']

def gas_command(update: Update, ctx: CallbackContext) -> None:
    try:
        params = {'module': 'gastracker', 'action': 'gasoracle', 'apikey': config.etherscan.token}
        r = requests.get('https://api.etherscan.io/api', params=params).json()['result']
        text = ' '.join(f"{float(r[k]):.3f}" for k in ['SafeGasPrice', 'ProposeGasPrice', 'FastGasPrice'])
        update.message.reply_text(text)
    except:
        traceback.print_exc()
        update.message.reply_text('ngmi')


def get_persistence(path):
    try:
        assert(os.path.getsize(path) > 0)
    except:
        os.remove(path)
    return PicklePersistence(path)


def main() -> None:
    persistence = get_persistence('apebot.pickle')
    updater = Updater(config.telegram.token, persistence=persistence)
    dispatcher = updater.dispatcher
    dispatcher.add_handler(CommandHandler("funding", funding_command))
    dispatcher.add_handler(CommandHandler("f1", funding_command))
    dispatcher.add_handler(CommandHandler("apy", apy_command))
    dispatcher.add_handler(CommandHandler("greed", greed_command))
    dispatcher.add_handler(CommandHandler("update_funding", update_funding_command))
    dispatcher.add_handler(CommandHandler("f", funding_command2))
    dispatcher.add_handler(CommandHandler("farb", farb_command))
    dispatcher.add_handler(CommandHandler("price_alert", price_alert_command))
    dispatcher.add_handler(CommandHandler("p", get_price_command))
    dispatcher.add_handler(CommandHandler("float", float_command))
    dispatcher.add_handler(CommandHandler("list_price_alert", list_price_alert_command))
    dispatcher.add_handler(CommandHandler("clear_price_alert", clear_price_alert_command))
    dispatcher.add_handler(CommandHandler("delete_price_alert", delete_price_alert_command))
    dispatcher.add_handler(CommandHandler(["point", "points"], points_command))
    dispatcher.add_handler(CommandHandler(["etherfi", "ethfi"], etherfi_command))
    dispatcher.add_handler(CommandHandler("lp", lp_command))
    dispatcher.add_handler(CommandHandler("lst", lst_command))
    dispatcher.add_handler(CommandHandler("peg", peg_command))
    dispatcher.add_handler(CommandHandler("gas", gas_command))
    dispatcher.add_handler(RegexHandler(r'https://(twitter|x).com/.*', twitter_command))
    updater.job_queue.run_repeating(update_markets, interval=3600, first=1) # 1h
    updater.job_queue.run_repeating(update_funding, interval=300, first=1) # 5m
    updater.job_queue.run_repeating(price_alert, interval=60, first=1) # 1m
    updater.job_queue.run_repeating(launchpool_alert, interval=3600, first=1) # 1h
    updater.job_queue.run_repeating(update_lp, interval=3600, first=1) # 1h
    updater.job_queue.run_repeating(check_kelp_withdraw, interval=300, first=1) # 5m
    updater.job_queue.run_repeating(update_funding, interval=300, first=1) # 5m
    #disabled
    #updater.job_queue.run_repeating(order_alert, interval=60, first=1) # 1m
    #updater.job_queue.run_repeating(ftt_alert, interval=31, first=1) # 1m
    updater.start_polling()
    updater.idle()


if __name__ == '__main__':
    main()

