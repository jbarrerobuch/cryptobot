# %% [markdown]
# # IMPORTS

# %%
import ccxt
import pandas as pd
import time
import datetime as dt
from config import myconfig

# %% [markdown]
# # INITIALIZE

# %%
exchange = ccxt.binance({
    "apiKey": myconfig.API_KEY,
    "secret": myconfig.API_SECRET,
    'enableRateLimit': False
})

# %% [markdown]
# # GET MARKET FEES

# %%
marketFees = exchange.fetch_fees()['trading']

# %%
marketFeesDF = pd.DataFrame.from_dict(marketFees, orient= 'index').drop(columns= 'info')

# %% [markdown]
# # GET MARKETS

# %%
markets = exchange.fetchMarkets()
#market_symbols = [market['symbol'] for market in markets]
#print(f'No. of market symbols: {len(market_symbols)}')
#print(f'Sample:{market_symbols[0:5]}')

# %%
markets[0]

# %% [markdown]
# # Load markets in a data frame  
#   
# Filter out inactive markets

# %%
markets_df = pd.DataFrame.from_dict(markets)
markets_df =  markets_df.loc[markets_df['active'] == True] # Drop inactive markets
markets_df.set_index('id', verify_integrity= True, inplace= True)

# %%
# Get only spot markets
spot_market = markets_df[markets_df.type == 'spot']

# Extrac decimal precision of markets
info_df = spot_market['info'].apply(pd.Series).drop(columns=[
    'symbol',
    'status',
    'baseAsset',
    'quoteAsset',
    'orderTypes',
    'icebergAllowed',
    'ocoAllowed',
    'quoteOrderQtyMarketAllowed',
    'allowTrailingStop',
    'isSpotTradingAllowed',
    'isMarginTradingAllowed',
    'filters',
    'permissions'
])
amounts_df = spot_market['limits'].apply(pd.Series)['amount'].apply(pd.Series)
amounts_df.rename(columns={'min':'amount_min', 'max':'amount_max'}, inplace= True)

spot_market.drop(columns=[
    'delivery', 'option', 'contract', 'linear',
    'inverse', 'contractSize', 'expiry',
    'expiryDatetime', 'margin', 'swap', 'future', 'strike',
    'optionType', 'precision', 'limits', 'info', 'settleId', 'settle', 'baseId', 'quoteId', 'lowercaseId'], inplace=True)
spot_market = pd.concat([spot_market, info_df, amounts_df], axis=1)

spot_market[spot_market['symbol'].str.contains('BTC/USDT')]

# %%
spot_symbols = list(spot_market.symbol)
#print('Total spot symbols',len(spot_symbols))

# %% [markdown]
# # STEP 1: GET ALL THE CRYPTO COMBINATIONS FOR USDT

# %%
def get_crypto_combinations(market_symbols, base):
    combinations = []
    for sym1 in market_symbols:
        
        sym1_token1 = sym1.split('/')[0]
        sym1_token2 = sym1.split('/')[1]
        
        if (sym1_token2 == base):
            for sym2 in market_symbols:
                sym2_token1 = sym2.split('/')[0]
                sym2_token2 = sym2.split('/')[1]
                if (sym1_token1 == sym2_token2):
                    for sym3 in market_symbols:
                        sym3_token1 = sym3.split('/')[0]
                        sym3_token2 = sym3.split('/')[1]
                        if((sym2_token1 == sym3_token1) and (sym3_token2 == sym1_token2)):
                            combination = {
                                'base':sym1_token2,
                                'intermediate':sym1_token1,
                                'ticker':sym2_token1,
                            }
                            combinations.append(combination)
                

    return combinations
        
wx_combinations_usdt = get_crypto_combinations(spot_symbols,'USDT')
#combinations_bnb = get_crypto_combinations(spot_market,'BUSD')


# %%
print(f'No. of USDT combinations: {len(wx_combinations_usdt)}')
#print(f'No. of BNB combinations: {len(combinations_bnb)}')

cominations_df = pd.DataFrame(wx_combinations_usdt)
cominations_df.head()

# %% [markdown]
# # STEP 2: PERFORM TRIANGULAR ARBITRAGE

# %% [markdown]
# ## Utility method to fetch the current ticker price

# %%
def fetch_current_ticker_price(tickerList, limiters= True):
    bidAsk = exchange.fetch_bids_asks(symbols= tickerList)

    # add 1 to limit counters
    if limiters:
        requestPer10sec['requests'] += 1
        requestPerMin['requests'] += 1
        requestPer24h['requests'] += 1

    #if operation == 'buy':
    #    return (bidAsk[ticker]['ask'], bidAsk[ticker]['askVolume'])
    #else:
    #    return (bidAsk[ticker]['bid'], bidAsk[ticker]['bidVolume'])
    return bidAsk

fetch_current_ticker_price('ETH/USDT', limiters= False)

# %% [markdown]
# ## Triangular Arbitrage

# %%
def check_buy_buy_sell(scrip1, scrip2, scrip3,investment_max_limit = 100, investment_min_limit = 5):
    
    fullfetch = fetch_current_ticker_price([scrip1, scrip2, scrip3])
    if verbose == True:
        print(f'{scrip1}: {fullfetch[scrip1]["bid"]}/{fullfetch[scrip1]["bidVolume"]} - {fullfetch[scrip1]["ask"]}/{fullfetch[scrip1]["askVolume"]}')
        print(f'{scrip2}: {fullfetch[scrip2]["bid"]}/{fullfetch[scrip2]["bidVolume"]} - {fullfetch[scrip2]["ask"]}/{fullfetch[scrip2]["askVolume"]}')
        print(f'{scrip3}: {fullfetch[scrip3]["bid"]}/{fullfetch[scrip3]["bidVolume"]} - {fullfetch[scrip3]["ask"]}/{fullfetch[scrip3]["askVolume"]}')

    # Vol analysis for OP size and PNL
    vol1 = fullfetch[scrip1]['bid']*fullfetch[scrip1]['bidVolume']
    vol2 = fullfetch[scrip2]['bid']*fullfetch[scrip2]['bidVolume']
    vol3 = fullfetch[scrip3]['ask']*fullfetch[scrip3]['askVolume']

    if verbose == True: print(f'Max inv: {investment_max_limit}')
    #Check volume of asset 1 is greater than investment limit
    if (investment_max_limit > vol1) and (vol1 > investment_min_limit):
        amount1 = vol1
        if verbose == True: print(f'Max inv reached to buy {amount1}')
    elif vol1 < investment_min_limit:
        if verbose == True: print(f'Vol1 lower than min {vol1}')
        return None
    else:
        amount1 = investment_max_limit
        if verbose == True: print(f'amount to buy {amount1}')

    amount2 = round(round(amount1/fullfetch[scrip1]['bid'],8) * (1-marketFeesDF.loc[scrip1]['taker']), 8) #include commission.
    if verbose == True: print(f'amount2 to buy: {amount2}')
    #Check volume of asset 2 is greater than investment limit
    if (amount2 > vol2) and (vol2 > investment_min_limit):
        amount2 = vol2
        amount1 = round(amount2 * fullfetch[scrip1]['bid'], 8) #Reverse conversion, without comission.
        if verbose == True: print(f'Max amount2 reached to buy {amount2}, amount1 {amount1}')
    elif vol2 < investment_min_limit:
        if verbose == True: print(f'Vol2 lower than min {vol2}')
        return None

    amount3 = round(round(amount2 / fullfetch[scrip2]['bid'],8) * (1-marketFeesDF.loc[scrip2]['taker']), 8)
    #Check volume of asset 3 is greater than investment limit
    if verbose == True: print(f'amount3 to sell {amount3}')
    if (amount3 > vol3) and (vol3 > investment_min_limit):
        amount3 = vol3
        amount2 = round(amount3 * fullfetch[scrip2]['bid'], 8) #Reverse conversion, without comission.
        amount1 = round(amount2 * fullfetch[scrip1]['bid'], 8) #Reverse conversion, without comission.
        if verbose == True: print(f'Max amount3 reached to sell {amount3}, amount2 {amount2}, amount1 {amount1}')
    elif vol3 < investment_min_limit:
        if verbose == True: print(f'Vol3 lower than min {vol3}')
        return None
    
    OP_return = round(round(amount3 * fullfetch[scrip3]['ask'],8) * (1-marketFeesDF.loc[scrip3]['taker']),8) #include commission in price
    if verbose == True: print('OP Return', OP_return)

    scrip_prices = {scrip1 : fullfetch[scrip1]["bid"], scrip2 : fullfetch[scrip2]['bid'], scrip3 : fullfetch[scrip3]['ask']}
    scrip_amounts = {scrip1: amount1, scrip2: amount2, scrip3: amount3}

    return OP_return, scrip_prices, scrip_amounts

# %%
def check_buy_sell_sell(scrip1, scrip2, scrip3,investment_max_limit = 100, investment_min_limit = 5):
        
    fullfetch = fetch_current_ticker_price([scrip1, scrip2, scrip3])
    if verbose == True:
        print(f'{scrip1}: {fullfetch[scrip1]["bid"]}/{fullfetch[scrip1]["bidVolume"]} - {fullfetch[scrip1]["ask"]}/{fullfetch[scrip1]["askVolume"]}')
        print(f'{scrip2}: {fullfetch[scrip2]["bid"]}/{fullfetch[scrip2]["bidVolume"]} - {fullfetch[scrip2]["ask"]}/{fullfetch[scrip2]["askVolume"]}')
        print(f'{scrip3}: {fullfetch[scrip3]["bid"]}/{fullfetch[scrip3]["bidVolume"]} - {fullfetch[scrip3]["ask"]}/{fullfetch[scrip3]["askVolume"]}')

    # Vol analysis for OP size and PNL - FIT CODE
    vol1 = fullfetch[scrip1]['bid']*fullfetch[scrip1]['bidVolume']
    vol2 = fullfetch[scrip2]['ask']*fullfetch[scrip2]['bidVolume']
    vol3 = fullfetch[scrip3]['ask']*fullfetch[scrip3]['askVolume']

    if verbose == True: print(f'Max inv: {investment_max_limit}')
    #Check volume of asset 1 is greater than investment limit
    if (investment_max_limit > vol1) and (vol1 > investment_min_limit):
        amount1 = vol1
        if verbose == True: print(f'Max inv reached to buy {amount1}')
    elif vol1 < investment_min_limit:
        if verbose == True: print(f'Vol1 lower than min {vol1}')
        return None
    else:
        amount1 = investment_max_limit
        if verbose == True: print(f'amount to buy is inves {amount1}')

    #Check volume of asset 2 is greater than investment limit
    amount2 = round(round(amount1/fullfetch[scrip1]['bid'], 8) * (1-marketFeesDF.loc[scrip1]['taker']), 8) #include commission
    if verbose == True: print(f'amount2 to buy: {amount2}')
    if (amount2 > vol2) and (vol2 > investment_min_limit):
        amount2 = vol2
        amount1 = round(amount2 * fullfetch[scrip1]['bid'], 8) # Reverse conversion without commission
        if verbose == True: print(f'Max amount2 reached to buy {amount2}, amount1 {amount1}')
    elif vol2 < investment_min_limit:
        if verbose == True: print(f'Vol1 lower than min {vol1}')
        return None
        
    #Check volume of asset 2 is greater than investment limit
    amount3 = round(round(amount2 * fullfetch[scrip2]['ask'], 8) * (1-marketFeesDF.loc[scrip2]['taker']), 8)
    if verbose == True: print(f'amount3 to sell {amount3}')
    if (amount3 > vol3) and (vol3 > investment_min_limit):
        amount3 = vol3
        amount2 = round(amount3 / fullfetch[scrip2]['bid'], 8) # Reverse conversion without comission
        amount1 = round(amount2 * fullfetch[scrip1]['bid'], 8) # Reverse conversion without comission
        if verbose == True: print(f'Max amount3 reached to sell {amount3}, amount2 {amount2}, amount1 {amount1}')
    elif vol3 < investment_min_limit:
        if verbose == True: print(f'Vol1 lower than min {vol1}')
        return None
    
    OP_return = round(round(amount3 * fullfetch[scrip3]['ask'], 8) * (1-marketFeesDF.loc[scrip3]['taker']),8) #include commission in price
    if verbose == True: print('OP Return', OP_return)

    scrip_prices = {scrip1 : fullfetch[scrip1]["bid"], scrip2 : fullfetch[scrip2]['ask'], scrip3 : fullfetch[scrip3]['ask']}
    scrip_amounts = {scrip1: amount1, scrip2: amount2, scrip3: amount3}

    return OP_return, scrip_prices, scrip_amounts

# %%
def check_profit_loss(OP_return, initial_investment, min_profit):
    min_profitable_price = initial_investment * 1 + min_profit
    profit = (OP_return >= min_profitable_price)
    return profit

# %% [markdown]
# # STEP 3: PLACE THE TRADE ORDERS

# %%
def place_buy_order(scrip, quantity, limit):
    order = exchange.create_limit_buy_order(scrip, quantity, limit)
    return order

def place_sell_order(scrip, quantity, limit):
    order = exchange.create_limit_sell_order(scrip, quantity, limit)
    return order 

def place_trade_orders(type, scrip1, scrip2, scrip3, initial_amount, scrip_prices):
    final_amount = 0.0
    if type == 'BUY_BUY_SELL':
        s1_quantity = initial_amount/scrip_prices[scrip1]
        place_buy_order(scrip1, s1_quantity, scrip_prices[scrip1])
        
        s2_quantity = s1_quantity/scrip_prices[scrip2]
        place_buy_order(scrip2, s2_quantity, scrip_prices[scrip2])
        
        s3_quantity = s2_quantity
        place_sell_order(scrip3, s3_quantity, scrip_prices[scrip3])
        
    elif type == 'BUY_SELL_SELL':
        s1_quantity = initial_amount/scrip_prices[scrip1]
        place_buy_order(scrip1, s1_quantity, scrip_prices[scrip1])
        
        s2_quantity = s1_quantity
        place_sell_order(scrip2, s2_quantity, scrip_prices[scrip2])
        
        s3_quantity = s2_quantity * scrip_prices[scrip2]
        place_sell_order(scrip3, s3_quantity, scrip_prices[scrip3])
        
        
    return final_amount

# %% [markdown]
# Sample order from exchange immediately after execution:   
# {'info': {'id': '2490462375', 'symbol': 'btcusdt', 'type': 'limit', 'side': 'buy', 'status': 'wait', 'price': '43201.0', 'origQty': '0.002314', 'executedQty': '0.0', 'createdTime': '1646302254000', 'updatedTime': '1646302254000'}, 'id': '2490462375', 'clientOrderId': None, 'timestamp': 1646302254000, 'datetime': '2022-03-03T10:10:54.000Z', 'lastTradeTimestamp': 1646302254000, 'status': 'open', 'symbol': 'BTC/USDT', 'type': 'limit', 'timeInForce': None, 'postOnly': None, 'side': 'buy', 'price': 43201.0, 'amount': None, 'filled': 0.0, 'remaining': None, 'cost': 0.0, 'fee': None, 'average': None, 'trades': [], 'fees': []}

# %% [markdown]
# # STEP 4: WRAPPING IT TOGETHER

# %%
#change datetime.now() for timestamp

def perform_triangular_arbitrage(scrip1, scrip2, scrip3, arbitrage_type,investment_limit, min_profit_percentage):
    start_time = time.time()

    OP_return = 0.0
    if(arbitrage_type == 'BUY_BUY_SELL'):
        # Check this combination for triangular arbitrage: scrip1 - BUY, scrip2 - BUY, scrip3 - SELL
        op = check_buy_buy_sell(scrip1, scrip2, scrip3,investment_limit)
        if not op == None:
            OP_return, scrip_prices,scrip_amounts = op
        else:
            return None
        
    elif(arbitrage_type == 'BUY_SELL_SELL'):
        # Check this combination for triangular arbitrage: scrip1 - BUY, scrip2 - SELL, scrip3 - SELL
        op = check_buy_sell_sell(scrip1, scrip2, scrip3,investment_limit)
        if not op == None:
            OP_return, scrip_prices,scrip_amounts = op
        else:
            return None

    profit = check_profit_loss(OP_return,scrip_amounts[scrip1], min_profit_percentage)
    result = f"{dt.datetime.now().strftime('%d-%b-%Y %H:%M:%S.%f')},\
            {arbitrage_type}, {scrip1}, {scrip_prices[scrip1]}, {scrip_amounts[scrip1]},\
            {scrip2}, {scrip_prices[scrip2]}, {scrip_amounts[scrip2]}, {scrip3}, {scrip_prices[scrip3]}, {scrip_amounts[scrip3]},\
            {scrip_amounts[scrip1]}, {OP_return}, {profit}"

    if profit:
        #place_trade_orders(arbitrage_type, scrip1, scrip2, scrip3, initial_investment, scrip_prices)
        print(result)
    end_time = time.time()
    exe_time = start_time - end_time
    result += f',{exe_time}'
    return result

# %% [markdown]
# ### Set request strict limits

# %%
#Límites estrictos:
#Ponderación de 1200 solicitudes por minuto (ten en cuenta que no es necesariamente lo mismo que 1200 solicitudes)
#50 órdenes cada 10 segundos
#160 000 órdenes cada 24 horas
#Nuestros límites estrictos están incluidos en el punto final [/api/v3/exchangeInfo].
placeholderTime = dt.datetime.strptime('00:00', '%H:%M')
requestPerMin = {'start': placeholderTime , 'requests': 0}
requestPer10sec = {'start': placeholderTime, 'end': placeholderTime, 'requests': 0}
requestPer24h = {'start': placeholderTime, 'end': placeholderTime, 'requests': 0}

limit10sec = False
limitPerMin = False
limitPer24H = False

# Calculate average request per minute
def AVGrequestsPerMin(requests, start):
    #print(end)
    interval = dt.datetime.now() - start
    #print(interval)
    try:
        avg = round(requests/ round(interval.total_seconds()/60,0), 0)
    except ZeroDivisionError:
        avg = 0
    return avg

# %%
def checkLimits(verbose= False):
    # Handle 24H limit
    if requestPer24h['requests'] >= (160000):
        deltaToEnd = requestPer24h['end'] - dt.datetime.now()
        if verbose in [True,'rateLimit']: print(f'24H limit reached, spleeping {deltaToEnd} until {requestPer24h["end"]}')
        time.sleep(deltaToEnd.total_seconds())
        requestPer24h['start'] = dt.datetime.now()
        requestPer24h['end'] = requestPer24h['start'] + dt.timedelta(hours= 24)
        requestPer24h['requests'] = 0
    elif requestPer24h['end'] < dt.datetime.now():
        requestPer24h['start'] = dt.datetime.now()
        requestPer24h['end'] = requestPer24h['start'] + dt.timedelta(hours= 24)
        requestPer24h['requests'] = 0
        if verbose in [True,'rateLimit']: print('24H counter reset')
    
    # Handle 10 seconds limit
    if requestPer10sec['requests'] >= (50):
        deltaToEnd = requestPer10sec['end'] - dt.datetime.now()
        if verbose in [True,'rateLimit']: print(f'10 sec. limit reached, sleeping {deltaToEnd} until {requestPer10sec["end"]}')
        time.sleep(deltaToEnd.total_seconds())
        requestPer10sec['start'] = dt.datetime.now()
        requestPer10sec['end'] = requestPer10sec['start'] + dt.timedelta(seconds= 10)
        requestPer10sec['requests'] = 0
    elif requestPer10sec['end'] < dt.datetime.now():
        requestPer10sec['start'] = dt.datetime.now()
        requestPer10sec['end'] = requestPer10sec['start'] + dt.timedelta(seconds= 10)
        requestPer10sec['requests'] = 0
        if verbose in [True,'rateLimit']: print('10 sec counter reset')
    
    # Handle 1 minute average limit
    avgMin = AVGrequestsPerMin(requestPerMin['requests'],requestPerMin['start'])
    if avgMin >= 1200:
        if verbose in [True,'rateLimit']: print(f'Request per minute limit reached, sleeping 5 minutes')
        time.sleep(300)
    
    # Limit verbose
    if verbose in [True,'rateLimit']: 
        print(f'Limit 10 sec: {requestPer10sec["requests"]}')
        print(f'Limit min AVG: {avgMin}')
        print(f'Limit 24h: {requestPer24h["requests"]}')

# %%
# brokerage commission from API included in price @ check bbs or sbb strategies
verbose = 'rateLimit' # True: full, 'rateLimit':limit counters
INVESTMENT_AMOUNT_DOLLARS = 100
MIN_PROFIT_percentage = 0.01
#BROKERAGE_PER_TRANSACTION_PERCENT = 0.2 ## taken from marketFeesDF

#Start minute average counter
requestPerMin['start'] = dt.datetime.now()

while(True):
    for combination in wx_combinations_usdt:

        base = combination['base']
        intermediate = combination['intermediate']
        ticker = combination['ticker']
        combination_ID = '/'.join([base, intermediate, ticker]) # Eg: "USDT/BTC/ETH"

        s1 = f'{intermediate}/{base}'    # Eg: BTC/USDT
        s2 = f'{ticker}/{intermediate}'  # Eg: ETH/BTC
        s3 = f'{ticker}/{base}'          # Eg: ETH/USDT
                
        # check request limits and sleep if exceed
        checkLimits(verbose=verbose)

        # Check triangular arbitrage for buy-buy-sell 
        bbs = perform_triangular_arbitrage(s1,s2,s3,'BUY_BUY_SELL',INVESTMENT_AMOUNT_DOLLARS, MIN_PROFIT_percentage)

        if not bbs == None:
            with open(f'output\TriBot_output_{dt.datetime.today().date().strftime("%d%m%Y")}.csv', 'a') as f:
                f.write(combination_ID+','+bbs+'\n')

        # check request limits
        checkLimits(verbose= True)

        # Check triangular arbitrage for buy-sell-sell 
        bss = perform_triangular_arbitrage(s3,s2,s1,'BUY_SELL_SELL',INVESTMENT_AMOUNT_DOLLARS, MIN_PROFIT_percentage)
        if not bss == None:
            with open(f'output\TriBot_output_{dt.datetime.today().date().strftime("%d%m%Y")}.csv', 'a') as f:
                f.write(combination_ID+','+bss+'\n')

# %%



