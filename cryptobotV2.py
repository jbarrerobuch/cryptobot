# %% [markdown]
# # IMPORTS

# %%
import ccxt
import pandas as pd
import time
import datetime as dt
from config import myconfig
import os

# %% [markdown]
# # INITIALIZE

# %%
exchange = ccxt.binance({
    "apiKey": myconfig.API_KEY_TEST,
    "secret": myconfig.API_SECRET_TEST
})
exchange.enableRateLimit = True
exchange.set_sandbox_mode(True)

# %% [markdown]
# # GET MARKETS

# %%
markets = exchange.fetchMarkets()

# %% [markdown]
# # Load markets in a data frame  
#   
# Filter out inactive markets

# %%
markets_df = pd.DataFrame.from_dict(markets)
markets_df =  markets_df.loc[markets_df['active'] == True] # Drop inactive markets
markets_df.set_index('symbol', verify_integrity= True, inplace= True,drop= False)

# %%
# Get only spot markets
spot_market = markets_df[markets_df.type == 'spot']

# %%
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

# Extract amount limits
amounts_df = spot_market['limits'].apply(pd.Series)['amount'].apply(pd.Series)
amounts_df.rename(columns={'min':'amount_min', 'max':'amount_max'}, inplace= True)

# Extract cost limits
cost_df = spot_market['limits'].apply(pd.Series)['cost'].apply(pd.Series)
cost_df.rename(columns={'min': 'cost_min', 'max': 'cost_max'}, inplace= True)

spot_market.drop(columns=[
    'delivery', 'option', 'contract', 'linear',
    'inverse', 'contractSize', 'expiry',
    'expiryDatetime', 'margin', 'swap', 'future', 'strike',
    'optionType', 'precision', 'limits', 'info', 'settleId', 'settle', 'baseId', 'quoteId', 'lowercaseId'], inplace=True)
spot_market = pd.concat([spot_market, info_df, amounts_df, cost_df], axis=1)

spot_market[spot_market['symbol'].str.contains('BTC/USDT')]

# %%
spot_symbols = list(spot_market.symbol)
print('Total spot symbols',len(spot_symbols))

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

#    # add 1 to limit counters
#    if limiters:
#        requestPer10sec['requests'] += 1
#        requestPerMin['requests'] += 1
#        requestPer24h['requests'] += 1
#
    #if operation == 'buy':
    #    return (bidAsk[ticker]['ask'], bidAsk[ticker]['askVolume'])
    #else:
    #    return (bidAsk[ticker]['bid'], bidAsk[ticker]['bidVolume'])
    return bidAsk

fetch_current_ticker_price('ETH/USDT', limiters= False)

# %%
spot_market.loc[['BTC/USDT', 'ETH/BTC', 'ETH/USDT']]

# %% [markdown]
# ## Triangular Arbitrage

# %%
def check_buy_buy_sell(scrip1, scrip2, scrip3, investment_max_limit= 100, verbose= False):
    if verbose in [True]: print('Checking buy_buy_sell')
    fullfetch = fetch_current_ticker_price([scrip1, scrip2, scrip3])
    if verbose in [True]:
        print(f'{scrip1}: {fullfetch[scrip1]["bid"]}/{fullfetch[scrip1]["bidVolume"]} - {fullfetch[scrip1]["ask"]}/{fullfetch[scrip1]["askVolume"]}')
        print(f'{scrip2}: {fullfetch[scrip2]["bid"]}/{fullfetch[scrip2]["bidVolume"]} - {fullfetch[scrip2]["ask"]}/{fullfetch[scrip2]["askVolume"]}')
        print(f'{scrip3}: {fullfetch[scrip3]["bid"]}/{fullfetch[scrip3]["bidVolume"]} - {fullfetch[scrip3]["ask"]}/{fullfetch[scrip3]["askVolume"]}')

    # OP parameters PRICE, AMOUNT & COST, FEE %
    price1 = fullfetch[scrip1]['ask']
    price2 = fullfetch[scrip2]['ask']
    price3 = fullfetch[scrip3]['bid']

    av_amount1 = fullfetch[scrip1]['askVolume'] # in BTC (base)
    av_amount2 = fullfetch[scrip2]['askVolume'] # in ETH (base)
    av_amount3 = fullfetch[scrip3]['bidVolume'] # in ETH (base)

    av_cost1 = float(exchange.cost_to_precision(scrip1, price1 * av_amount1)) # in USDT (quote)
    av_cost2 = float(exchange.cost_to_precision(scrip2, price2 * av_amount2)) # in BTC (quote)
    av_cost3 = float(exchange.cost_to_precision(scrip3, price3 * av_amount3)) # in USDT (quote)

    fee1 = spot_market.loc[scrip1]['taker']
    fee2 = spot_market.loc[scrip2]['taker']
    fee3 = spot_market.loc[scrip3]['taker']

    # Minimun amount (base) & cost (quote)
    am_min1 = spot_market.loc[scrip1]['amount_min'] # in BTC (base)
    am_min2 = spot_market.loc[scrip2]['amount_min'] # in ETH (base)
    am_min3 = spot_market.loc[scrip3]['amount_min'] # in ETH (base)

    ct_min1 = spot_market.loc[scrip1]['cost_min'] # in USDT (quote)
    ct_min2 = spot_market.loc[scrip2]['cost_min'] # in BTC (quote)
    ct_min3 = spot_market.loc[scrip3]['cost_min'] # in USDT (quote)

    #Max investment in base amount
    investment_max_limit_amount = float(exchange.amount_to_precision(scrip1, investment_max_limit/price1))
    if verbose in [True]: print(f'Max investment: cost/amount {investment_max_limit}/{investment_max_limit_amount}')

    # Conditions to reject the OP
    if (av_amount1 < am_min1) or (av_amount2 < am_min2) or (av_amount3 < am_min3):
        if verbose in [True]: print(f'Any available amount is lower than min amount, skip OP. av_amounnt1 {av_amount1}/{am_min1} av_amount2 {av_amount2}/{am_min2} av_amount3 {av_amount3}/{am_min3}')
        return None
    elif (av_cost1 < ct_min1) or av_cost2 < ct_min2 or av_cost3 < ct_min3:
        if verbose in [True]: print(f'Any available cost is lower than min cost, skip OP. av_cost1 {av_cost1}/{ct_min1} av_cost2 {av_cost2}/{ct_min2} av_cost3 {av_cost3}/{ct_min3}')
        return None

    # Amount & cost for volume available in scrip1
    if av_amount1 <= investment_max_limit_amount:
        if verbose in [True]: print('Amount 1 is within max limit')
        amount1 = av_amount1
        cost1 = float(exchange.cost_to_precision(scrip1, amount1 * price1))
        cost2 = float(exchange.cost_to_precision(scrip2, amount1 * (1 - fee1)))
        amount2 = float(exchange.amount_to_precision(scrip2, cost2/price2))
        amount3 = float(exchange.amount_to_precision(scrip3, amount2 * (1 - fee2))) # change from buy to sell
        cost3 = float(exchange.cost_to_precision(scrip1, amount3 * price3))
        OP_return = cost3 * (1 - fee3)

    # Amount & cost for max limit in scrip1
    else:
        if verbose in [True]: print('Amount 1 is max limit')
        amount1 = float(exchange.amount_to_precision(scrip1, investment_max_limit_amount))
        cost1 = float(exchange.cost_to_precision(scrip1, amount1 * price1))
        cost2 = float(exchange.cost_to_precision(scrip2, amount1 * (1 - fee1)))
        amount2 = float(exchange.amount_to_precision(scrip2, cost2/price2))
        amount3 = float(exchange.amount_to_precision(scrip3, amount2 * (1 - fee2))) # change from buy to sell
        cost3 = float(exchange.cost_to_precision(scrip3, amount3 * price3))
        OP_return = cost3 * (1 - fee3)

    if verbose in [True, 'trade']:
        print(f'cost1 {cost1} amount1 {amount1} price1 {price1}')
        print(f'cost2 {cost2} amount2 {amount2} price2 {price2}')
        print(f'cost3 {cost3} amount3 {amount3} price3 {price3}')
        print(f'OP return {OP_return}')

    # Check amount2 is available in scrip2
    if amount2 > av_amount2:
        if verbose in [True]: print('Amount2 not available, recalculate all params from av_amount2')
        amount2 = av_amount2
        cost2 = float(exchange.cost_to_precision(scrip2, amount2 * price2))
        amount1 = float(exchange.amount_to_precision(scrip1, cost2 / (1 - fee1)))
        cost1 = float(exchange.cost_to_precision(scrip1, amount1 * price1))
        amount3 = float(exchange.amount_to_precision(scrip3, amount2 * (1 - fee2))) # change from buy to sell
        cost3 = float(exchange.cost_to_precision(scrip3, amount3 * price3))
        OP_return = cost3 * (1 - fee3)
        if verbose in [True, 'trade']:
            print(f'cost1 {cost1} amount1 {amount1} price1 {price1}')
            print(f'cost2 {cost2} amount2 {amount2} price2 {price2}')
            print(f'cost3 {cost3} amount3 {amount3} price3 {price3}')
            print(f'OP return {OP_return}')
    else:
        if verbose in [True]: print('Amount2 is available to continue')

    # Check amount3 is available in scrip3
    if amount3 > av_amount3:
        if verbose in [True]: print('Amount3 not available, recalculate all params from av_amount3')
        amount3 = av_amount3
        cost3 = float(exchange.cost_to_precision(scrip3, amount3 * price3))
        amount2 = float(exchange.amount_to_precision(scrip2, amount3/(1 - fee2))) # change from buy to sell
        cost2 = float(exchange.cost_to_precision(scrip2, amount2 * price2))
        amount1 = float(exchange.amount_to_precision(scrip1, cost2/(1-fee1)))
        cost1 = float(exchange.cost_to_precision(scrip1, amount1 * price1))
        OP_return = cost3 * (1 - fee3)
        if verbose in [True, 'trade']:
            print(f'cost1 {cost1} amount1 {amount1} price1 {price1}')
            print(f'cost2 {cost2} amount2 {amount2} price2 {price2}')
            print(f'cost3 {cost3} amount3 {amount3} price3 {price3}')
            print(f'OP return {OP_return}')
    else:
        if verbose in [True]: print('Amount3 is available to continue')

    scrip_prices = {scrip1 : fullfetch[scrip1]["ask"], scrip2 : fullfetch[scrip2]['ask'], scrip3 : fullfetch[scrip3]['bid']}
    scrip_amounts = {scrip1: amount1, scrip2: amount2, scrip3: amount3}
    scrip_costs = {scrip1: cost1, scrip2: cost2, scrip3: cost3}

    return OP_return, scrip_prices, scrip_amounts, scrip_costs

def check_buy_sell_sell(scrip1, scrip2, scrip3, investment_max_limit = 100, verbose= False):
    if verbose in [True]: print('Checking buy_buy_sell')
    fullfetch = fetch_current_ticker_price([scrip1, scrip2, scrip3])
    if verbose in [True]:
        print(f'{scrip1}: {fullfetch[scrip1]["bid"]}/{fullfetch[scrip1]["bidVolume"]} - {fullfetch[scrip1]["ask"]}/{fullfetch[scrip1]["askVolume"]}')
        print(f'{scrip2}: {fullfetch[scrip2]["bid"]}/{fullfetch[scrip2]["bidVolume"]} - {fullfetch[scrip2]["ask"]}/{fullfetch[scrip2]["askVolume"]}')
        print(f'{scrip3}: {fullfetch[scrip3]["bid"]}/{fullfetch[scrip3]["bidVolume"]} - {fullfetch[scrip3]["ask"]}/{fullfetch[scrip3]["askVolume"]}')

    # OP parameters PRICE, AMOUNT & COST, FEE %
    price1 = fullfetch[scrip1]['ask']
    price2 = fullfetch[scrip2]['bid']
    price3 = fullfetch[scrip3]['bid']

    av_amount1 = fullfetch[scrip1]['askVolume'] # in BTC (base)
    av_amount2 = fullfetch[scrip2]['bidVolume'] # in ETH (base)
    av_amount3 = fullfetch[scrip3]['bidVolume'] # in ETH (base)

    av_cost1 = price1 * av_amount1 # in USDT (quote)
    av_cost2 = price2 * av_amount2  # in BTC (quote)
    av_cost3 = price3 * av_amount3  # in USDT (quote)

    fee1 = spot_market.loc[scrip1]['taker']
    fee2 = spot_market.loc[scrip2]['taker']
    fee3 = spot_market.loc[scrip3]['taker']

    # Minimun amount (base) & cost (quote)
    am_min1 = spot_market.loc[scrip1]['amount_min'] # in BTC (base)
    am_min2 = spot_market.loc[scrip2]['amount_min'] # in ETH (base)
    am_min3 = spot_market.loc[scrip3]['amount_min'] # in ETH (base)

    ct_min1 = spot_market.loc[scrip1]['cost_min'] # in USDT (quote)
    ct_min2 = spot_market.loc[scrip2]['cost_min'] # in BTC (quote)
    ct_min3 = spot_market.loc[scrip3]['cost_min'] # in USDT (quote)

    #Max investment in base amount
    investment_max_limit_amount = float(exchange.amount_to_precision(scrip1, investment_max_limit/price1))
    if verbose in [True]: print(f'Max investment: cost/amount {investment_max_limit}/{investment_max_limit_amount}')

    # Conditions to reject the OP
    if (av_amount1 < am_min1) or (av_amount2 < am_min2) or (av_amount3 < am_min3):
        if verbose: print(f'Any available amount is lower than min amount, skip OP. av_amounnt1 {av_amount1}/{am_min1} av_amount2 {av_amount2}/{am_min2} av_amount3 {av_amount3}/{am_min3}')
        return None
    elif (av_cost1 < ct_min1) or av_cost2 < ct_min2 or av_cost3 < ct_min3:
        if verbose: print(f'Any available cost is lower than min cost, skip OP. av_cost1 {av_cost1}/{ct_min1} av_cost2 {av_cost2}/{ct_min2} av_cost3 {av_cost3}/{ct_min3}')
        return None

    # Amount & cost for volume available in scrip1
    if av_amount1 <= investment_max_limit_amount:
        if verbose in [True]: print('Amount 1 is within max limit')
        amount1 = av_amount1
        cost1 = float(exchange.cost_to_precision(scrip1, amount1 * price1))
        amount2 = float(exchange.amount_to_precision(scrip2, amount1 * (1 - fee1))) #change from buy to sell
        cost2 = float(exchange.cost_to_precision(scrip2, amount2 * price2))
        amount3 = float(exchange.amount_to_precision(scrip3, cost2 * (1 - fee2)))
        cost3 = float(exchange.cost_to_precision(scrip3, amount3 * price3))
        OP_return = cost3 * (1 - fee3)

    # Amount & cost for max limit in scrip1
    else:
        if verbose in [True]: print('Amount 1 is max limit')
        amount1 = investment_max_limit_amount
        cost1 = float(exchange.cost_to_precision(scrip1, amount1 * price1))
        amount2 = float(exchange.amount_to_precision(scrip2, amount1 * (1 - fee1))) #change from buy to sell
        cost2 = float(exchange.cost_to_precision(scrip2, amount2 * price2))
        amount3 = float(exchange.amount_to_precision(scrip3, cost2 * (1 - fee2)))
        cost3 = float(exchange.cost_to_precision(scrip3, amount3 * price3))
        OP_return = cost3 * (1 - fee3)

    if verbose in [True, 'trade']:
        print(f'cost1 {cost1} amount1 {amount1} price1 {price1}')
        print(f'cost2 {cost2} amount2 {amount2} price2 {price2}')
        print(f'cost3 {cost3} amount3 {amount3} price3 {price3}')
        print(f'OP return {OP_return}')

    # Check amount2 is available in scrip2
    if amount2 > av_amount2:
        if verbose in [True]: print('Amount2 not available, recalculate all params from av_amount2')
        amount2 = av_amount2
        cost2 = float(exchange.cost_to_precision(scrip2, amount2 * price2))
        amount1 = float(exchange.amount_to_precision(scrip1, amount2 / (1 - fee1)))
        cost1 = float(exchange.cost_to_precision(scrip1, amount1 * price1))
        amount3 = float(exchange.cost_to_precision(scrip3, cost2 * (1 - fee2)))
        cost3 = float(exchange.cost_to_precision(scrip3, amount3 * price3))
        OP_return = cost3 * (1 - fee3)
        if verbose in [True, 'trade']:
            print(f'cost1 {cost1} amount1 {amount1} price1 {price1}')
            print(f'cost2 {cost2} amount2 {amount2} price2 {price2}')
            print(f'cost3 {cost3} amount3 {amount3} price3 {price3}')
            print(f'OP return {OP_return}')
    else:
        if verbose in [True]: print('Amount2 is available to continue')
    
    # Check amount3 is available in scrip3
    if amount3 > av_amount3:
        if verbose in [True]: print('Amount3 not available, recalculate all params from av_amount3')
        amount3 = av_amount3
        cost3 = float(exchange.cost_to_precision(scrip3, amount3 * price3))
        cost2 = float(exchange.cost_to_precision(scrip2, amount3/(1 - fee2)))
        amount2 = float(exchange.amount_to_precision(scrip2, cost2 / price2))
        amount1 = float(exchange.amount_to_precision(scrip1, amount2/(1 - fee1)))
        cost1 = float(exchange.cost_to_precision(scrip1, amount1 * price1))
        OP_return = cost3 * (1 - fee3)
        if verbose in [True, 'trade']:
            print(f'cost1 {cost1} amount1 {amount1} price1 {price1}')
            print(f'cost2 {cost2} amount2 {amount2} price2 {price2}')
            print(f'cost3 {cost3} amount3 {amount3} price3 {price3}')
            print(f'OP return {OP_return}')
    else:
        if verbose in [True]: print('Amount3 is available to continue')


    scrip_prices = {scrip1 : fullfetch[scrip1]["ask"], scrip2 : fullfetch[scrip2]['ask'], scrip3 : fullfetch[scrip3]['bid']}
    scrip_amounts = {scrip1: amount1, scrip2: amount2, scrip3: amount3}
    scrip_costs = {scrip1: cost1, scrip2: cost2, scrip3: cost3}

    return OP_return, scrip_prices, scrip_amounts, scrip_costs

# %%
def check_profit_loss(OP_return, initial_investment, min_profit):
    min_profitable_price = initial_investment * 1 + min_profit
    profit = (OP_return >= min_profitable_price)
    return profit


# %% [markdown]
# # STEP 3: PLACE THE TRADE ORDERS

# %%
def place_buy_order(scrip, quantity, limit):
    quantity = float(exchange.amount_to_precision (scrip, quantity))
    limit = float(exchange.price_to_precision(scrip,limit))
    order = exchange.create_limit_buy_order(scrip, quantity, limit)
    return order

def place_sell_order(scrip, quantity, limit):
    quantity = float(exchange.amount_to_precision(scrip, quantity))
    limit = float(exchange.price_to_precision(scrip,limit))
    order = exchange.create_limit_sell_order(scrip, quantity, limit)
    return order 

def place_trade_orders(type, scrip1, scrip2, scrip3, initial_amount, scrip_prices):
    final_amount = 0.0
    OP_details = []
    start = time.time()
    if type == 'BUY_BUY_SELL':
        
        # Trade 1
        s1_quantity = initial_amount/scrip_prices[scrip1]
        order1 = place_buy_order(scrip1, s1_quantity, scrip_prices[scrip1])
        if verbose in [True]: print(order1)
        if verbose in ['trade','exe']: print(f'Order 1: {order1["symbol"]} {order1["side"]} {order1["status"]} {order1["price"]} {order1["amount"]} {order1["filled"]} {order1["remaining"]} {order1["cost"]} {order1["fee"]}')
        
        # Trade 2
        s2_quantity = float(order1['amount'])/scrip_prices[scrip2]
        order2 = place_buy_order(scrip2, s2_quantity, scrip_prices[scrip2])
        if verbose in [True]: print('Order 2',order2)
        if verbose in ['trade','exe']: print(f'Order 2: {order2["symbol"]} {order2["side"]} {order2["status"]} {order2["price"]} {order2["amount"]} {order2["filled"]} {order2["remaining"]} {order2["cost"]} {order2["fee"]}')
        
        # Trade 3
        s3_quantity = float(order2['amount'])
        order3 = place_sell_order(scrip3, s3_quantity, scrip_prices[scrip3])
        if verbose in [True]: print('Order 3',order3)
        if verbose in ['trade','exe']: print(f'Order 3: {order3["symbol"]} {order3["side"]} {order3["status"]} {order3["price"]} {order3["amount"]} {order3["filled"]} {order3["remaining"]} {order3["cost"]} {order3["fee"]}')
    
    
        
    elif type == 'BUY_SELL_SELL':
        s1_quantity = initial_amount/scrip_prices[scrip1]
        order1 = place_buy_order(scrip1, s1_quantity, scrip_prices[scrip1])
        if verbose in [True]: print(order)
        if verbose in ['trade','exe']: print(f'Order 1: {order1["symbol"]} {order1["side"]} {order1["status"]} {order1["price"]} {order1["amount"]} {order1["filled"]} {order1["remaining"]} {order1["cost"]} {order1["fee"]}')
        
        s2_quantity = order1['amount']
        order2 = place_sell_order(scrip2, s2_quantity, scrip_prices[scrip2])
        if verbose in [True]: print('Order 2',order2)
        if verbose in ['trade','exe']: print(f'Order 2: {order2["symbol"]} {order2["side"]} {order2["status"]} {order2["price"]} {order2["amount"]} {order2["filled"]} {order2["remaining"]} {order2["cost"]} {order2["fee"]}')
        
        s3_quantity = order2['amount'] * scrip_prices[scrip2]
        order3 = place_sell_order(scrip3, s3_quantity, scrip_prices[scrip3])
        if verbose in [True]: print('Order 3',order3)
        if verbose in ['trade','exe']: print(f'Order 3: {order3["symbol"]} {order3["side"]} {order3["status"]} {order3["price"]} {order3["amount"]} {order3["filled"]} {order3["remaining"]} {order3["cost"]} {order3["fee"]}')
    
    if not os.path.exists(f'executions\TriBot_exec_{dt.datetime.today().date().strftime("%d%m%Y")}.csv'):
        with open(f'executions\TriBot_exec_{dt.datetime.today().date().strftime("%d%m%Y")}.csv', 'a') as f:
            f.write('tri_ID,date,arbitrage_type,pair_1, price_1, amount_1, cost1, fee1, average1, pair_2, price_2, amount_2, cost2, fee2, average2, pair_3, price_3, amount_3, cost3, fee3, average3, PnL, exe_time\n')
    
    end = time.time()

    OP_details =[
        "_".join([order1['id'],order2['id'],order3['id']]),
        dt.datetime.now().strftime('%d-%b-%Y %H:%M:%S.%f'),
        type,
        scrip1,
        order1['price'],
        order1['amount'],
        order1['cost'],
        order1['fee'],
        order1['average'],
        scrip2,
        order2['price'],
        order2['amount'],
        order2['cost'],
        order2['fee'],
        order2['average'],
        scrip3,
        order3['price'],
        order3['amount'],
        order3['cost'],
        order3['fee'],
        order3['average'],
        order1['cost'] - order3['cost'],
        end - start
        ]

    with open(f'executions\TriBot_exec_{dt.datetime.today().date().strftime("%d%m%Y")}.csv', 'a') as f:
        f.write(",".join(OP_details))

    final_amount = order3['cost']

    return (final_amount, OP_details[0])

# %% [markdown]
# Sample order from exchange immediately after execution:   
# {'info': {'id': '2490462375', 'symbol': 'btcusdt', 'type': 'limit', 'side': 'buy', 'status': 'wait', 'price': '43201.0', 'origQty': '0.002314', 'executedQty': '0.0', 'createdTime': '1646302254000', 'updatedTime': '1646302254000'}, 'id': '2490462375', 'clientOrderId': None, 'timestamp': 1646302254000, 'datetime': '2022-03-03T10:10:54.000Z', 'lastTradeTimestamp': 1646302254000, 'status': 'open', 'symbol': 'BTC/USDT', 'type': 'limit', 'timeInForce': None, 'postOnly': None, 'side': 'buy', 'price': 43201.0, 'amount': None, 'filled': 0.0, 'remaining': None, 'cost': 0.0, 'fee': None, 'average': None, 'trades': [], 'fees': []}

# %% [markdown]
# # STEP 4: WRAPPING IT TOGETHER

# %%
def perform_triangular_arbitrage(scrip1, scrip2, scrip3, arbitrage_type,investment_limit, min_profit_percentage,verbose= False):
    start_time = time.time()
    exectuted_return = ''
    tri_id = ''
    OP_return = 0.0
    if(arbitrage_type == 'BUY_BUY_SELL'):
        # Check this combination for triangular arbitrage: scrip1 - BUY, scrip2 - BUY, scrip3 - SELL
        op = check_buy_buy_sell(scrip1, scrip2, scrip3,investment_limit, verbose=verbose)
        if not op == None:
            OP_return, scrip_prices, scrip_amounts, scrip_costs = op
        else:
            return None
        
    elif(arbitrage_type == 'BUY_SELL_SELL'):
        # Check this combination for triangular arbitrage: scrip1 - BUY, scrip2 - SELL, scrip3 - SELL
        op = check_buy_sell_sell(scrip1, scrip2, scrip3,investment_limit, verbose=verbose)
        if not op == None:
            OP_return, scrip_prices, scrip_amounts, scrip_costs = op
        else:
            return None

    profit = check_profit_loss(OP_return,scrip_costs[scrip1], min_profit_percentage)

    result = f"{dt.datetime.now().strftime('%d-%b-%Y %H:%M:%S.%f')},"\
            f"{arbitrage_type}, {scrip1}, {scrip_prices[scrip1]}, {scrip_amounts[scrip1]},"\
            f"{scrip2}, {scrip_prices[scrip2]}, {scrip_amounts[scrip2]}, {scrip3}, {scrip_prices[scrip3]}, {scrip_amounts[scrip3]},"\
            f"{scrip_costs[scrip1]}, {OP_return}, {profit}"

    if profit:
        exectuted_return, tri_id = place_trade_orders(arbitrage_type, scrip1, scrip2, scrip3, investment_limit, scrip_prices)
        print(result)
    if verbose in [True, 'trade']: print(result)
    end_time = time.time()
    exe_time = end_time - start_time

    result += f',{exe_time},{exectuted_return},{tri_id}'
    return result

# %% [markdown]
# ## Execute the loop

# %%
verbose = 'trade' # True/False, error, trade, info
max_invested_amount = 100
MIN_PROFIT_percentage = 0.01

while(True):
    for combination in wx_combinations_usdt:

        base = combination['base']
        intermediate = combination['intermediate']
        ticker = combination['ticker']
        combination_ID = '/'.join([base, intermediate, ticker]) # Eg: "USDT/BTC/ETH"

        s1 = f'{intermediate}/{base}'    # Eg: BTC/USDT
        s2 = f'{ticker}/{intermediate}'  # Eg: ETH/BTC
        s3 = f'{ticker}/{base}'          # Eg: ETH/USDT

        try:

            # Set max investment amount
            wallet = exchange.fetchBalance()
            if verbose in [True, 'info', 'trade']: print(f'USDT free= {wallet["USDT"]["free"]} max limit investment= {max_invested_amount}')
            if wallet['USDT']['free'] > max_invested_amount:
                max_invested_amount = 100
            else:
                max_invested_amount = wallet['USDT']['free']

            # Check triangular arbitrage for buy-buy-sell 
            bbs = perform_triangular_arbitrage(s1,s2,s3,'BUY_BUY_SELL',max_invested_amount, MIN_PROFIT_percentage, verbose=verbose)

            if not os.path.exists(f'output\TriBot_output_{dt.datetime.today().date().strftime("%d%m%Y")}.csv'):
                with open(f'output\TriBot_output_{dt.datetime.today().date().strftime("%d%m%Y")}.csv', 'a') as f:
                    f.write('combination_ID,date,arbitrage_type,pair_1,price_1,amount_1,pair_2,price_2,amount_2,pair_3,price_3,amount_3,initial_amount,OP_return,Profitable,exe_time,executed_return,tri_id\n')
            
            if not bbs == None:
                with open(f'output\TriBot_output_{dt.datetime.today().date().strftime("%d%m%Y")}.csv', 'a') as f:
                    f.write(combination_ID+','+bbs+'\n')


            # Check triangular arbitrage for buy-sell-sell 
            bss = perform_triangular_arbitrage(s3,s2,s1,'BUY_SELL_SELL',max_invested_amount, MIN_PROFIT_percentage, verbose=verbose)
            if not bss == None:
                with open(f'output\TriBot_output_{dt.datetime.today().date().strftime("%d%m%Y")}.csv', 'a') as f:
                    f.write(combination_ID+','+bss+'\n')
            
            errCatch = 0                                    # Restart error counter after complete execution without exceptions
            
        except ccxt.NetworkError as err:
            if verbose in [True,'error']: print(f'Network error: {err}')
            if errCatch == 1:
                if verbose in [True,'error']: print(f'Error catch {errCatch} Sleeping 30 minutes')
                time.sleep(30*60)
                errCatch +=1
            elif errCatch == 2:
                if verbose in [True,'error']: print(f'Error catch {errCatch} Sleeping 60 minutes')
                time.sleep(60*60)
                errCatch += 1
            elif errCatch == 3:
                if verbose in [True,'error']: print(f'Error catch {errCatch} Sleeping 90 minutes')
                time.sleep(90*60)
                errCatch += 1
            else:
                if verbose in [True,'error']: print(f'Error catch {errCatch}  BREAK!')
                break
            
        except ccxt.ExchangeError as err:
            if verbose in [True,'error']: print(f'Network error: {err}')
            if errCatch <= 5:
                if verbose in [True,'error']: print(f'Error catch {errCatch} Sleeping 5 minutes')
                time.sleep(5*60)
                errCatch +=1
            else:
                break

# %% [markdown]
# # Improvements
# OP Prices from the top bid and ask [DONE]  
# OP Amoint from the top bid and ask [DONE]  
# Minimun volumen to trade per pair [DONE]  
# Calculate fee per pair and apply it [DONE]  
# Handle Strict limits via API [suppose to be done with exchange.enableLimit = True][Testing]  
# Implement REAL order execution
# Take functions to a .py  
# Get Pair combinations independently of the position of the asset (base or quote)  
# Get OP prices by ponderating bids and ask from the order book until the max of investment  
# 
# 
# # Bugfixing
# Execution time: is calculate in each OP check but result is always negative. [DONE]  
# OP2 in BSS is amount/price instead of amount*price [20/05/22] [Tesing]  
# TimeOut Error appers after a while running. Added an Error Catch [22/05/22][Testing]  


