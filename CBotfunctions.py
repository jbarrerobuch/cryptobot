# IMPORTS

import ccxt
import pandas as pd
import time
import datetime as dt
from config import myconfig
import os
import json

class Tribot ():

    def __init__(self,exchange_name, api_key, api_secret, sandbox_net=False, test_mode=True) -> None:

        self.exchange_name = exchange_name # [binance, bitfinex]

        # Verbose logging
        self.verbose_levels = {
            "all": ["all"],
            "info": ["info", "error"],
            "error": ["error"]
            }
        self.verbose = "Info"

        # Execution mode: test mode on to bypass sending trading orders
        self.test_mode = test_mode

        # Trading data
        self.investment_limit = 100
        self.run_summary = {
            "total_checks": 0,
            "profitable_trades": 0,
            "start_timestamp": dt.datetime.now()
        }

        # Exchange parameters
        self.exchange = ccxt.Exchange()
        self.init_exchange(exchange_name=self.exchange_name, api_key=api_key, api_secret=api_secret, sandbox_net=sandbox_net)


        # Market info
        self.markets = pd.DataFrame()
        self.get_markets()
        self.init_assets = ["USDT", "BUSD", "USD", "EUR"]
        self.combinations = []
        self.get_crypto_combinations()
        self.trading_fees = pd.DataFrame()
        self.get_fees()


    def init_exchange(self, exchange_name, api_key, api_secret, enable_rate_limit=True, sandbox_net=True):
        if self.exchange_name == "binance":
            self.exchange = ccxt.binance({
                "apiKey": api_key,
                "secret": api_secret,
                "recWindow":10000
            })
            self.exchange.enableRateLimit = enable_rate_limit
            self.exchange.set_sandbox_mode(sandbox_net)
        elif self.exchange_name == "bitfinex":
            self.exchange = ccxt.bitfinex2({
                "apiKey": api_key,
                "secret": api_secret,
                "recWindow":10000
            })
            self.exchange.enableRateLimit = enable_rate_limit
            self.exchange.set_sandbox_mode(sandbox_net)
        
        print(f"Initialize {self.exchange_name}\nRate limit: {enable_rate_limit}\nSandbox: {sandbox_net}\nTest mode: {self.test_mode}\n")
    
    def get_markets(self):

        # GET MARKETS and Load in a data frame
        data_markets = pd.DataFrame.from_dict(self.exchange.fetchMarkets())

        # Drop inactive markets
        data_markets =  data_markets.loc[data_markets["active"] == True]

        # Set the symbol column as Dataframe's index
        data_markets.set_index("symbol", verify_integrity= True, inplace= True,drop= False)

        # Get only spot markets
        data_markets = data_markets[data_markets.type == "spot"]


        # Extract order_amount limits
        amounts_df = data_markets["limits"].apply(pd.Series)["amount"].apply(pd.Series)
        amounts_df.rename(columns={"min":"amount_min", "max":"amount_max"}, inplace= True)

        # Extract order_cost limits
        cost_df = data_markets["limits"].apply(pd.Series)["cost"].apply(pd.Series)
        cost_df.rename(columns={"min": "cost_min", "max": "cost_max"}, inplace= True)

        self.markets = pd.concat([data_markets, amounts_df, cost_df], axis=1)
        if self.verbose in self.verbose_levels["all"]: print(f"{len(self.markets)} spot markets available")


    def get_fees(self):
        self.trading_fees = pd.DataFrame.from_dict(self.exchange.fetch_trading_fees(),orient="index")


   
    def get_crypto_combinations(self):
         # STEP 1: GET ALL THE CRYPTO COMBINATIONS FOR USDT
        market_symbols = self.markets.symbol
        combos = []

        for init_asset in self.init_assets:
            for symbol1 in market_symbols:
                base_curr1 = symbol1.split("/")[0]
                quote_curr1 = symbol1.split("/")[1]
                
                if (quote_curr1 == init_asset):
                    for symbol2 in market_symbols:
                        base_curr2 = symbol2.split("/")[0]
                        quote_curr2 = symbol2.split("/")[1]
                        if (base_curr1 == quote_curr2):
                            for symbol3 in market_symbols:
                                base_curr3 = symbol3.split("/")[0]
                                quote_curr3 = symbol3.split("/")[1]
                                if((base_curr2 == base_curr3) and (quote_curr3 == quote_curr1)):
                                    combination = {
                                        "base":quote_curr1,
                                        "intermediate":base_curr1,
                                        "ticker":base_curr2,
                                    }
                                    combos.append(combination)
        
        self.combinations = pd.DataFrame(combos)
        self.combinations["id"] = self.combinations['base'] + "_" + self.combinations['intermediate'] + "_" + self.combinations['ticker']
        self.combinations = self.combinations.set_index("id", verify_integrity=True)
        self.combinations["score"] = 0

    # STEP 2: PERFORM TRIANGULAR ARBITRAGE
    # Utility method to fetch the pondered price
    # Given a trading amount, the price of the operation is calculated from the order book.
    def get_pondered_price(self, symbol:str, trade:str, op_value:float):
        """Returns a dict of dict. Containing information to trade an specific value.
        symbol: is the pair symbol analyzed.
        trade: "buy" or "sell", operation type.
        op_value: value to be traded.
        verbose: default False.

        If want to trade 100 USDT for BTC -> symbol ="BTC/USDT", trade="buy", op_value=100
        If want to trade 0.001 BTC for USD -> symbol ="BTC/USDT", trade="sell", op_value=0.001
        If want to trade BTC for 100 USDT worth [NOT IMPLEMENTED]
        
        Return example:
        {"BTC/USDT": {"pondered_price": 28392.33, "limit_price": 28392.33, "total_value": 100, "total_amount": 0.003522077969648845}}
        """
        # retrieve the order book for the specified symbol
        order_book = self.exchange.fetch_order_book(symbol)

        # use the asks or bids order book depending on the trade type
        if trade == "buy":
            order_book_data = order_book["asks"]
        elif trade == "sell":
            order_book_data = order_book["bids"]
        else:
            raise ValueError("Invalid trade type. Must be buy or sell.")

        # convert the amount to the quote currency if we"re buying, or the base currency if we"re selling
        base_currency = symbol.split("/")[0]
        quote_currency = symbol.split("/")[1]

        if trade == "buy":
            cost_to_trade = op_value

            if self.verbose in self.verbose_levels["all"]: print(f"\nTo {trade} {cost_to_trade} {quote_currency} of {base_currency}")

        elif trade == "sell":
            amount_to_trade = op_value

            if self.verbose in self.verbose_levels["all"]: print(f"\nTo {trade} {amount_to_trade} {base_currency} of {quote_currency}")

        # calculate the total value of the order book bids up to the amount we want to trade
        total_cost_accumulated = 0
        total_amount_accumulated = 0
        for order in order_book_data:
            price, amount_available = order

            if trade == "buy":

                # calculate the cost in quote currency
                cost_available = price * amount_available

                #if self.verbose in self.verbose_levels["all"]: 
                #    print(f"Price: {price} Amount avail.: {amount_available} cost avail.: {cost_available}")
                #    print(f"Accumulated cost: {total_cost_accumulated} Accumulated amount: {total_amount_accumulated}")

                if total_cost_accumulated + cost_available >= cost_to_trade:
                    # this ask would put us over the amount we want to trade

                    remaining_cost = cost_to_trade - total_cost_accumulated
                    remaining_amount = remaining_cost / price
                    #if self.verbose in self.verbose_levels["all"]:
                    #    print("available to buy more than needed")
                    #    print(f"cost to trade = trade cost quote - total cost {remaining_cost} = {cost_to_trade} - {total_cost_accumulated}")
                    #    print(f"amount to trade: {remaining_amount}")
                    #    print(f"Accumulated cost {total_cost_accumulated}, Accumulated amount {total_amount_accumulated}\n")
                    total_cost_accumulated += remaining_cost
                    total_amount_accumulated += remaining_amount
                    limit_price = price

                    break

                else:
                    total_amount_accumulated += amount_available
                    total_cost_accumulated += cost_available

            elif trade == "sell":

                # calculate the value available is the same as the amount
                #if self.verbose in self.verbose_levels["all"]:
                #    print(f"Price: {price} Amount avail.: {amount_available}")
                #    print(f"Accumulated cost: {total_cost_accumulated} Accumulated amount: {total_amount_accumulated}")


                if total_amount_accumulated + amount_available >= amount_to_trade:

                    # this bid would put us over the amount we want to trade
                    remaining_amount = amount_to_trade - total_amount_accumulated
                    #if self.verbose in self.verbose_levels["all"]:
                    #    print("available to sell more than needed")
                    #    print(f"Amount to trade = trade amount base - total amount {remaining_amount} = {amount_to_trade} - {total_amount_accumulated}")
                    #    print(f"Accumulated cost {total_cost_accumulated}, Accumulated amount {total_amount_accumulated}\n")
                    total_cost_accumulated += price * remaining_amount
                    total_amount_accumulated += remaining_amount
                    limit_price = price
                    break
                else:
                    total_amount_accumulated += amount_available
                    total_cost_accumulated += price * amount_available

        # calculate the pondered price as the total value divided by the amount we want to trade
        try:
            pondered_price = total_cost_accumulated / total_amount_accumulated
        except ZeroDivisionError as err:
            if self.verbose in self.verbose_levels["error"]: print(f"\n{err}\nOrder book for {symbol} - {trade}:\n{order_book_data}\n")
            pondered_price = 0

        if self.verbose in self.verbose_levels["all"]: print(f"pondered price: {pondered_price} = {total_cost_accumulated} / {total_amount_accumulated}\n")
        
        return {symbol: {
                    "pondered_price": pondered_price,
                    "limit_price": limit_price,
                    "total_cost": total_cost_accumulated,
                    "total_amount": total_amount_accumulated}}


    # Triangular Arbitrage
    def check_buy_buy_sell(self, pair1, pair2, pair3, investment_limit):
        if self.verbose in self.verbose_levels["all"]: print(f"\nChecking buy_buy_sell {pair1} {pair2} {pair3}\n")

        fullfetch = {}
        
        fullfetch[pair1] = self.get_pondered_price(pair1, trade="buy", op_value= investment_limit)[pair1]
        fullfetch[pair1]["total_amount"] -= fullfetch[pair1]["total_amount"] * self.trading_fees.loc[pair1]["taker"] # take the trading fees from the amount result
        fullfetch[pair2] = self.get_pondered_price(pair2, trade="buy", op_value= fullfetch[pair1]["total_amount"])[pair2]
        fullfetch[pair2]["total_amount"] -= fullfetch[pair2]["total_amount"] * self.trading_fees.loc[pair2]["taker"] # take the trading fees from the amount result
        fullfetch[pair3] = self.get_pondered_price(pair3, trade="sell", op_value= fullfetch[pair2]["total_amount"])[pair3]
        fullfetch[pair3]["total_cost"] -= fullfetch[pair3]["total_cost"] * self.trading_fees.loc[pair3]["taker"] # take the trading fees from the value result

        if self.verbose in self.verbose_levels["all"]:
            print(f"{pair1}: {fullfetch[pair1]}")
            print(f"{pair2}: {fullfetch[pair2]}")
            print(f"{pair3}: {fullfetch[pair3]}\n")

        # Max investment in amount according to the order book available
        investment_max_limit_amount = fullfetch[pair1]["total_amount"]
        if self.verbose in self.verbose_levels["all"]: print(f"Max investment: cost/amount {investment_limit}/{investment_max_limit_amount}")

        OP_return = fullfetch[pair3]["total_cost"]
        scrip_prices = {pair1 : fullfetch[pair1]["limit_price"], pair2 : fullfetch[pair2]["limit_price"], pair3 : fullfetch[pair3]["limit_price"]}
        scrip_amounts = {pair1: fullfetch[pair1]["total_amount"], pair2: fullfetch[pair2]["total_amount"], pair3: fullfetch[pair3]["total_amount"]}
        scrip_costs = {pair1: fullfetch[pair1]["total_cost"], pair2: fullfetch[pair2]["total_cost"], pair3: fullfetch[pair3]["total_cost"]}

        return OP_return, scrip_prices, scrip_amounts, scrip_costs


    def check_buy_sell_sell(self, pair1, pair2, pair3, investment_limit):
        if self.verbose in self.verbose_levels["all"]: print(f"\nChecking buy_sell_sell {pair1} {pair2} {pair3}\n")

        fullfetch = {}

        fullfetch[pair1] = self.get_pondered_price(pair1, trade="buy", op_value= investment_limit)[pair1]
        fullfetch[pair1]["total_amount"] -= fullfetch[pair1]["total_amount"] * self.trading_fees.loc[pair1]["taker"] # take the trading fees from the amount result
        fullfetch[pair2] = self.get_pondered_price(pair2, trade="sell", op_value= fullfetch[pair1]["total_amount"])[pair2]
        fullfetch[pair2]["total_cost"] -= fullfetch[pair2]["total_cost"] * self.trading_fees.loc[pair2]["taker"] # take the trading fees from the amount result
        fullfetch[pair3] = self.get_pondered_price(pair3, trade="sell", op_value= fullfetch[pair2]["total_cost"])[pair3]
        fullfetch[pair3]["total_cost"] -= fullfetch[pair3]["total_cost"] * self.trading_fees.loc[pair3]["taker"] # take the trading fees from the amount result

        if self.verbose in self.verbose_levels["all"]:
            print(f"{pair1}: {fullfetch[pair1]}")
            print(f"{pair2}: {fullfetch[pair2]}")
            print(f"{pair3}: {fullfetch[pair3]}\n")

        #Max investment in base amount
        investment_max_limit_amount = fullfetch[pair1]["total_amount"]
        if self.verbose in self.verbose_levels["all"]: print(f"Max investment: cost/amount {investment_limit}/{investment_max_limit_amount}")

        OP_return = fullfetch[pair3]["total_cost"]
        scrip_prices = {pair1 : fullfetch[pair1]["limit_price"], pair2 : fullfetch[pair2]["limit_price"], pair3 : fullfetch[pair3]["limit_price"]}
        scrip_amounts = {pair1: fullfetch[pair1]["total_amount"], pair2: fullfetch[pair2]["total_amount"], pair3: fullfetch[pair3]["total_amount"]}
        scrip_costs = {pair1: fullfetch[pair1]["total_cost"], pair2: fullfetch[pair2]["total_cost"], pair3: fullfetch[pair3]["total_cost"]}

        return OP_return, scrip_prices, scrip_amounts, scrip_costs


    def check_profit_loss(self, OP_return, initial_investment, min_profit):
        min_profit = initial_investment * (1 + min_profit)
        profit = (OP_return >= min_profit)
        return profit


    # STEP 3: PLACE THE TRADE ORDERS
    def place_buy_order(self, scrip, quantity, limit, slippage = 0):
        quantity = float(self.exchange.amount_to_precision (scrip, quantity))
        limit =  float(self.exchange.price_to_precision(scrip,limit * (1+ slippage)))
        order = self.exchange.create_limit_buy_order(scrip, quantity, limit)
        return order

    def place_sell_order(self, scrip, quantity, limit, slippage = 0):
        quantity = float(self.exchange.amount_to_precision(scrip, quantity))
        limit = float(self.exchange.price_to_precision(scrip,limit * (1 - slippage)))
        order = self.exchange.create_limit_sell_order(scrip, quantity, limit)
        return order 

    def place_trade_orders(self, type, Pair1, Pair2, Pair3, investment_limit, scrip_prices, OP_ID= "NA", slippage_trade1= 0, slippage_trade2= 0, slippage_trade3= 0):
        if self.verbose in self.verbose_levels["all"]: print("----Placing orders----")
        start = time.time()

        # Check if logger exists or create it        
        if not os.path.exists(f"executions\TriBot_{self.exchange_name}_exec_{dt.datetime.today().date().strftime('%d%m%Y')}.csv"):
            with open(f"executions\TriBot_{self.exchange_name}_exec_{dt.datetime.today().date().strftime('%d%m%Y')}.csv", "a") as f:
                f.write("OP_ID,date,arbitrage_type,pair_1, price_1, amount_1, cost1, fee1, average1, pair_2, price_2, amount_2, cost2, fee2, average2, pair_3, price_3, amount_3, cost3, fee3, average3, PnL, exe_time\n")

        # Placing orders for BUY BUY SELL leg
        if type == "BUY_BUY_SELL":
            
            # Trade 1
            Pair1_amount = investment_limit/scrip_prices[Pair1]
            trade1 = self.place_buy_order(Pair1, Pair1_amount, scrip_prices[Pair1], slippage=slippage_trade1)

            # Handle the execution
            status = trade1["status"]
            waiter = 0
            # Check every 0.5 second until the trade 1 is complete otherwise cancel the trade.
            while (not status == "closed"):
                if waiter < 10:
                    if self.verbose in self.verbose_levels["all"]: print("Waiting to close order1, sleeping 0.5 sec")
                    time.sleep(0.5)
                    waiter += 1
                    trade1 = self.exchange.fetch_order(trade1["info"]["orderId"],trade1["symbol"])
                    status = trade1["status"]
                else: # Trade 2 will continue with the traded amount
                    if self.verbose in self.verbose_levels["all"]: print("trade not filled in 5 seconds, cancelling")
                    self.exchange.cancel_order(trade1["info"]["orderId"],trade1["symbol"])
                    trade1 = self.exchange.fetch_order(trade1["info"]["orderId"],trade1["symbol"])

            waiter = 0
            if self.verbose in self.verbose_levels["all"]: print("Trade1 closed updated order info")

            # Log trade 1
            trade1["OP_ID"] = OP_ID
            jOrder = json.dumps(trade1)
            with open(f"executions\TriBot_{self.exchange_name}_exec_{dt.datetime.today().date().strftime('%d%m%Y')}.csv", "a") as f:
                f.write(jOrder)

            if trade1["fee"] == None: trade1["fee"] = {"cost": 0}
            if self.verbose in self.verbose_levels["all"]: print(trade1)
            if self.verbose in self.verbose_levels["all"]: print(f"Order 1: {trade1['symbol']} order:{trade1['side']} status:{trade1['status']} price:{trade1['price']} amount:{trade1['amount']} filled:{trade1['filled']} remaining:{trade1['remaining']} cost:{trade1['cost']} fee:{trade1['fee']}")
            
            # Trade 2
            s2_quantity = float(trade1["filled"])/scrip_prices[Pair2]
            order2 = self.place_buy_order(Pair2, s2_quantity, scrip_prices[Pair2], slippage= slippage_trade2)

            # Handle the execution
            status = order2["status"]
            waiter = 0
            while (not status == "closed"):
                if waiter < 10:
                    if self.verbose in self.verbose_levels["all"]: print("Waiting to close order2, sleeping 1 sec")
                    time.sleep(1)
                    waiter += 1
                    order2 = self.exchange.fetch_order(order2["info"]["orderId"],order2["symbol"])
                    order2["OP_ID"] = OP_ID
                    status = order2["status"]
                else:
                    if self.verbose in self.verbose_levels["all"]: print("trade not filled in 10 seconds, cancelling")
                    self.exchange.cancel_order(order2["info"]["orderId"],order2["symbol"])
                    order2 = self.exchange.fetch_order(order2["info"]["orderId"],order2["symbol"])

            waiter = 0
            if self.verbose in self.verbose_levels["all"]: print("Order2 closed updated order info")
            # Log trade 2
            order2["OP_ID"] = OP_ID
            jOrder = json.dumps(order2)
            with open(f"executions\TriBot_{self.exchange_name}_exec_{dt.datetime.today().date().strftime('%d%m%Y')}.csv", "a") as f:
                f.write(jOrder)

            if order2["fee"] == None: trade1["fee"] = {"cost": 0}
            if self.verbose in self.verbose_levels["all"]: print("Order 2",order2)
            if self.verbose in self.verbose_levels["all"]: print(f"Order 2: {order2['symbol']} order:{order2['side']} status:{order2['status']} price:{order2['price']} amount:{order2['amount']} filled:{order2['filled']} remaining{order2['remaining']} cost:{order2['cost']} fee:{order2['fee']}")
            
            # Trade 3
            s3_quantity = float(order2["filled"])
            order3 = self.place_sell_order(Pair3, s3_quantity, scrip_prices[Pair3], slippage= slippage_trade3)

            # Handle the execution
            status = order3["status"]
            waiter = 0
            while (not status == "closed"):
                if waiter < 10:
                    if self.verbose in self.verbose_levels["all"]: print("Waiting to close order3, sleeping 1 sec")
                    time.sleep(1)
                    waiter += 1
                    order3 = self.exchange.fetch_order(order3["info"]["orderId"],order3["symbol"])
                    order3["OP_ID"] = OP_ID
                    status = order3["status"]
                else:
                    if self.verbose in self.verbose_levels["all"]: print("trade not filled in 10 seconds, cancelling")
                    self.exchange.cancel_order(order3["info"]["orderId"],order3["symbol"])
                    order3 = self.exchange.fetch_order(order3["info"]["orderId"],order3["symbol"])

            waiter = 0
            if self.verbose in self.verbose_levels["all"]: print("Order3 closed updated order info")
            # Log trade 3
            order3["OP_ID"] = OP_ID
            jOrder = json.dumps(order3)
            with open(f"executions\TriBot_{self.exchange_name}_exec_{dt.datetime.today().date().strftime('%d%m%Y')}.csv", "a") as f:
                f.write(jOrder)

            if order3["fee"] == None: order3["fee"] = {"cost": 0}
            if self.verbose in self.verbose_levels["all"]: print("Order 3",order3)
            if self.verbose in self.verbose_levels["all"]: print(f"Order 3: {order3['symbol']} order:{order3['side']} status:{order3['status']} price:{order3['price']} amount:{order3['amount']} filled:{order3['filled']} remaining{order3['remaining']} cost:{order3['cost']} fee:{order3['fee']}")
            
        elif type == "BUY_SELL_SELL":
            #trade 1
            Pair1_amount = investment_limit/scrip_prices[Pair1]
            trade1 = self.place_buy_order(Pair1, Pair1_amount, scrip_prices[Pair1],slippage= slippage_trade1)

            # Handle the execution
            status = trade1["status"]
            waiter = 0
            while (not status == "closed"):
                if waiter < 10:
                    if self.verbose in self.verbose_levels["all"]: print("Waiting to close order1, sleeping 1 sec")
                    time.sleep(1)
                    waiter += 1
                    trade1 = self.exchange.fetch_order(trade1["info"]["orderId"],trade1["symbol"])
                    status = trade1["status"]
                else:
                    if self.verbose in self.verbose_levels["all"]: print("trade not filled in 10 seconds, cancelling")
                    self.exchange.cancel_order(trade1["info"]["orderId"],trade1["symbol"])
                    trade1 = self.exchange.fetch_order(trade1["info"]["orderId"],trade1["symbol"])

            waiter = 0
            if self.verbose in self.verbose_levels["all"]: print("Order1 closed updated order info")
            # Log trade 1
            trade1["OP_ID"] = OP_ID
            jOrder = json.dumps(trade1)
            with open(f"executions\TriBot_{self.exchange_name}_exec_{dt.datetime.today().date().strftime('%d%m%Y')}.csv", "a") as f:
                f.write(jOrder)
            
            if trade1["fee"] == None: trade1["fee"] = {"cost": 0}           
            if self.verbose in self.verbose_levels["all"]: print(trade1)
            if self.verbose in self.verbose_levels["all"]: print(f"Order 1: {trade1['symbol']} order:{trade1['side']} status:{trade1['status']} price:{trade1['price']} amount:{trade1['amount']} filled:{trade1['filled']} remaining{trade1['remaining']} cost:{trade1['cost']} fee:{trade1['fee']}")
            
            #trade 2
            s2_quantity = trade1["filled"]
            order2 = self.place_sell_order(Pair2, s2_quantity, scrip_prices[Pair2], slippage= slippage_trade2)

            # Handle the execution
            status = order2["status"]
            waiter = 0
            while (not status == "closed"):
                if waiter < 10:
                    if self.verbose in self.verbose_levels["all"]: print("Waiting to close order2, sleeping 1 sec")
                    time.sleep(1)
                    waiter += 1
                    order2 = self.exchange.fetch_order(order2["info"]["orderId"],order2["symbol"])
                    order2["OP_ID"] = OP_ID
                    status = order2["status"]
                else:
                    if self.verbose in self.verbose_levels["all"]: print("trade not filled in 10 seconds, cancelling")
                    self.exchange.cancel_order(order2["info"]["orderId"],order2["symbol"])
                    order2 = self.exchange.fetch_order(order2["info"]["orderId"],order2["symbol"])

            waiter = 0
            if self.verbose in [True, "trade", "info"]: print("Order2 closed updated order info")
            # Log trade 2
            order2["OP_ID"] = OP_ID
            jOrder = json.dumps(order2)
            with open(f"executions\TriBot_{self.exchange_name}_exec_{dt.datetime.today().date().strftime('%d%m%Y')}.csv", "a") as f:
                f.write(jOrder)

            if order2["fee"] == None: trade1["fee"] = {"cost": 0}
            if self.verbose in [True]: print("Order 2",order2)
            if self.verbose in ["trade","info"]: print(f"Order 2: {order2['symbol']} order:{order2['side']} status:{order2['status']} price:{order2['price']} amount:{order2['amount']} filled:{order2['filled']} remaining{order2['remaining']} cost:{order2['cost']} fee:{order2['fee']}")
            
            #trade 3
            s3_quantity = order2["filled"] * scrip_prices[Pair2]
            order3 = self.place_sell_order(Pair3, s3_quantity, scrip_prices[Pair3], slippage= slippage_trade3)

            # Handle the execution
            status = order3["status"]
            waiter = 0
            while (not status == "closed"):
                if waiter < 10:
                    if self.verbose in [True, "trade"]: print("Waiting to close order3, sleeping 1 sec")
                    time.sleep(1)
                    waiter += 1
                    order3 = self.exchange.fetch_order(order3["info"]["orderId"],order3["symbol"])
                    status = order3["status"]
                else:
                    if self.verbose in self.verbose_levels["all"]: print("trade not filled in 10 seconds, cancelling")
                    self.exchange.cancel_order(order3["info"]["orderId"],order3["symbol"])
                    order3 = self.exchange.fetch_order(order3["info"]["orderId"],order3["symbol"])

            waiter = 0
            if self.verbose in [True, "trade", "info"]: print("Order3 closed updated order info")
            # Log trade 3
            order3["OP_ID"] = OP_ID
            jOrder = json.dumps(order3)
            with open(f"executions\TriBot_{self.exchange_name}_exec_{dt.datetime.today().date().strftime('%d%m%Y')}.csv", "a") as f:
                f.write(jOrder)
            
            if order3["fee"] == None: order3["fee"] = {"cost": 0}
            if self.verbose in [True]: print("Order 3",order3)
            if self.verbose in ["trade","info"]: print(f"Order 3: {order3['symbol']} order:{order3['side']} status:{order3['status']} price:{order3['price']} amount:{order3['amount']} filled:{order3['filled']} remaining{order3['remaining']} cost:{order3['cost']} fee:{order3['fee']}")
        
        end = time.time()

        # Gathering of operation details to be saved in summary
        OP_details =[
            "_".join([trade1["info"]["orderId"],order2["info"]["orderId"],order3["info"]["orderId"]]),
            dt.datetime.now().strftime("%d-%b-%Y %H:%M:%S.%f"),
            type,
            Pair1,
            str(trade1["price"]),
            str(trade1["amount"]),
            str(trade1["cost"]),
            str(trade1["fee"]["cost"]),
            str(trade1["average"]),
            Pair2,
            str(order2["price"]),
            str(order2["amount"]),
            str(order2["cost"]),
            str(order2["fee"]["cost"]),
            str(order2["average"]),
            Pair3,
            str(order3["price"]),
            str(order3["amount"]),
            str(order3["cost"]),
            str(order3["fee"]["cost"]),
            str(order3["average"]),
            str(order3["cost"] - trade1["cost"]),
            str(end - start)
            ]
        OP_details = ",".join(OP_details)+"\n"

        # Write operations details in csv log
        with open(f"summary\TriBot_{self.exchange_name}_summary_{dt.datetime.today().date().strftime('%d%m%Y')}.csv", "a") as f:
            print(OP_details)
            f.write(OP_details)

        final_amount = order3["cost"]

        if self.verbose in self.verbose_levels["all"]: print("----End placing orders----")

        return (final_amount, OP_details[0])

    # STEP 4: WRAPPING IT TOGETHER

    def perform_triangular_arbitrage(self, pair1, pair2, pair3, arbitrage_type,investment_limit, min_profit_percentage, slippage_trade1=0, slippage_trade2=0, slippage_trade3=0):
        start_time = time.time()
        combination_ID = "_".join([pair1.split("/")[1], pair2.split("/")[1], pair3.split("/")[0]])
        executed_return = ""
        OP_ID = ""
        OP_return = 0.0

        # Check this combination for triangular arbitrage: Pair1 - BUY, Pair2 - BUY, Pair3 - SELL
        if arbitrage_type == "BUY_BUY_SELL":
            OP_ID = f"BBS_{pair1}_{pair2}_{pair3}" # Initialize Operation ID
            op = self.check_buy_buy_sell(pair1=pair1, pair2=pair2, pair3=pair3, investment_limit=investment_limit)

        
        # Check this combination for triangular arbitrage: Pair1 - BUY, Pair2 - SELL, Pair3 - SELL
        elif arbitrage_type == "BUY_SELL_SELL":
            OP_ID = f"BSS_{pair1}_{pair2}_{pair3}" # Initialize Operation ID
            op = self.check_buy_sell_sell(pair1=pair1, pair2=pair2, pair3=pair3,investment_limit=investment_limit)


        if not op == None:
            OP_return, pair_prices, trade_amounts, trade_costs = op
            profit = self.check_profit_loss(OP_return,trade_costs[pair1], min_profit_percentage)
        else:
            pair_prices = {pair1: 0,
                           pair2: 0,
                           pair3: 0}
            trade_amounts = {pair1: 0,
                           pair2: 0,
                           pair3: 0}
            trade_costs = {pair1: 0,
                           pair2: 0,
                           pair3: 0}
            profit = None

        result = f"{dt.datetime.now().strftime('%d-%b-%Y %H:%M:%S.%f')},"\
                f"{arbitrage_type}, {pair1}, {pair_prices[pair1]}, {trade_amounts[pair1]},"\
                f"{pair2}, {pair_prices[pair2]}, {trade_amounts[pair2]}, {pair3}, {pair_prices[pair3]}, {trade_amounts[pair3]},"\
                f"{trade_costs[pair1]}, {OP_return}, {profit}"

        if self.verbose in self.verbose_levels["all"]: print(f"\n{result}\n")

        if profit:
            OP_ID += str(dt.datetime.now()) # add date time to the Operation ID
            self.run_summary["profitable_trades"] +=1
            if self.test_mode:
                executed_return = 0000
            else:
                executed_return, tri_id = self.place_trade_orders(arbitrage_type, pair1, pair2, pair3, investment_limit, pair_prices, OP_ID= OP_ID, slippage_trade1= slippage_trade1, slippage_trade2= slippage_trade2, slippage_trade3= slippage_trade3)

            self.combinations.loc[self.combinations.index == combination_ID, ["score"]] += 10
            if self.verbose in self.verbose_levels["all"]: print(f"\n{result}\n")
            if self.verbose in self.verbose_levels["all"]: print(f"Profit in {combination_ID} {profit}")
        else:
            self.combinations.loc[self.combinations.index == combination_ID, ["score"]] -= 1
        

        end_time = time.time()
        execute_duration = end_time - start_time

        # Handle execution without placing orders
        try:
            result += f",{execute_duration},{executed_return},{OP_ID}"
        except:
            result += f",{execute_duration},,{OP_ID}"

        return result

    # Execute the loop
    def start_trading(self, initial_investment=100, verbose=False):
        self.verbose = verbose # True/False, error, trade, info
        self.investment_limit = initial_investment

        MIN_PROFIT_percentage = 0.001
        errCatch = 0
        slippage_trade1 = 0.000 # Percentage
        slippage_trade2 = 0.000 # Percentage
        slippage_trade3 = 0.000 # Percentage

        # Build the list of combination

        init_assets = ["USDT"]
        self.run_summary["total checks"] = 0


        while(True):

            if self.run_summary["total checks"] % 50 == 0:
                wallet = self.exchange.fetchBalance()
                if self.verbose in self.verbose_levels["all"]:
                    for asset in init_assets:
                        print("\n----------------------------------------------------------")
                        print(f"{asset} balance= {wallet[asset]} max limit investment= {self.investment_limit}")
                        print(f"Checks: {self.run_summary['total_checks']} Profit trades: {self.run_summary['profitable_trades']}") 
                        print(f"Running time(Hours): {(dt.datetime.now()-self.run_summary['start_timestamp']).total_seconds()/3600}")
                        print(f"No. of combinations with {self.init_assets}: {len(self.combinations)}")
                        print("----------------------------------------------------------\n")

            #Sort combination by score, descending and work on top score combination
            self.combinations = self.combinations.sort_values(by="score", ascending=False)
            combination = self.combinations.iloc[0]
            print(f"-------[ {combination.name} - score: {combination.score} ]-------")
            if errCatch >= 3: break

            init_asset = combination["base"]
            intermediate = combination["intermediate"]
            end_asset = combination["ticker"]
            combination_ID = "_".join([init_asset, intermediate, end_asset]) # Eg: "USDT_BTC_ETH"


            s1 = f"{intermediate}/{init_asset}"    # Eg: BTC/USDT
            s2 = f"{end_asset}/{intermediate}"  # Eg: ETH/BTC
            s3 = f"{end_asset}/{init_asset}"          # Eg: ETH/USDT
            try:
                #if wallet["USDT"]["free"] > max_invested_amount:
                #    max_invested_amount = 100
                #else:
                #    max_invested_amount = wallet["USDT"]["free"]

                # Check triangular arbitrage for buy-buy-sell
                bbs = self.perform_triangular_arbitrage(pair1=s1, pair2=s2, pair3=s3,
                                                        arbitrage_type="BUY_BUY_SELL",
                                                        investment_limit=self.investment_limit,
                                                        min_profit_percentage=MIN_PROFIT_percentage,
                                                        slippage_trade1=slippage_trade1, 
                                                        slippage_trade2=slippage_trade2,
                                                        slippage_trade3=slippage_trade3)

                if not os.path.exists(f"output\TriBot_{self.exchange_name}_output_{dt.datetime.today().date().strftime('%d%m%Y')}.csv"):
                    with open(f"output\TriBot_{self.exchange_name}_output_{dt.datetime.today().date().strftime('%d%m%Y')}.csv", "a") as f:
                        f.write("combination_ID,date,arbitrage_type,pair_1,price_1,amount_1,pair_2,price_2,amount_2,pair_3,price_3,amount_3,initial_amount,OP_return,Profitable,exe_time,executed_return,tri_id\n")
                
                if not bbs == None:
                    try:
                        with open(f"output\TriBot_{self.exchange_name}_output_{dt.datetime.today().date().strftime('%d%m%Y')}.csv", "a") as f:
                            f.write(combination_ID+","+bbs+"\n")
                    except PermissionError:
                        if self.verbose in self.verbose_levels: print("Catched Permission Error")
                        f.close()
                        time.sleep(1)
                        with open(f"output\TriBot_{self.exchange_name}_output_{dt.datetime.today().date().strftime('%d%m%Y')}.csv", "a") as f:
                            f.write(combination_ID+","+bbs+"\n")

                # Check triangular arbitrage for buy-sell-sell 
                bss = self.perform_triangular_arbitrage(pair1=s3, pair2=s2, pair3=s1,
                                                        arbitrage_type="BUY_SELL_SELL",
                                                        investment_limit=self.investment_limit,
                                                        min_profit_percentage=MIN_PROFIT_percentage,
                                                        slippage_trade1=slippage_trade1,
                                                        slippage_trade2=slippage_trade2,
                                                        slippage_trade3=slippage_trade3)
                
                if not bss == None:
                    try:
                        with open(f"output\TriBot_{self.exchange_name}_output_{dt.datetime.today().date().strftime('%d%m%Y')}.csv", "a") as f:
                            f.write(combination_ID+","+bss+"\n")
                    except PermissionError:
                        if self.verbose in self.verbose_levels: print("Catched Permission Error")
                        f.close()
                        time.sleep(1)
                        with open(f"output\TriBot_{self.exchange_name}_output_{dt.datetime.today().date().strftime('%d%m%Y')}.csv", "a") as f:
                            f.write(combination_ID+","+bbs+"\n")
                            
                errCatch = 0      # Restart error counter after complete execution without exceptions
                
            except ccxt.NetworkError as err:
                if self.verbose in self.verbose_levels["error"]: print(f"\nNetwork error: {err}")
                if errCatch == 0:
                    if self.verbose in self.verbose_levels["error"]: print(f"Error catch {errCatch} Sleeping 30 minutes")
                    time.sleep(30*60)
                    errCatch +=1
                elif errCatch == 1:
                    if self.verbose in self.verbose_levels["error"]: print(f"Error catch {errCatch} Sleeping 60 minutes")
                    time.sleep(60*60)
                    errCatch += 1
                elif errCatch == 2:
                    if self.verbose in self.verbose_levels["error"]: print(f"Error catch {errCatch} Sleeping 90 minutes")
                    time.sleep(90*60)
                    errCatch += 1
                else:
                    if self.verbose in self.verbose_levels["error"]: print(f"Error catch {errCatch}  BREAK!")
                    break
                
            except ccxt.ExchangeError as err:
                if self.verbose in self.verbose_levels["error"]: print(f"\nNetwork error: {err}")
                if errCatch <= 3:
                    if self.verbose in self.verbose_levels["error"]: print(f"Error catch {errCatch} Sleeping 5 minutes")
                    time.sleep(5*60)
                    errCatch +=1
                else:
                    break
            
            self.run_summary["total checks"] +=1