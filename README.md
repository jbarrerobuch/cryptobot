# TriangularArbitrageCryptos
[modify description]
Triangular arbitrage is a technique that tries to exploit the price discrepancy across three different assets at the same time.  
For example, we can exchange BTC for USDT, BTC for ETH and ETH back to USDT.   
If the net worth in doing these three trades simultaneously is profitable then the 3 trades are executed simultaneously.  

Here we implement the triangular arbitrage in 4 steps.  
Step 1: Get all the valid crypto combinations. 
Step 2: Perform triangular arbitrage  
Step 3: Place the trade orders  
Step 4: Bundle it together  

Refer to this blog to understand more on triangular arbitrage implemented in this repo:  
https://lakshmi1212.medium.com/automated-triangular-arbitrage-of-cryptos-in-4-steps-a678f7b01ce7

# Improvements
OP Prices from the top bid and ask [DONE]  
OP Amoint from the top bid and ask [DONE]  
Minimun volumen to trade per pair [DONE]  
Calculate fee per pair and apply it [DONE]  
Handle Strict limits via API [suppose to be done with exchange.enableLimit = True][DONE]  
Implement REAL order execution [DONE]  
Real order execution log [Testing]  [adding OP ID i.e. BBS_USDT-BTC-ETH_[timestampMiliseconds] to every trade excecuted for singl triangule operation]  
Handle partial execution or slipage [added slipage % per trade, profit dropped dramatically]  
Take functions to a .py  
Get Pair combinations independently of the position of the asset (base or quote)  
Get OP prices by ponderating bids and ask from the order book until the max of investment  


# Bugfixing
Execution time: is calculate in each OP check but result is always negative. [DONE]  
OP2 in BSS is amount/price instead of amount*price [20/05/22] [DONE]  
TimeOut Error appers after a while running. Added an Error Catch [22/05/22][Exception added][DONE]  
There are fee from executed order equal to None, setting it to 0 to avoid error when concatenating data for CSV [Done]  
local variable 'status' referenced before assignment [Done]  
After order cancelled; Network error: binance {"code":-2011,"msg":"Unknown order sent."} [posible solution remove order fetch after cancelling or catch exception]