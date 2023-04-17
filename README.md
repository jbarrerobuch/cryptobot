# TriangularArbitrageCryptos

## Description  
Triangular arbitrage is an strategy that exploits deviations from 3 correlated markets.
For example, BTC/USDT, ETH/BTC, ETH/USDT
If the net worth is profitable after trading fees trades will be executed one after the other.  
  
This price mismatch is calculated from the pondered price to buy certain amount from the order book, not just from the high and low bids and asks. Tested execution in binance and bitfinex.  


Refer to this blog to understand more on triangular arbitrage implemented in this repo:
https://lakshmi1212.medium.com/automated-triangular-arbitrage-of-cryptos-in-4-steps-a678f7b01ce7  
Fork from: https://github.com/Lakshmi-1212/TriangularArbitrageCryptos

Here we implement the triangular arbitrage in 4 steps.  
Step 1: Get all the valid crypto combinations.  
Step 2: Perform triangular arbitrage.  
Step 3: Place the trade orders.  
Step 4: Bundle it together.  


## Improvements
- Operation prices from the top bid and ask [DONE-> Deprecated]  
- Operation amount from the top bid and ask [DONE-> Deprecated]  
- Minimun volumen to trade per pair [DONE-> Deprecated]  
- Calculate fee per pair and apply it [DONE]  
- Implement REAL order execution [DONE]  
- Real order execution log [DONE] to csv
- Checks logged to csv [DONE]
- Handle partial execution [DONE]
- Add slipage as a percentage to every trade [DONE]  
- Build the bot as a class [DONE]  
- Get assets prices from the order book until the max of investment. [DONE]
- Added bitfinex compatibility [DONE]  


## Bugfixing
- Execution time: is calculate in each OP check but result is always negative. [DONE]  
- OP2 in BSS is amount/price instead of amount*price [20/05/22] [DONE]  
- TimeOut Error appers after a while running. Added an Error Catch [22/05/22][Exception added][DONE]  
- There are fees from executed order equal to None, setting it to 0 to avoid error when concatenating data for CSV [Done]  
- Local variable 'status' referenced before assignment [Done]  
- After some time running it gets stuck, no error nor exception raised. It happened after getting the pondered price of the second pair. Happened in bitfinex, to check if this happens in binance too.
=======
Triangular arbitrage is an strategy that tries to exploit uncorrelations from 3 correlated markets.  
For example, BTC/USDT, ETH/BTC, ETH/USDT   
If the net worth is profitable after trading fees trades will be executed one after the other.  

This price mismatch is calculated from the pondered price to buy certain amount from the order book, not just from the high and low bids and asks.
Tested execution in binance and bitfinex.

Refer to this blog to understand more on triangular arbitrage implemented in this repo:  
https://lakshmi1212.medium.com/automated-triangular-arbitrage-of-cryptos-in-4-steps-a678f7b01ce7
forked from: https://github.com/Lakshmi-1212/TriangularArbitrageCryptos
