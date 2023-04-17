from CBotfunctions import Tribot
from config import myconfig
import time

tribot = Tribot(exchange_name="bitfinex",
                api_key=myconfig.BITFINEX_KEY,
                api_secret=myconfig.BITFINEX_SECRET,
                sandbox_net=False,
                test_mode=True)
tribot.start_trading(initial_investment=15, verbose="all")