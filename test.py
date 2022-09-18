# from binance.client import Client

# api_key = "xxx"
# api_secret = "xxx"

# client = Client(api_key, api_secret)
# exchange_info = client.get_exchange_info()
# with open('coins.txt', 'a+') as f:
#     for s in exchange_info['symbols']:
#         f.write(s['symbol'] + '\n') 

with open('coins.txt', 'r') as f:
    with open('YES.txt', 'a+') as y:
        for s in f:
            if 'USDT' in s:
                y.write(s)