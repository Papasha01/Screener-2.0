from db_req import select_user_id, select_all_user_id, insert_user_id
from datetime import date, datetime, timedelta
from binance.spot import Spot as Client
import unicorn_binance_websocket_api
from progress.spinner import Spinner 
from progress.bar import Bar
from threading import Thread
from loguru import logger
import configparser
import json
import time
import telebot
import pandas as pd
import sys

# Логирование
logger.add("simple.log")
logger.debug("Start script")

def import_cfg():
    '''
    Импорт файла конфигурации
    '''
    try:
        config = configparser.ConfigParser()                          # создаём объекта парсера
        config.read("cfg.ini")                                        # читаем конфиг
        delta = timedelta(minutes = float(config["Settings"]["delta"]))
        time_resend = timedelta(minutes = float(config["Settings"]["time_resend"]))
        limit = float(config["Settings"]["limit"])
        cf_update = float(config["Settings"]["cf_update"])
        cf_distance = float(config["Settings"]["cf_distance"])
        return delta, time_resend, limit, cf_update, cf_distance
    except Exception as e:
        logger.error(f'import config {e}')
        sys.exit()      

def get_list_coins():
    '''
    Парсинг файла с монетами в массив
    '''
    try:
        list_coin = []
        coins = open('coins.txt')
        for row in coins: list_coin.append(row.rstrip())
        coins.close()
        return list_coin
    except FileNotFoundError as e:
        logger.error(e)
        sys.exit()

def get_first_data():
    '''
    Первое получение данных
    '''

    global data_depth, data_price, limit, list_coin
    bar_import_coins = Bar('Importing Coins', max = len(list_coin))

    for row in list_coin:
        global limit
        spot_client = Client(base_url="https://api1.binance.com")
        depth_dict = spot_client.depth(row, limit= 150)

        price_dict = spot_client.avg_price(row)
        frame_dict = {'coin': row, 'price': price_dict['price']}
        frame = pd.DataFrame([frame_dict])
        data_price = pd.concat([data_price, frame], ignore_index=True)
        
        del depth_dict["lastUpdateId"]
        for ba in depth_dict.values():
            for i in ba:
                if float(i[0]) * float(i[1]) > limit:
                    frame_dict = {'coin': row, 'price': i[0], 'quantity': i[1], 'dt': datetime.now(), 'dt_resend': datetime.now(), 'in_range':0}
                    frame = pd.DataFrame([frame_dict])
                    data_depth = pd.concat([data_depth, frame], ignore_index=True)
        bar_import_coins.next()
        
    bar_import_coins.finish()
    print('Import Complite')
    return data_depth

def get_depth_from_websocket():
    '''
    Получение данных с websocket
    '''
    global data_depth, data_price, ubwa
    spinner_running = Spinner('Checking ')
    
    def check(ba):
        global data_depth, limit, cf_update
        filter_depth = ((data_depth.coin == jsMessage['data']['s']) & (data_depth.price ==  ba[0]))
        if float(ba[0]) * float(ba[1]) > limit:
            if len(data_depth.loc[filter_depth]) != 0:
                if float(ba[1]) > float(data_depth.loc[filter_depth]['quantity']) * cf_update:
                    data_depth.quantity.where(~(filter_depth), other=ba[1], inplace=True)
                else:
                    data_depth.quantity.where(~(filter_depth), other=ba[1], inplace=True) 
                    data_depth.dt.where(~(filter_depth), other=datetime.now(), inplace=True) 
            else:
                frame_dict = {'coin': jsMessage['data']['s'], 'price': ba[0], 'quantity': ba[1], 'dt': datetime.now(), 'dt_resend': datetime.now(), 'in_range': 0}
                frame = pd.DataFrame([frame_dict])
                data_depth = pd.concat([data_depth, frame], ignore_index=True)
        elif len(data_depth.loc[filter_depth]) != 0:
            filter_depth = filter_depth
            data_depth = data_depth.loc[filter_depth]

    while True:
        oldest_data_from_stream_buffer = ubwa.pop_stream_data_from_stream_buffer()
        if oldest_data_from_stream_buffer:
            jsMessage = json.loads(oldest_data_from_stream_buffer)
            if 'stream' in jsMessage.keys():
                if jsMessage['data']['e'] == 'depthUpdate':
                    for bid in jsMessage['data']['b']:
                        check(bid)
                    for ask in jsMessage['data']['a']:
                        check(ask)
                elif jsMessage['data']['e'] == 'aggTrade':
                    filter_agg = data_price.coin == jsMessage['data']['s']
                    data_price.price.where(~(filter_agg), other=jsMessage['data']['p'], inplace=True) 
        else: 
            time.sleep(0.1)
            spinner_running.next()


def check_old_data():
    '''
    Проверка данных, отправка уведомлений
    '''
    while True:
        try:
            for index, row in data_depth.iterrows():
                filter_depth = ((data_depth.coin == row['coin']) & (data_depth.price ==  row['price']))
                percentage_to_density = -(float(data_price.loc[(data_price.coin == row['coin'])].values[0][1]) / float(row['price']) - 1)
                if abs(percentage_to_density) <= cf_distance and (float(row['quantity']) * float(row['price'])) > limit and row['dt'] < datetime.now() - delta:
                    if datetime.now() - time_resend > row['dt_resend'] and row['in_range'] != 1:
                        data_depth.dt_resend.where(~(filter_depth), other=datetime.now(), inplace=True) 
                        data_depth.in_range.where(~(filter_depth), other=1, inplace=True) 
                        # print(f"\n\nCoin: {row['coin']}\nPrice: {row['price']}\nQuantity: {row['quantity']}\nAmount: {round(float(row['quantity']) * float(row['price']), 2)}$\nPercentage to density: {round(percentage_to_density*100, 2)}%\nDate of discovery: {row['dt']}")
                        send_telegram(row, percentage_to_density)
                        logger.info(f'{str(row)} {str(percentage_to_density)})')
                else: 
                    data_depth.in_range.where(~(filter_depth), other=0, inplace=True) 
        except Exception as e:
            logger.error(e)
            time.sleep(10)
            check_old_data()

token = '5276441681:AAHi9DX8ZYWVlm49AEBU1be0gVEXWmeKoZ8'
bot=telebot.TeleBot(token)

# Запуск цикла Telebot
def polling():
    time.sleep(5)
    try: 
        bot.polling(none_stop=True) 
    except Exception as e: 
        logger.error(e)
        time.sleep(5)
        polling()

# Взаимодействие с ботом
@bot.message_handler(commands=['start'])
def start_handler(message):
    bot.send_message(message.chat.id, "I'm working!")
    logger.debug(f"User №{message.chat.id} send I'm working)")
    if not select_user_id(str(message.chat.id)):
        insert_user_id(str(message.chat.id))
        logger.debug(f'User №{message.chat.id} added to the database)')

@bot.message_handler(commands=['check'])
def start_handler(message):
    global data_depth, data_price
    filter_depth = ((data_depth.in_range ==  1))
    if len(data_depth.loc[filter_depth]) == 0:
        logger.debug(f"User №{message.chat.id} send No records)")
        bot.send_message(message.chat.id, 'No records')
    else:
        all_verified_record = ''
        for index, row in data_depth.iterrows():
            if row['in_range'] == 1:
                percentage_to_density = -((float(data_price.loc[(data_price.coin == row['coin'])].values[0][1]) / float(row['price'])) - 1)
                all_verified_record += f"Coin: {row['coin']}\nPrice: {row['price']}\nQuantity: {row['quantity']}\nAmount: {round(float(row['price']) * float(row['quantity']), 2)}$\nPercentage to density: {round(percentage_to_density*100, 2)}%\nDate of discovery: {row['dt']}\n\n"
        logger.debug(f"User №{message.chat.id} send all_verified_record")
        bot.send_message(message.chat.id, all_verified_record)
        

# Отправка уведомления в телеграм
def send_telegram(row, percentage_to_density):
    if select_all_user_id():
        for user_id in select_all_user_id():
            try:
                bot.send_message(user_id[0], f"Coin: {row['coin']}\nPrice: {row['price']}\nQuantity: {row['quantity']}\nAmount: {round(float(row['price']) * float(row['quantity']), 2)}$\nPercentage to density: {round(percentage_to_density*100, 2)}%\nDate of discovery: {row['dt']}\n\n")
            except telebot.apihelper.ApiException as e:
                if e.description == "Forbidden: bot was blocked by the user":
                    print(f"Attention please! The user {user_id[0]} has blocked the bot")

# Global variable
delta, time_resend, limit, cf_update, cf_distance = import_cfg()
data_depth = pd.DataFrame(columns=['coin', 'price', 'quantity', 'dt', 'dt_resend', 'in_range'])
data_price = pd.DataFrame(columns=['coin', 'price'])
list_coin = get_list_coins()

ubwa = unicorn_binance_websocket_api.BinanceWebSocketApiManager(exchange="binance.com")
ubwa.create_stream(['depth', 'aggTrade'], list_coin)
print('Successful connection')

def main():
    get_first_data()
    Thread(target=get_depth_from_websocket).start()
    Thread(target=check_old_data).start()
    Thread(target=polling).start()


if __name__ == '__main__':
    main()