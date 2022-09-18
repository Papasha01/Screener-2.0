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

# Отключение предупреждений
pd.options.mode.chained_assignment = None

# Логирование
logger.add("simple.log")
logger.info("Start script")

# Импорт файла конфигурации
def import_cfg():
    try:
        config = configparser.ConfigParser()                          # создаём объекта парсера
        config.read("cfg.ini")                                        # читаем конфиг
        delta = timedelta(minutes = float(config["Settings"]["delta"]))
        time_resend = timedelta(minutes = float(config["Settings"]["time_resend"]))
        limit = float(config["Settings"]["limit"])
        cf_distance = float(config["Settings"]["cf_distance"])
        bot_token = str(config["Settings"]["bot_token"])
        return delta, time_resend, limit, cf_distance, bot_token
    except Exception as e:
        logger.error(f'import config {e}')
        sys.exit(1)      

# Парсинг файла с монетами в массив
def get_list_coins():
    try:
        list_coin = []
        coins = open('coins.txt')
        for row in coins: list_coin.append(row.rstrip())
        coins.close()
        return list_coin
    except FileNotFoundError as e:
        logger.error(e)
        sys.exit()

# Первое получение данных
def get_first_data():
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
                    frame_dict = {'coin': row, 'price': i[0], 'quantity': i[1], 'dt': datetime.now(), 'dt_resend': datetime(1970, 1, 1), 'in_range':0}
                    frame = pd.DataFrame([frame_dict])
                    data_depth = pd.concat([data_depth, frame], ignore_index=True)
        bar_import_coins.next()
        
    bar_import_coins.finish()
    logger.debug("Import Complite")
    return data_depth

# Функция фильтрации плотностей
def filte_cod(row):
    return ((data_depth.coin == row['coin']) & (data_depth.price ==  row['price']))

# Функция фильтрации плотностей
def filter_check(jsMes, ba):
    return ((data_depth.coin == jsMes['data']['s']) & (data_depth.price ==  ba[0]))

# Получение данных с websocket
def get_depth_from_websocket():
    global data_depth, data_price, ubwa
    spinner_running = Spinner('Checking ')
    
    def check(ba):
        global data_depth, limit
        
        if float(ba[0]) * float(ba[1]) > limit:
            if len(data_depth.loc[filter_check(jsMes, ba)]) != 0:
                data_depth.loc[filter_check(jsMes, ba), 'quantity'] = ba[1]
            else:
                frame_dict = {'coin': jsMes['data']['s'], 'price': ba[0], 'quantity': ba[1], 'dt': datetime.now(), 'dt_resend': datetime(1970, 1, 1), 'in_range': 0}
                frame = pd.DataFrame([frame_dict])
                data_depth = pd.concat([data_depth, frame], ignore_index=True)
        elif len(data_depth.loc[filter_check(jsMes, ba)]) != 0:
            data_depth = data_depth.loc[-filter_check(jsMes, ba)]

    while True:
        oldest_data_from_stream_buffer = ubwa.pop_stream_data_from_stream_buffer()
        if oldest_data_from_stream_buffer:
            jsMes = json.loads(oldest_data_from_stream_buffer)
            if 'stream' in jsMes.keys():
                if jsMes['data']['e'] == 'depthUpdate':
                    for bid in jsMes['data']['b']:
                        check(bid)
                    for ask in jsMes['data']['a']:
                        check(ask)
                elif jsMes['data']['e'] == 'aggTrade':
                    data_price.loc[data_price.coin == jsMes['data']['s'], 'price'] = jsMes['data']['p']
        else: 
            time.sleep(0.1)
            spinner_running.next()

# Проверка данных, отправка уведомлений
def check_old_data():
    global data_depth, data_price
    while True:
        try:
            for index, row in data_depth.iterrows():
                percentage_to_density = -(float(data_price.loc[(data_price.coin == row['coin'])].values[0][1]) / float(row['price']) - 1)
                if abs(percentage_to_density) <= cf_distance and row['dt'] < datetime.now() - delta:
                    data_depth.loc[filte_cod(row), 'in_range'] = 1  
                    if row['dt_resend'] < datetime.now() - time_resend and row['in_range'] != 1:
                        data_depth.loc[filte_cod(row), 'dt_resend'] = datetime.now() 
                        send_telegram(row, percentage_to_density)
                        logger.info(f'{str(row)} {str(percentage_to_density)})')
                else: 
                    data_depth.loc[filte_cod(row), 'in_range'] = 0
            time.sleep(0.2)
        except Exception as e:
            logger.error(e)
            time.sleep(10)
            check_old_data()

# Global variable
data_depth = pd.DataFrame(columns=['coin', 'price', 'quantity', 'dt', 'dt_resend', 'in_range'])
data_price = pd.DataFrame(columns=['coin', 'price'])
ubwa = unicorn_binance_websocket_api.BinanceWebSocketApiManager(exchange="binance.com")

# Подключение к вебсокету
def connect_ws():
    global ubwa, list_coin
    ubwa.create_stream(['depth', 'aggTrade'], list_coin)
    print('Successful connection')

# Импорт файла конфигурации
delta, time_resend, limit, cf_distance, bot_token = import_cfg()

# Запуск цикла Telebot
bot=telebot.TeleBot(bot_token)

def polling():
    time.sleep(5)
    try: 
        bot.polling(none_stop=True) 
    except Exception as e: 
        logger.error(e)
        time.sleep(5)
        polling()

# Обработка команды /start
@bot.message_handler(commands=['start'])
def start_handler(message):
    bot.send_message(message.chat.id, "I'm working!")
    logger.debug(f"User №{message.chat.id} send I'm working")
    if not select_user_id(str(message.chat.id)):
        insert_user_id(str(message.chat.id))
        logger.debug(f'User №{message.chat.id} added to the database)')

# Обработка команды /check
@bot.message_handler(commands=['check'])
def start_handler(message):
    global data_depth, data_price
    if len(data_depth.loc[data_depth.in_range ==  1]) == 0:
        logger.info(f"User №{message.chat.id} send No records")
        bot.send_message(message.chat.id, 'No records')
    else:
        all_verified_record = ''
        for index, row in data_depth.iterrows():
            if row['in_range'] == 1:
                percentage_to_density = -((float(data_price.loc[(data_price.coin == row['coin'])].values[0][1]) / float(row['price'])) - 1)
                all_verified_record += f"Coin: {row['coin']}\nPrice: {row['price']}\nQuantity: {row['quantity']}\nAmount: {round(float(row['price']) * float(row['quantity']), 2)}$\nPercentage to density: {round(percentage_to_density*100, 2)}%\nDate of discovery: {row['dt']}\n\n"
        logger.info(f"User №{message.chat.id} send all_verified_record")
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

def main():
    global list_coin
    list_coin = get_list_coins()
    connect_ws()
    Thread(target=get_depth_from_websocket).start()
    get_first_data()
    Thread(target=check_old_data).start()
    Thread(target=polling).start()

if __name__ == '__main__':
    main()