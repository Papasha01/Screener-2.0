from datetime import date, datetime, timedelta
from binance.spot import Spot as Client
import unicorn_binance_websocket_api
from progress.spinner import Spinner 
from progress.bar import Bar
from threading import Thread
from loguru import logger
from pygame import mixer
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

def get_first_data(list_coin):
    '''
    Первое получение данных
    '''

    bar_import_coins = Bar('Importing Coins', max = len(list_coin))
    global data_depth

    for row in list_coin:
        global limit
        spot_client = Client(base_url="https://api1.binance.com")
        depth_dict = spot_client.depth(row, limit= 150)
        del depth_dict["lastUpdateId"]
        for ba in depth_dict.values():
            for i in ba:
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
    
    def check(ba):
        global data_depth
        filter_depth = ((data_depth.coin == jsMessage['data']['s']) & (data_depth.price ==  ba[0]))
        if len(data_depth.loc[filter_depth]) == 0:
            frame_dict = {'coin': jsMessage['data']['s'], 'price': ba[0], 'quantity': ba[1], 'dt': datetime.now(), 'dt_resend': datetime.now(), 'in_range': 0}
            frame = pd.DataFrame([frame_dict])
            data_depth = pd.concat([data_depth, frame], ignore_index=True)
        data_depth.quantity.where(~(filter_depth), other=ba[1], inplace=True) 

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
                    filter_agg = ((data_price.coin == jsMessage['data']['s']) & (data_price.price ==  jsMessage['data']['p']))
                    if len(data_price.loc[filter_agg]) == 0:
                        data_price = pd.DataFrame([{'coin': jsMessage['data']['s'], 'price': jsMessage['data']['p']}])

                        print(data_price.loc[(data_price.coin == 'ADAUSDT')].values[0][1])
        else: 
            time.sleep(0.5)


def check_old_data():
    '''
    Проверка данных, отправка уведомлений
    '''
    while True:
        for index, row in data_depth.iterrows():
            #try:
                sign_ptd = float(data_price.loc[(data_price.coin == row['coin'])].values[0][1]) / float(row['price'])
                percentage_to_density = abs((sign_ptd) - 1)
                if  percentage_to_density <= cf_distance:
                    if datetime.now() - time_resend > row['dt_resend'] and row['in_range'] != 1:

                        filter_depth = ((data_depth.coin == row['coin']) & (data_depth.price == row['price']))
                        data_depth.dt_resend.where(~(filter_depth), other=datetime.now(), inplace=True) 
                        data_depth.in_range.where(~(filter_depth), other=1, inplace=True) 
                        
                        if sign_ptd > 1: percentage_to_density = -percentage_to_density
                        #print(f'\n\nCoin: { row['coin'] }\nPrice: {record[2]}\nQuantity: {record[3]}\nAmount: {round(float(record[2]) * float(record[3]), 2)}$\nPercentage to density: {round(percentage_to_density*100, 2)}%\nDate of discovery: {record[4]}')
                        #send_telegram(record, percentage_to_density)
                        #logger.debug(f'{str(record)} {str(percentage_to_density)})')
                        # sound_notification.play()
                #else: 
                    #update_out_from_range(record[1], record[2])
            #except Exception as e:
                #logger.error(e)

                # time.sleep(10)
                check_old_data()


# Global variable
delta, time_resend, limit, cf_update, cf_distance = import_cfg()
data_depth = pd.DataFrame(columns=['coin', 'price', 'quantity', 'dt', 'dt_resend', 'in_range'])
data_price = pd.DataFrame(columns=['coin', 'price'])
list_coin = get_list_coins()
ubwa = unicorn_binance_websocket_api.BinanceWebSocketApiManager(exchange="binance.com")
ubwa.create_stream(['depth', 'aggTrade'], list_coin)
print('Successful connection')

def main():
    # Progress
    spinner_running = Spinner('Checking ')

    data_depth = get_first_data(list_coin=list_coin)
    Thread(target=get_depth_from_websocket).start()
    Thread(target=check_old_data).start()
    #get_depth_from_websocket(list_coin= list_coin)

if __name__ == '__main__':
    main()