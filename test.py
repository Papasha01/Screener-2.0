from random import random
from threading import Thread
import pandas as pd 

df1 = pd.DataFrame([[1,'Bob', 'Builder'],
                  [2,'Sally', 'Baker'],
                  [3,'Scott', 'Candle Stick Maker']], 
columns=['id','name', 'occupation'])

df2 = pd.DataFrame([[1,'Bob', 'Builder'],
                  [2,'Sally', 'Baker'],
                  [3,'Scott', 'Candle Stick Maker']], 
columns=['id','name', 'occupation'])

def q1():
    while True:
        filter = df1.id < 1
        df1.loc[filter, 'id'] = random()

def q2():
    while True:
        filter = df2.id < 1
        df2.loc[filter, 'id'] = random()

Thread(target=q1).start()
Thread(target=q2).start()