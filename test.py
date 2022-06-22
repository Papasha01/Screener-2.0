import pandas as pd 

df = pd.DataFrame([ [-10, -9, 8], [6, 2, -4], [-8, 5, 1]], columns=['a', 'b', 'c']) 

df.a.where(~((df.a < 0) | (df.b == 2)), other= -999, inplace=True) 

for index, row in df.iterrows():
    print(row)


