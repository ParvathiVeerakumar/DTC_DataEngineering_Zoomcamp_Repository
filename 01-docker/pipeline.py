import sys
import pandas as pd

print(f'system arguments: {sys.argv}')

month =sys.argv[1]
df = pd.DataFrame({'Day':[1,2], 'Num_Passengers':[23,34]})
df['Month'] = month
print(df)
df.to_parquet(f"output_{month}.parquet")
print(f'hello from pipeline. month={month}')