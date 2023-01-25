from sqlalchemy import create_engine
import pandas as pd

engine = create_engine('postgresql://root:root@localhost:5432/ny_taxi')
engine.connect()

df = pd.read_csv('green_tripdata_2019-01.csv', nrows=1)

print(
pd.io.sql.get_schema(df, name='green_taxi_data', con=engine)
)

df_iter = pd.read_csv('green_tripdata_2019-01.csv', iterator=True, chunksize=50000)
next(df_iter)

df.head(n=0).to_sql(con=engine, name='green_taxi_data', if_exists='replace', index=False)

from time import time
counter = 0
while True:
    counter += 1
    t_start = time()
    df = next(df_iter)
    print(counter)

    df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
    df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)
    df.to_sql(name='green_taxi_data', con=engine, if_exists='append', index=False)
    t_end = time()
    print('Inserted another chunk..., took %.3f second' % (t_end-t_start))

print('Completed')