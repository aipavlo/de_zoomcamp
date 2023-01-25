from sqlalchemy import create_engine
import pandas as pd

engine = create_engine('postgresql://root:root@localhost:5432/ny_taxi')
engine.connect()

df = pd.read_csv('newag', nrows=100000)

print(
pd.io.sql.get_schema(df, name='green_taxi_data', con=engine)
)

# df.head(n=0).to_sql(con=engine, name='green_taxi_data', if_exists='replace')


df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)

df.to_sql(name='green_taxi_data', con=engine, if_exists='append')

print('Inserted another chunk')
