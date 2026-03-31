import pandas as pd
from sqlalchemy import create_engine
#import psycopg2


def generate_dim_date(start='2020-01-01', end='2029-12-31'):
    dates = pd.date_range(start, end, freq='D')
    df = pd.DataFrame({'full_date': dates})
    df['date_id']       = df['full_date'].dt.strftime('%Y%m%d').astype(int)
    df['day_of_week']   = df['full_date'].dt.dayofweek + 1  # 1=Mon
    df['day_name']      = df['full_date'].dt.day_name()
    df['month_number']  = df['full_date'].dt.month
    df['month_name']    = df['full_date'].dt.month_name()
    df['quarter']       = df['full_date'].dt.quarter
    df['year']          = df['full_date'].dt.year
    df['is_weekend']    = df['full_date'].dt.dayofweek >= 5
    df['fiscal_year']   = df.apply(lambda r: r['year']+(1 if r['month_number']>=8 else 0), axis=1)
    return df
df = generate_dim_date()
engine = create_engine('sqlite+pysqlite:///:memory:')
df.to_sql('dim_date', engine, if_exists='replace', index=False)
print(f'Loaded {len(df)} date rows')
df.to_csv("output.csv", index=False, encoding="utf-8")