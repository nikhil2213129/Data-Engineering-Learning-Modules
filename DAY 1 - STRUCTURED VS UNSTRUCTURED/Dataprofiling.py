import pandas as pd, random, os, time
from datetime import date, timedelta

random.seed(42)
rows = [{'order_id': f'ORD-{i:03d}',
         'product': random.choice(['Laptop','Monitor','Keyboard','Mouse','Headset']),
         'qty': random.randint(1, 10),
         'price': round(random.uniform(10, 500), 2),
         'order_date': str(date(2024,1,1) + timedelta(days=random.randint(0, 364)))}
        for i in range(100)]
df = pd.DataFrame(rows)
df.to_csv('out/orders.csv')
df.to_json('out/orders.json', orient = "records", lines = True)
df.to_parquet('out/orders.parquet', compression = "gzip")


def profile_file(path):
    ext = path.rsplit('.', 1)[-1]
    readers = {'csv': pd.read_csv, 'json': pd.read_json, 'parquet': pd.read_parquet}
    t0 = time.time()
    df = readers[ext](path)
    elapsed = round(time.time()-t0, 3)
    nulls = df.isnull().sum()
    return {
    'rows': len(df),
    'cols': df.shape[1],
    'read_time': elapsed,
    'null_pct': (nulls / len(df) * 100).round(2).to_dict(),
    'dtypes': df.dtypes.astype(str).to_dict(),
    }
    