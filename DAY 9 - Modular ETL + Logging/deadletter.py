import sqlite3
conn = sqlite3.connect(':memory:')
data = """ CREATE TABLE dead_letter_sales (
    id              BIGSERIAL PRIMARY KEY,
    run_id          VARCHAR(40),
    run_date        DATE,
    raw_data        JSONB,          -- original row as JSON
    error_reason    VARCHAR(200),   -- which rule(s) failed
    created_at      TIMESTAMP DEFAULT NOW()
); """
conn.execute(data)
def load_dead_letter(errors_df, run_id, run_date, engine):
    if errors_df.empty: return
    dl = errors_df.copy()
    dl['run_id']   = run_id
    dl['run_date'] = run_date
    dl['raw_data'] = dl.drop(columns=['run_id','run_date','error_reason'],
                             errors='ignore').to_dict('records')
    dl['raw_data'] = dl['raw_data'].apply(str)
    dl[['run_id','run_date','raw_data','error_reason']].to_sql(
        'dead_letter_sales', engine, if_exists='append', index=False)
    print(f'Dead letter: {len(dl)} rows with run_id={run_id}')