import pandas as pd
from dataclasses import dataclass
import logging
import json
import time

logger = logging.getLogger('datamart.etl')
logger.setLevel(logging.INFO)

stream_handler = logging.StreamHandler()
stream_handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s %(name)s %(message)s'))

file_handler = logging.FileHandler('etl.log')
file_handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s %(name)s %(message)s'))

logger.addHandler(stream_handler)
logger.addHandler(file_handler)

@dataclass
class ValidationResult:
    valid:  pd.DataFrame
    errors: pd.DataFrame
    error_rate: float
def validate_sales(df: pd.DataFrame) -> ValidationResult:
    rules = {
        'missing_sale_id':   df['sale_id'].isna(),
        'missing_product_id':df['product_id'].isna(),
        'negative_quantity': df['quantity'] < 0,
        'invalid_discount':  ~df['discount_pct'].between(0, 1),
        'future_date':       df['sale_date'] > pd.Timestamp.today(),
    }
    error_mask = pd.concat(rules.values(), axis=1).any(axis=1)
    error_df   = df[error_mask].copy()
    error_df['error_reasons'] = pd.concat(rules.values(), axis=1).apply(
        lambda row: ','.join([k for k,v in rules.items() if row[rules.keys().index(k)]]),
        axis=1
    )


def run_pipeline(run_date: str):
    run_id = f"{run_date}_{int(time.time())}"
    logger.info(json.dumps({'event': 'pipeline_start', 'run_id': run_id, 'date': run_date}))
    t0 = time.time()

    try:
        df = extract_sales(run_date)
        logger.info(json.dumps({'event': 'extract_done', 'run_id': run_id, 'rows': len(df)}))

        df_clean = transform_sales(df)
        result = validate_sales(df_clean)

        logger.info(json.dumps({
            'event': 'validate_done',
            'run_id': run_id,
            'valid': len(result.valid),
            'errors': len(result.errors),
            'error_rate': round(result.error_rate, 2)
        }))

        load_to_warehouse(result.valid, 'sales_fact')

    except Exception as e:
        logger.error(json.dumps({'event': 'pipeline_error', 'run_id': run_id, 'error': str(e)}))
    finally:
        elapsed = round(time.time() - t0, 2)
        logger.info(json.dumps({'event': 'pipeline_end', 'run_id': run_id, 'elapsed': elapsed}))
