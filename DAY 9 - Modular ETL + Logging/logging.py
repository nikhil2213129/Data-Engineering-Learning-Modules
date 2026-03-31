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
