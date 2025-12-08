import pandas as pd
import sqlite3
import json
import logging
import logging.handlers
from logging.handlers import QueueHandler
from pathlib import Path
from multiprocessing import Pool, Manager
from auction_filter import auction_filter
from logger_config import setup_logger
import gc

"""
First problem starts with the implementation of class - auction_filter. 
To utilize multiprocessing we have to create workers, each one with their own auction_filter object
We cannot create `auction_filter` in the parent process because it holds an active logger (file handle), which cannot be serialized (pickled).
To safely handle logging from multiple processes, a `QueueHandler` was implemented. 
Workers push logs to a queue, and a listener in the main process writes them to the actual log file.
"""

LOGGER_CONF = ('dataframe_setup', 'dataframe_setup.log')
json_dir = Path(r"archive/parsed_archive")
output_db = "dataframe.db"
batch_size = 100


worker_instance = None

def init_worker(queue, logger_conf):
    logger_name, _ = logger_conf
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.INFO)

    if not logger.hasHandlers():
        handler = QueueHandler(queue)
        logger.addHandler(handler)


    global worker_instance
    worker_instance = auction_filter(logger_conf)

def worker_task(file_path):
    processed_auctions = []

    with open(file_path, 'r') as f:
        auctions = json.load(f)

    for auction in auctions:
        processed_auctions.append(worker_instance.flat_single_auction(auction))

    return processed_auctions

def process_batch(file_batch, pool, is_first_batch):
    results = pool.map(worker_task, file_batch)

    all_auctions_unfiltered = [auction for sublist in results for auction in sublist]
    all_auctions = [i for i in all_auctions_unfiltered if i is not None]

    if not all_auctions:
        return False

    df = pd.DataFrame(all_auctions).fillna(0)
    df = df.infer_objects(copy=False) #it changes type of data in a column to matching one

    con = sqlite3.connect(output_db)
    table_name = 'auctions_processed'

    if is_first_batch:
        df.to_sql(table_name, con, if_exists='replace', index=False)
    else:
        cursor = con.cursor()
        cursor.execute(f"PRAGMA table_info({table_name})")
        db_cols = {row[1] for row in cursor.fetchall()}
        new_cols = set(df.columns) - db_cols

        for col in new_cols:
            pd_type = df[col].dtype
            sql_type = "TEXT"
            if "int" in str(pd_type):
                sql_type = "INTEGER"
            elif "float" in str(pd_type):
                sql_type = "REAL"

            try:
                cursor.execute(f'ALTER TABLE {table_name} ADD COLUMN "{col}" {sql_type}')
                print(f"DEBUG: Dodano nową kolumnę do bazy: {col}")
            except sqlite3.OperationalError as e:
                print(f"Warning: Could not add column {col}: {e}")
        df.to_sql(table_name, con, if_exists='append', index=False)
    con.close()

    del df
    del all_auctions
    del results
    gc.collect()
    return True

def df_setup():
    main_logger = setup_logger(*LOGGER_CONF)

    m = Manager()
    queue = m.Queue()

    listener = logging.handlers.QueueListener(queue, *main_logger.handlers)
    listener.start()

    try:
        file_paths = list(json_dir.glob('*.json'))
        total_files = len(file_paths)
        with Pool(initializer=init_worker, initargs=(queue, LOGGER_CONF)) as pool: #It needs initargs separatly, otheriwse it will execute in place here in line 62
            for i in range(0, total_files, batch_size):
                batch = file_paths[i: i + batch_size]
                is_first = (i==0)
                process_batch(batch, pool, is_first)
    finally:
        listener.stop()

if __name__ == '__main__':
    df_setup()