from CUBRIDPy.CUBRIDConnection import *
import logging

logging.root.setLevel(logging.INFO)
logging.info('Executing test: %s...' % __file__)

conn = CUBRIDConnection()
conn.connect()
cur = conn.cursor()

try:
    count = cur.batch_execute(['create table t'])
    assert count == 1
    logging.info('Table t was created.')
    count = cur.batch_execute(['drop table t'])
    assert count == 1
    logging.info('Table t was dropped.')
    count = cur.batch_execute([
        'create table t',
        'drop table t'
    ])
    assert count == 2
    logging.info('Table t was created and dropped.')
finally:
    cur.close()
    conn.close()

logging.info('Test completed.')
