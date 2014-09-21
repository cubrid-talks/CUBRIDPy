from CUBRIDPy.CUBRIDConnection import *
import logging

logging.root.setLevel(logging.INFO)
logging.info('Executing test: %s...' % __file__)

conn = CUBRIDConnection()
conn.connect()
cur = conn.cursor()

try:
    count = cur.execute('create table t')
    assert count == 1
    logging.info('Table t was created.')
    count = cur.execute('drop table t')
    assert count == 1
    logging.info('Table t was dropped.')
finally:
    cur.close()
    conn.close()

logging.info('Test completed.')
