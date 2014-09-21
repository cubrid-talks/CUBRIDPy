from CUBRIDPy.CUBRIDConnection import *
import logging

logging.root.setLevel(logging.INFO)
logging.info('Executing test: %s...' % __file__)

conn = CUBRIDConnection()
conn.connect()
cur = conn.cursor()

try:
    query = 'create table t'
    cur.execute(query)
    assert cur.description is None
finally:
    cur.execute('drop table if exists t')
    cur.close()
    conn.close()

logging.info('Test completed.')
