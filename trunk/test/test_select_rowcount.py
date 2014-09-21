from CUBRIDPy.CUBRIDConnection import *
import logging

logging.root.setLevel(logging.INFO)
logging.info('Executing test: %s...' % __file__)

conn = CUBRIDConnection()
conn.connect()
cur = conn.cursor()

try:
    query = 'select * from code'
    cur.execute(query)
    assert cur.rowcount == 6

    query = 'select * from code limit 1'
    cur.execute(query)
    assert cur.rowcount == 1

    query = 'update code set f_name=\'xxx\' where s_name=\'xxx\''
    cur.execute(query)
    assert cur.rowcount == 1 # one statement executed

    query = 'update code set f_name=f_name'
    cur.execute(query)
    assert cur.rowcount == 1 # one statement executed
finally:
    cur.close()
    conn.close()

logging.info('Test completed.')
