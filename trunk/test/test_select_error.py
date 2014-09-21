from datetime import time, date
from CUBRIDPy.CUBRIDConnection import *
import logging

logging.root.setLevel(logging.INFO)
logging.info('Executing test: %s...' % __file__)

conn = CUBRIDConnection()
conn.connect()
cur = conn.cursor()

try:
    query = 'select * from xyz'
    cur.execute(query)
except Exception as ex:
    assert ex.message == "Error receiving socket data!"
    assert cur.error_code == -493
    assert cur.error_msg == 'Syntax: Unknown class "xyz". select * from xyz'
    assert cur.rownumber == -1
    assert cur._total_rows_count == 0
    assert cur._query_results is None
finally:
    cur.close()
    conn.close()

logging.info('Test completed.')
