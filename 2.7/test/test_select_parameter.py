from CUBRIDPy.CUBRIDConnection import *
import logging

logging.root.setLevel(logging.INFO)
logging.info('Executing test: %s...' % __file__)

conn = CUBRIDConnection()
conn.connect()
cur = conn.cursor()
code = 'X'

try:
    query = "select * from code where s_name = '%s'"
    cur.execute(query, code)
    assert cur._last_query == 'select * from code where s_name = \'%s\'' % code
    assert cur._total_rows_count == 1

    row = cur.fetchone()
    assert row[0] == code
    assert row[1] == 'Mixed'
finally:
    cur.close()
    conn.close()

logging.info('Test completed.')
