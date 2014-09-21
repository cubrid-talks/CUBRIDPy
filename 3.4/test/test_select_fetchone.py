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
    assert cur._total_rows_count == 6
    assert cur.rownumber == -1

    row = cur.fetchone()
    assert cur.rownumber == 0
    assert row[0] == 'X'
    assert row[1] == 'Mixed'

    logging.info(cur.fetchone())
    assert cur.rownumber == 1
    logging.info(cur.fetchone())
    logging.info(cur.fetchone())
    logging.info(cur.fetchone())
    row = cur.fetchone()
    assert cur.rownumber == 5
    assert row[0] == 'G'
    assert row[1] == 'Gold'

    row = cur.fetchone()
    assert cur.rownumber == -1
    assert row is None
finally:
    cur.close()
    conn.close()

logging.info('Test completed.')
