from CUBRIDPy.CUBRIDConnection import *
import logging

logging.root.setLevel(logging.INFO)
logging.info('Executing test: %s...' % __file__)

conn = CUBRIDConnection()
conn.connect()
cur = conn.cursor()

try:
    query = 'select * from nation'
    cur.execute(query)
    assert cur._total_rows_count == 215
    assert cur.rownumber == -1

    row = cur.fetchoneassoc()
    assert cur.rownumber == 0
    assert row['code'] == 'SRB'
    assert row['name'] == 'Serbia'
    assert row['continent'] == 'Europe'
    assert row['capital'] == 'Beograd'
    logging.info(cur.fetchoneassoc())
    assert cur.rownumber == 1
    logging.info(cur.fetchoneassoc())
    logging.info(cur.fetchoneassoc())

    row = cur.fetchoneassoc()
    assert cur.rownumber == 4
    assert row['code'] == 'ZIM'
    assert row['name'] == 'Zimbabwe'
finally:
    cur.close()
    conn.close()

logging.info('Test completed.')
