from CUBRIDPy.CUBRIDConnection import *
import logging

logging.root.setLevel(logging.INFO)
logging.info('Executing test: %s...' % __file__)

conn = CUBRIDConnection()
conn.connect()
cur = conn.cursor()

try:
    query = 'select 1 as "first", \'one\' as "second"'
    cur.execute(query)
    assert cur._total_rows_count == 1

    description = cur.description
    row = cur.fetchone()
    assert row[0] == 1
    assert row[1] == 'one'
    assert description[0] == ('first', type(int), None, None, 10L, 0, True)
    assert description[1] == ('second', type(str), None, None, -1L, 0, True)
finally:
    cur.close()
    conn.close()

logging.info('Test completed.')
