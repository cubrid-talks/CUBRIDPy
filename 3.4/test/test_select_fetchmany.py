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

    rows = cur.fetchmany()
    assert len(rows) == 1
    assert cur._query_cursor_pos == 1
    assert rows[0][0] == 'SRB'
    assert rows[0][1] == 'Serbia'
    assert rows[0][2] == 'Europe'
    assert rows[0][3] == 'Beograd'

    rows = cur.fetchmany(100)
    assert len(rows) == 100
    assert cur._query_cursor_pos == 101
    assert rows[len(rows) - 1][0] == 'LBR'
    assert rows[len(rows) - 1][1] == 'Liberia'
    assert rows[len(rows) - 1][2] == 'Africa'
    assert rows[len(rows) - 1][3] == 'Monrovia'
finally:
    cur.close()
    conn.close()

logging.info('Test completed.')
