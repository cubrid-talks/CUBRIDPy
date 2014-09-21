from datetime import  date
from CUBRIDPy.CUBRIDConnection import *
import logging

logging.root.setLevel(logging.INFO)
logging.info('Executing test: %s...' % __file__)

conn = CUBRIDConnection()
conn.connect()
cur = conn.cursor()

try:
    query = 'select * from game'
    cur.execute(query)
    assert cur._total_rows_count == 8653
    assert cur.rownumber == -1

    rows = cur.fetchall()
    assert rows[0][0] == 2004
    assert rows[0][1] == 20021
    assert rows[0][2] == 14345
    assert rows[0][3] == 30116
    assert rows[0][4] == 'NGR'
    assert rows[0][5] == 'B'
    assert rows[0][6] == date(2004, 8, 28)

    last_row = len(rows) - 1
    assert rows[last_row][0] == 1988
    assert rows[last_row][1] == 20084
    assert rows[last_row][2] == 16631
    assert rows[last_row][3] == 30060
    assert rows[last_row][4] == 'AUS'
    assert rows[last_row][5] == 'S'
    assert rows[last_row][6] == date(1988, 9, 20)

    assert cur.rownumber == -1
finally:
    cur.close()
    conn.close()

logging.info('Test completed.')
