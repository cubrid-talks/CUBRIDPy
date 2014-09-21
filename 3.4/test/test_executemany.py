from CUBRIDPy.CUBRIDConnection import *
import logging

logging.root.setLevel(logging.INFO)
logging.info('Executing test: %s...' % __file__)

conn = CUBRIDConnection()
conn.connect()
cur = conn.cursor()

try:
    row_count = cur.executemany([
        'drop table if exists mytest',
        'create table mytest(id int)',
        'insert into mytest values(10)',
        'insert into mytest values(20)',
    ], None)
    assert len(row_count) == 4

    cur.execute('select * from mytest')
    row = cur.fetchone()
    assert row[0] == 10
    row = cur.fetchone()
    assert row[0] == 20
finally:
    cur.execute('drop table if exists mytest')
    cur.close()
    conn.close()

logging.info('Test completed.')
