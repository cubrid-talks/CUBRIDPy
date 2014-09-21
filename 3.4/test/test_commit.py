from CUBRIDPy.CUBRIDConnection import *
import logging

logging.root.setLevel(logging.INFO)
logging.info('Executing test: %s...' % __file__)

conn = CUBRIDConnection()
conn.connect()
cur = conn.cursor()

try:
    cur.executemany(
        [
            'drop table if exists autocommit_t',
            'create table autocommit_t(id int primary key, age int, name varchar(40))'
        ], None)

    conn.set_autocommit(False)
    assert conn.auto_commit == False

    cur.execute("insert into autocommit_t (name,id, age) values('for rollback', 6, 66)")

    conn.commit()
    cur.execute("select * from autocommit_t")
    rows = cur.fetchall()
    assert len(rows) == 1
finally:
    conn.set_autocommit(True)
    cur.execute("drop table if exists autocommit_t")
    cur.close()
    conn.close()

logging.info('Test completed.')
