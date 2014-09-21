from datetime import time, date
from CUBRIDPy.CUBRIDConnection import *
import logging

logging.root.setLevel(logging.INFO)
logging.info('Executing test: %s...' % __file__)

conn = CUBRIDConnection()
conn.connect()
cur = conn.cursor()

try:
    cur.batch_execute([
        'drop table if exists test_data_types',
        'create table test_data_types(' +
        'a bigint,' +
        'b character(1),' +
        'c date,' +
        'd datetime,' +
        'e double,' +
        'f float,' +
        'g integer,' +
        'h monetary,' +
        'i national character(1),' +
        'j national character varying(100),' +
        'k numeric(15,0),' +
        'l character varying(100),' +
        'm time,' +
        'n timestamp,' +
        'o character varying(4096))'
    ])
    logging.info('Table test_data_types was created.')
    cur.execute(
        'insert into test_data_types values(15, \'a\', \'2012-10-02\',' +
        '\'2012-10-02 13:25:45\', 1.5, 2.5, 14, 3.14, N\'9\', N\'95\', 16, \'varchar\', ' +
        '\'1899-12-31 13:25:45\',' +
        '\'2012-10-02 13:25:45\', \'varchar\')'
    )
    logging.info('Inserted a record into table test_data_types.')

    cur.execute('select * from test_data_types')
    row = cur.fetchone()
    assert row[0] == 15
    assert row[1] == 'a'
    assert row[2] == date(2012, 10, 2)
    assert row[3] == datetime.datetime(2012, 10, 2, 13, 25, 45)
    assert row[4] == 1.5
    assert row[5] == 2.5
    assert row[6] == 14
    assert row[7] == 3.14
    assert row[8] == '9'
    assert row[9] == '95'
    assert row[10] == 16.0
    assert row[11] == 'varchar'
    assert row[12] == time(13, 25, 45)
    assert row[13] == datetime.datetime(2012, 10, 2, 13, 25, 45, 0)
    assert row[14] == 'varchar'
finally:
    cur.execute('drop table if exists test_data_types')
    cur.close()
    conn.close()

logging.info('Test completed.')
