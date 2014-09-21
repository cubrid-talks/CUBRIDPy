from CUBRIDPy.CUBRIDConnection import *
import logging

logging.root.setLevel(logging.INFO)
logging.info('Executing test: %s...' % __file__)

clob_data = 'My one and only CUBRID CLOB test data'
conn = CUBRIDConnection()
conn.connect()
cur = conn.cursor()

try:
    cur.executemany(
        ['drop table if exists test_lob',
         'create table test_lob(cl CLOB)',
         'insert into test_lob values(CHAR_TO_CLOB(\'%s\'))' % clob_data
        ], None)

    query = 'select * from test_lob'
    cur.execute(query)
    row = cur.fetchone()
    lob_handle, lob_type, lob_buffer_size = row[0]
    assert lob_type == CUBRID_DATA_TYPE.CCI_U_TYPE_CLOB
    assert lob_buffer_size == len(clob_data)
    read_clob_data = cur.read_lob(lob_handle, lob_type, 0, lob_buffer_size)
    assert str(read_clob_data) == clob_data
finally:
    cur.execute('drop table if exists test_lob')
    cur.close()
    conn.close()

logging.info('Test completed.')
