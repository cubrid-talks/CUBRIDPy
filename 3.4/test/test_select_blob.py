import random
from CUBRIDPy.CUBRIDConnection import *
import logging

logging.root.setLevel(logging.INFO)
logging.info('Executing test: %s...' % __file__)

blob_data = ''.join([chr(random.randrange(ord('0'), ord('0') + 2)) for _ in range(512 * 8)])
conn = CUBRIDConnection()
conn.connect()
cur = conn.cursor()

try:
    cur.executemany(
        ['drop table if exists test_lob',
         'create table test_lob(cl BLOB)',
         'insert into test_lob values(BIT_TO_BLOB(B\'%s\'))' % blob_data
        ], None)

    query = 'select * from test_lob'
    cur.execute(query)
    row = cur.fetchone()
    lob_handle, lob_type, lob_buffer_size = row[0]
    assert lob_type == CUBRID_DATA_TYPE.CCI_U_TYPE_BLOB
    assert lob_buffer_size == len(blob_data) / 8
    read_blob_data = cur.read_lob(lob_handle, lob_type, 0, lob_buffer_size)
    first_byte = int(blob_data[0:8], 2)
    last_byte = int(blob_data[511 * 8:512 * 8], 2)
    assert first_byte == read_blob_data[0]
    assert last_byte == read_blob_data[511]
finally:
    cur.execute('drop table if exists test_lob')
    cur.close()
    conn.close()

logging.info('Test completed.')
