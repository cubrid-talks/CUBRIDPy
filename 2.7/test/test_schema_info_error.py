from CUBRIDPy.CUBRIDConnection import *
import logging

logging.root.setLevel(logging.INFO)
logging.info('Executing test: %s...' % __file__)

conn = CUBRIDConnection()
conn.connect()
val = True

try:
    conn.schema_info('xyz', 'record')
except Error, ex:
    assert ex._err_id == ERROR_ID.ERROR_INVALID_SCHEMA_TYPE
    assert ex._err_msg == 'Invalid schema type!'
finally:
    conn.close()

logging.info('Test completed.')
