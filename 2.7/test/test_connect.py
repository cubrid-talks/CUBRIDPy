from CUBRIDPy.CUBRIDConnection import *
import logging

logging.root.setLevel(logging.INFO)
logging.info('Executing test: %s...' % __file__)

conn = CUBRIDConnection('127.0.0.1', 30000, 'demodb', 'public', '')
try:
    conn.connect()
    assert conn.error_code == 0
    logging.info('Connect successful!')
    conn.close()
except Exception, ex:
    logging.error(str(ex))
    conn.close()

conn = CUBRIDConnection('localhost', 30000, 'demodb', 'public', '')
try:
    conn.connect()
    assert conn.error_code == 0
    logging.info('Connect successful!')
    conn.close()
except Exception, ex:
    logging.error(str(ex))
    conn.close()

conn = CUBRIDConnection(database = 'demodb', broker_address = 'localhost')
try:
    conn.connect()
    assert conn.error_code == 0
    logging.info('Connect successful!')
    conn.close()
except Exception, ex:
    logging.error(str(ex))
    conn.close()

conn = CUBRIDConnection()
try:
    conn.connect()
    assert conn.error_code == 0
    assert conn.user == 'public'
    assert conn.database == 'demodb'
    assert conn.broker_address == '127.0.0.1'
    logging.info('Connect successful!')
    conn.close()
except Exception, ex:
    logging.error(str(ex))
    conn.close()

logging.info('Test completed.')


