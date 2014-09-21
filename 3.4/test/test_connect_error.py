from CUBRIDPy.CUBRIDConnection import *
import logging

logging.root.setLevel(logging.DEBUG)
logging.info('Executing test: %s...' % __file__)

conn = CUBRIDConnection('127.0.0.1', 30000, 'demodb', 'x_public', '')
try:
    conn.connect()
    raise Exception('Connect successful!')
except Exception as ex:
    assert conn.error_code == -165
    assert conn.error_msg == 'User "x_public" is invalid.'

conn = CUBRIDConnection('127.0.0.1', 30000, 'demodb', 'public', 'x')
try:
    conn.connect()
    raise Exception('Connect successful!')
except Exception as ex:
    assert conn.error_code == -171
    assert conn.error_msg == 'Incorrect or missing password.'

conn = CUBRIDConnection('127.0.0.1', 30000, 'x_demodb', 'public', '')
try:
    conn.connect()
    raise Exception('Connect successful!')
except Exception as ex:
    assert conn.error_code == -677
    assert conn.error_msg == "Failed to connect to database server, 'x_demodb', on the following host(s): localhost:localhost"

conn = CUBRIDConnection('127.0.0.1', 21, 'demodb', 'public', '')
try:
    conn.connect()
    raise Exception('Connect successful!')
except Exception as ex:
    assert str(ex) == "[Errno 10061] No connection could be made because the target machine actively refused it"

logging.info('Please wait...')
conn = CUBRIDConnection('xyz', 30000, 'demodb', 'public', '')
try:
    conn.connect()
    raise Exception('Connect successful!')
except Exception as ex:
    assert str(ex) == "[Errno 11004] getaddrinfo failed"

logging.info('Test completed.')


