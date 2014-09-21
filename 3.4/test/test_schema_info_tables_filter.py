from CUBRIDPy.CUBRIDConnection import *
import logging

logging.root.setLevel(logging.INFO)
logging.info('Executing test: %s...' % __file__)

conn = CUBRIDConnection()
conn.connect()

try:
    tables = conn.schema_info('tables', 'code')
    assert len(tables) == 1
    assert tables[0].name == 'code'
    assert tables[0].type == 2

    tables = conn.schema_info('tables', 'xyz')
    assert len(tables) == 0
finally:
    conn.close()

logging.info('Test completed.')
