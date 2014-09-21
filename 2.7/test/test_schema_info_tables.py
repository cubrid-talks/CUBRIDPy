from CUBRIDPy.CUBRIDConnection import *
import logging

logging.root.setLevel(logging.INFO)
logging.info('Executing test: %s...' % __file__)

conn = CUBRIDConnection()
conn.connect()

try:
    tables = conn.schema_info('tables')
    assert len(tables) == 34
    assert tables[0][0] == 'db_collation'
    assert tables[0].name == 'db_collation'
    assert tables[0].type == 0
    assert tables[24].name == 'code'
    assert tables[24].type == 2
    assert tables[33].name == '_cub_schema_comments'
finally:
    conn.close()

logging.info('Test completed.')
