from CUBRIDPy.CUBRIDConnection import *
import logging

logging.root.setLevel(logging.INFO)
logging.info('Executing test: %s...' % __file__)

conn = CUBRIDConnection()
conn.connect()

try:
    views = conn.schema_info('views')
    assert len(views) == 17
    assert views[0].name == 'db_collation'
    assert views[0].type == 0
    assert views[2].name == 'db_stored_procedure'
    assert views[16].name == 'db_class'
finally:
    conn.close()

logging.info('Test completed.')
