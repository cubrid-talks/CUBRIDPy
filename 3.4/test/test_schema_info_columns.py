from CUBRIDPy.CUBRIDConnection import *
import logging

logging.root.setLevel(logging.INFO)
logging.info('Executing test: %s...' % __file__)

conn = CUBRIDConnection()
conn.connect()

try:
    columns = conn.schema_info('columns', 'code')
    assert len(columns) == 2
    assert columns[0][0] == 's_name'
    assert columns[0][1] == 1
    assert columns[0][2] == 0
    assert columns[0][3] == 1
    assert columns[0][4] == 0
    assert columns[0][5] == False
    assert columns[0][6] == 0
    assert columns[0][7] == False
    assert columns[0][8] == ''
    assert columns[0][9] == 1
    assert columns[0][10] == 'code'
    assert columns[0][11] == 'code'
    assert columns[0][12] == False

    assert columns[1].attr_name == 'f_name'
    assert columns[1].domain == 2
    assert columns[1].scale == 0
    assert columns[1].precision == 6
    assert columns[1].indexed == 0
    assert columns[1].not_null == False
    assert columns[1].shared == 0
    assert columns[1].unique == False
    assert columns[1].default == ''
    assert columns[1].attr_order == 2
    assert columns[1].table_name == 'code'
    assert columns[1].source_class == 'code'
    assert columns[1].is_key == False
finally:
    conn.close()

logging.info('Test completed.')
