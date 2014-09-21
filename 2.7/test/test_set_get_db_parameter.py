from CUBRIDPy.CUBRIDConnection import *
import logging

logging.root.setLevel(logging.INFO)
logging.info('Executing test: %s...' % __file__)

conn = CUBRIDConnection()
conn.connect()
logging.info('Connect successful!')

try:
    in_param_value = CUBRID_ISOLATION_LEVEL.TRAN_REP_CLASS_COMMIT_INSTANCE
    conn.set_db_parameter(CCI_DB_PARAM.CCI_PARAM_ISOLATION_LEVEL, in_param_value)
    out_param_value = conn.get_db_parameter(CCI_DB_PARAM.CCI_PARAM_ISOLATION_LEVEL)
    assert in_param_value == out_param_value
finally:
    conn.close()

logging.info('Test completed.')
