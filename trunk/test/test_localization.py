import logging
from CUBRIDPy.localization.ERROR_ID import ERROR_ID
from CUBRIDPy.localization.localization import get_error_message

logging.root.setLevel(logging.INFO)
logging.info('Executing test: %s...' % __file__)

error_msg = get_error_message('en-us')
assert error_msg == ''

error_msg = get_error_message('en-us', ERROR_ID.INCORRECT_LOCALE_VALUE)
assert error_msg == 'Incorrect localization value'

error_msg = get_error_message('en-us', -1002)
assert error_msg == 'CAS_ER_NO_MORE_MEMORY'

error_msg = get_error_message('en-us', -2013)
assert error_msg == 'CUBRID_ER_SQL_UNPREPARE'

logging.info('Test completed.')
