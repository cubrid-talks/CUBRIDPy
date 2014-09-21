from CUBRIDPy.CUBRIDConnection import *
import logging

logging.root.setLevel(logging.INFO)
logging.info('Executing test: %s...' % __file__)

conn = CUBRIDConnection()
conn.connect()
cur = conn.cursor()

try:
    executed_queries_count = cur.batch_execute([
        'create table',
        'drop table ttt'
    ])
    assert executed_queries_count == 2
    assert cur.error_code == 0
    assert cur.error_msg == ''
    assert cur.batch_execute_error_codes[0] == -493
    assert cur.batch_execute_error_msgs[0] == 'Syntax: Syntax error: unexpected END OF STATEMENT '
    assert cur.batch_execute_error_codes[1] == -494
    assert cur.batch_execute_error_msgs[1] == 'Semantic: Unknown class "ttt". drop ttt'
finally:
    cur.close()
    conn.close()

logging.info('Test completed.')
