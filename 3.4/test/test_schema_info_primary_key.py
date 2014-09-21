from CUBRIDPy.CUBRIDConnection import *
import logging

logging.root.setLevel(logging.INFO)
logging.info('Executing test: %s...' % __file__)

conn = CUBRIDConnection()
conn.connect()

try:
    exported_keys = conn.schema_info('primary key', 'record')
    assert len(exported_keys) == 4

    assert exported_keys[0][0] == 'record'
    assert exported_keys[0][1] == 'athlete_code'
    assert exported_keys[0][2] == 3
    assert exported_keys[0][3] == 'pk_record_host_year_event_code_athlete_code_medal'

    assert exported_keys[3].table_name == 'record'
    assert exported_keys[3].attr_name == 'medal'
    assert exported_keys[3].key_seq == 4
    assert exported_keys[3].key_name == 'pk_record_host_year_event_code_athlete_code_medal'
finally:
    conn.close()

logging.info('Test completed.')
