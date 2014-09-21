from CUBRIDPy.CUBRIDConnection import *
import logging

logging.root.setLevel(logging.INFO)
logging.info('Executing test: %s...' % __file__)

conn = CUBRIDConnection()
conn.connect()

try:
    imported_keys = conn.schema_info('imported keys', 'game')
    assert len(imported_keys) == 2
    assert imported_keys[0][0] == 'game'
    assert imported_keys[0][1] == 'event_code'
    assert imported_keys[0][2] == 'event'
    assert imported_keys[0][3] == 'code'
    assert imported_keys[0][4] == 1
    assert imported_keys[0][5] == 1
    assert imported_keys[0][6] == 1
    assert imported_keys[0][7] == 'fk_game_event_code'
    assert imported_keys[0][8] == 'pk_event_code'

    assert imported_keys[1].pktable_name == 'game'
    assert imported_keys[1].pkcolumn_name == 'athlete_code'
    assert imported_keys[1].fktable_name == 'athlete'
    assert imported_keys[1].fkcolumn_name == 'code'
    assert imported_keys[1].key_seq == 1
    assert imported_keys[1].update_action == 1
    assert imported_keys[1].delete_action == 1
    assert imported_keys[1].fk_name == 'fk_game_athlete_code'
    assert imported_keys[1].pk_name == 'pk_athlete_code'
finally:
    conn.close()

logging.info('Test completed.')
