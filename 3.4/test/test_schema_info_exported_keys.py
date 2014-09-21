from CUBRIDPy.CUBRIDConnection import *
import logging

logging.root.setLevel(logging.INFO)
logging.info('Executing test: %s...' % __file__)

conn = CUBRIDConnection()
conn.connect()

try:
    exported_keys = conn.schema_info('exported keys', 'athlete')
    assert len(exported_keys) == 1
    assert exported_keys[0][0] == 'game'
    assert exported_keys[0][1] == 'athlete_code'
    assert exported_keys[0][2] == 'athlete'
    assert exported_keys[0][3] == 'code'
    assert exported_keys[0][4] == 1
    assert exported_keys[0][5] == 1
    assert exported_keys[0][6] == 1
    assert exported_keys[0][7] == 'fk_game_athlete_code'
    assert exported_keys[0][8] == 'pk_athlete_code'

    assert exported_keys[0].pktable_name == 'game'
    assert exported_keys[0].pkcolumn_name == 'athlete_code'
    assert exported_keys[0].fktable_name == 'athlete'
    assert exported_keys[0].fkcolumn_name == 'code'
    assert exported_keys[0].key_seq == 1
    assert exported_keys[0].update_action == 1
    assert exported_keys[0].delete_action == 1
    assert exported_keys[0].fk_name == 'fk_game_athlete_code'
    assert exported_keys[0].pk_name == 'pk_athlete_code'
finally:
    conn.close()

logging.info('Test completed.')
