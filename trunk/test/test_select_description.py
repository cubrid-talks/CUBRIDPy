from CUBRIDPy.CUBRIDConnection import *
import logging

logging.root.setLevel(logging.INFO)
logging.info('Executing test: %s...' % __file__)

conn = CUBRIDConnection()
conn.connect()
cur = conn.cursor()

try:
    query = 'select * from game'
    cur.execute(query)
    assert cur.description[0][0] == 'host_year'
    assert cur.description[0][1] == type(int)
    assert cur.description[0][2] is None
    assert cur.description[0][3] is None
    assert cur.description[0][4] == 10
    assert cur.description[0][5] == 0
    assert cur.description[0][6] == True

    assert cur.description[1][0] == 'event_code'
    assert cur.description[2][0] == 'athlete_code'
    assert cur.description[3][0] == 'stadium_code'
    assert cur.description[4][0] == 'nation_code'
    assert cur.description[5][0] == 'medal'

    assert cur.description[6][0] == 'game_date'
    assert cur.description[6][1] == type(datetime.date)
    assert cur.description[6][2] is None
    assert cur.description[6][3] is None
    assert cur.description[6][4] == 10
    assert cur.description[6][5] == 0
    assert cur.description[6][6] == False
finally:
    cur.close()
    conn.close()

logging.info('Test completed.')
