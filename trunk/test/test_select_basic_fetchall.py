from CUBRIDPy.CUBRIDConnection import *
import logging

logging.root.setLevel(logging.INFO)
logging.info('Executing test: %s...' % __file__)

conn = CUBRIDConnection()
conn.connect()
cur = conn.cursor()

try:
    query = 'select * from nation'
    cur.execute(query)

    data = cur.fetchall()
    assert len(data) == 215

    for row in data:
        row_val = ''
        for col in range(0, len(row)):
            row_val += str(row[col])
            if col < len(row) - 1:
                row_val += ', '
        logging.info(row_val)
        if row[0] == 'SRB':
            assert row[0] == 'SRB'
            assert row[1] == 'Serbia'
            assert row[2] == 'Europe'
            assert row[3] == 'Beograd'
        if row[0] == 'AFG':
            assert row[0] == 'AFG'
            assert row[1] == 'Afghanistan'
            assert row[2] == 'Asia'
            assert row[3] == 'Kabul'
finally:
    cur.close()
    conn.close()

logging.info('Test completed.')
