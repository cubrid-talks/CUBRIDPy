from CUBRIDPy.constants.DATA_TYPES import *
from CUBRIDPy.constants.CUBRID import *
from CUBRIDPy.protocol.CUBRIDProtocol import CUBRIDProtocol
import logging

class Fetch(CUBRIDProtocol):
    """
    Fetch class implementation
    """

    def __init__(self, sock, CAS_INFO, query_handle, query_packet, current_tuple_count, locale = 'en-US'):
        """
        Class constructor
        :param socket sock: Connection socket.
        :param array CAS_INFO: CAS data.
        :param locale: localization information
        :return:
        """
        logging.debug('Initializing Fetch...')
        self._query_handle = query_handle
        self._query_packet = query_packet
        self._current_tuple_count = current_tuple_count
        self._fetch_results = None

        super(Fetch, self).__init__(sock, CAS_INFO, locale)

        return

    def SendRequest(self):
        """
        Send request to the database engine
        """
        logging.debug('Sending fetch request...')

        # specific data to be sent (array of: [part_length, part])
        data = [
            # command code
            [DATA_TYPES.BYTE_SIZEOF, CAS_FUNCTION_CODE.CAS_FC_FETCH],
            # data length
            [DATA_TYPES.INT_SIZEOF, DATA_TYPES.INT_SIZEOF],
            # data
            [DATA_TYPES.INT_SIZEOF, self._query_handle],
            # data length
            [DATA_TYPES.INT_SIZEOF, DATA_TYPES.INT_SIZEOF],
            # data
            [DATA_TYPES.INT_SIZEOF, self._current_tuple_count + 1],
            # data length
            [DATA_TYPES.INT_SIZEOF, DATA_TYPES.INT_SIZEOF],
            # data
            [DATA_TYPES.INT_SIZEOF, 100], # Fetch size; 0 = default; recommended = 100
            # data length
            [DATA_TYPES.INT_SIZEOF, DATA_TYPES.BYTE_SIZEOF],
            # data
            [DATA_TYPES.BYTE_SIZEOF, 0], # autocommit mode
            # data length
            [DATA_TYPES.INT_SIZEOF, DATA_TYPES.INT_SIZEOF],
            # data
            [DATA_TYPES.INT_SIZEOF, 0] # Is the ResultSet index...?
        ]

        super(Fetch, self).PrepareRequestData(data)
        super(Fetch, self).SendRequest()

        return

    def ProcessResponse(self):
        """
        Process response
        """
        logging.debug('Fetch data received successfully.')
        self.tupleCount = self._response_buffer.ReadInt()
        self._fetch_results = self._query_packet._get_columns_data(self.tupleCount, self._response_buffer)

        return

    def GetResponse(self):
        """
        Get response from the database engine
        """
        logging.debug('Reading fetch response...')
        super(Fetch, self).GetResponse()

        return

