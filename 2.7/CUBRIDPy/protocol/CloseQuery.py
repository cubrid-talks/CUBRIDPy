from CUBRIDPy.constants.DATA_TYPES import *
from CUBRIDPy.constants.CUBRID import *
from CUBRIDPy.protocol.CUBRIDProtocol import CUBRIDProtocol
import logging

class CloseQuery(CUBRIDProtocol):
    """
    CloseQuery class implementation
    """

    def __init__(self, sock, CAS_INFO, query_handle, locale = 'en-US'):
        """
        Class constructor
        :param socket sock: Connection socket.
        :param array CAS_INFO: CAS data.
        """
        logging.debug('Closing connection...')
        self._query_handle = query_handle

        super(CloseQuery, self).__init__(sock, CAS_INFO, locale)

        return

    def SendRequest(self):
        """
        Send request to the database engine
        """
        logging.debug('Sending close query request...')

        # specific data to be sent (array of: [part_length, part])
        data = [
            # command code
            [DATA_TYPES.BYTE_SIZEOF, CAS_FUNCTION_CODE.CAS_FC_CLOSE_REQ_HANDLE],
            # data length
            [DATA_TYPES.INT_SIZEOF, DATA_TYPES.INT_SIZEOF],
            # data
            [DATA_TYPES.INT_SIZEOF, self._query_handle],
            # data length
            [DATA_TYPES.INT_SIZEOF, DATA_TYPES.BYTE_SIZEOF],
            # data
            [DATA_TYPES.BYTE_SIZEOF, 0] # autocommit mode
        ]

        super(CloseQuery, self).PrepareRequestData(data)
        super(CloseQuery, self).SendRequest()

        return

    def ProcessResponse(self):
        """
        Process response
        """
        logging.debug('Close query data received successfully.')

        return

    def GetResponse(self):
        """
        Get response from the database engine
        """
        logging.debug('Reading close query response...')
        super(CloseQuery, self).GetResponse()

        return
