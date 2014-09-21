from CUBRIDPy.constants.DATA_TYPES import *
from CUBRIDPy.constants.CUBRID import *
from CUBRIDPy.protocol.CUBRIDProtocol import CUBRIDProtocol
import logging

class CloseConnection(CUBRIDProtocol):
    """
    CloseConnection class implementation
    """

    def __init__(self, sock, CAS_INFO, locale = 'en-US'):
        """
        Class constructor
        :param socket sock: Connection socket
        :param array CAS_INFO: CAS
        """
        logging.debug('Initializing CloseConnection...')

        super(CloseConnection, self).__init__(sock, CAS_INFO, locale)


    def SendRequest(self):
        """
        Send request to the database engine
        """
        logging.debug('Send close connection request...')
        # specific data to be sent (array of: [part_length, part])
        data = [
            # command code
            [DATA_TYPES.BYTE_SIZEOF, CAS_FUNCTION_CODE.CAS_FC_CON_CLOSE]
        ]

        super(CloseConnection, self).PrepareRequestData(data)
        super(CloseConnection, self).SendRequest()


    def ProcessResponse(self):
        """
        Process response
        """
        logging.debug('Connection closed.')


    def GetResponse(self):
        """
        Get response from the database engine
        """
        logging.debug('Reading close connection response...')
        super(CloseConnection, self).GetResponse()

