from CUBRIDPy.constants.DATA_TYPES import *
from CUBRIDPy.constants.CUBRID import *
from CUBRIDPy.protocol.CUBRIDProtocol import CUBRIDProtocol
import logging

class Rollback(CUBRIDProtocol):
    """
    Rollback class implementation
    """

    def __init__(self, sock, CAS_INFO, locale = 'en-US'):
        """
        Class constructor
        :param socket sock: Connection socket
        :param array CAS_INFO: CAS data
        :param str locale: locale data
        """
        logging.debug('Initializing Rollback...')

        super(Rollback, self).__init__(sock, CAS_INFO, locale)

        return

    def SendRequest(self):
        """
        Send request to the database engine
        """
        logging.debug('Sending rollback request...')

        # specific data to be sent (array of: [part_length, part])
        data = [
            # command code
            [DATA_TYPES.BYTE_SIZEOF, CAS_FUNCTION_CODE.CAS_FC_END_TRAN],
            # data length
            [DATA_TYPES.INT_SIZEOF, DATA_TYPES.BYTE_SIZEOF],
            # data
            [DATA_TYPES.BYTE_SIZEOF, CCI_TRANSACTION_TYPE.CCI_TRAN_ROLLBACK]
        ]

        super(Rollback, self).PrepareRequestData(data)
        super(Rollback, self).SendRequest()

        return

    def ProcessResponse(self):
        """
        Process response
        """
        logging.debug('Rollback data received successfully.')

        return

    def GetResponse(self):
        """
        Get response from the database engine
        """
        logging.debug('Reading close query response...')
        super(Rollback, self).GetResponse()

        return

