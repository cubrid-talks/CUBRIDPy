from CUBRIDPy.constants.DATA_TYPES import *
from CUBRIDPy.constants.CUBRID import *
from CUBRIDPy.protocol.CUBRIDProtocol import CUBRIDProtocol
import logging

class GetDatabaseVersion(CUBRIDProtocol):
    """
    GetDatabaseVersion class implementation
    """

    def __init__(self, sock, CAS_INFO, locale = 'en-US'):
        """
        Class constructor
        :param socket sock: Connection socket.
        :param array CAS_INFO: CAS data.
        """
        logging.debug('Getting database version...')
        self.db_version = None

        super(GetDatabaseVersion, self).__init__(sock, CAS_INFO, locale)

        return

    def SendRequest(self):
        """
        Send request to the database engine
        """
        logging.debug('Send database engine version request...')

        # specific data to be sent (array of: [part_length, part])
        data = [
            # command code
            [DATA_TYPES.BYTE_SIZEOF, CAS_FUNCTION_CODE.CAS_FC_GET_DB_VERSION],
            # data length
            [DATA_TYPES.INT_SIZEOF, DATA_TYPES.BYTE_SIZEOF],
            # data
            [DATA_TYPES.BYTE_SIZEOF, 1] # 1 is auto-commit flag
        ]

        super(GetDatabaseVersion, self).PrepareRequestData(data)
        super(GetDatabaseVersion, self).SendRequest()

        return

    def ProcessResponse(self):
        """
        Process response
        """
        self.db_version = self._response_buffer.ReadStringToEnd()
        logging.debug('Database engine version is: %s.' % self.db_version)

        return

    def GetResponse(self):
        """
        Get response from the database engine
        """
        logging.debug('Reading database engine version response...')
        super(GetDatabaseVersion, self).GetResponse()

        return
