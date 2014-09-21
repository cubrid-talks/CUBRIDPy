from CUBRIDPy.constants.DATA_TYPES import *
from CUBRIDPy.constants.CUBRID import *
from CUBRIDPy.protocol.CUBRIDProtocol import CUBRIDProtocol
import logging

class SetAutoCommitMode(CUBRIDProtocol):
    """
    SetAutoCommitMode class implementation
    """

    def __init__(self, sock, CAS_INFO, auto_commit_mode, locale = 'en-US'):
        """
        Class constructor
        :param auto_commit_mode: Auto-commit mode
        :param socket sock: Connection socket.
        :param array CAS_INFO: CAS data.
        :param str locale: locale data
        """
        logging.debug('Initializing SetAutoCommitMode...')
        self._auto_commit_mode = 1 if auto_commit_mode else 0

        super(SetAutoCommitMode, self).__init__(sock, CAS_INFO, locale)

        return

    def SendRequest(self):
        """
        Send request to the database engine
        """
        logging.debug('Sending set auto commit mode request...')

        # specific data to be sent (array of: [part_length, part])
        data = [
            # command code
            [DATA_TYPES.BYTE_SIZEOF, CAS_FUNCTION_CODE.CAS_FC_SET_DB_PARAMETER],
            # data length
            [DATA_TYPES.INT_SIZEOF, DATA_TYPES.INT_SIZEOF],
            # data
            [DATA_TYPES.INT_SIZEOF, CCI_DB_PARAM.CCI_PARAM_AUTO_COMMIT],
            # data length
            [DATA_TYPES.INT_SIZEOF, DATA_TYPES.INT_SIZEOF],
            # data
            [DATA_TYPES.INT_SIZEOF, self._auto_commit_mode]
        ]

        super(SetAutoCommitMode, self).PrepareRequestData(data)
        super(SetAutoCommitMode, self).SendRequest()

        return

    def ProcessResponse(self):
        """
        Process response
        """
        logging.debug('Set auto commit mode data received successfully.')

        return

    def GetResponse(self):
        """
        Get response from the database engine
        """
        logging.debug('Reading close query response...')
        super(SetAutoCommitMode, self).GetResponse()

        return

