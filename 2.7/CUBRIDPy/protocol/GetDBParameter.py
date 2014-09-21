from CUBRIDPy.constants.DATA_TYPES import *
from CUBRIDPy.constants.CUBRID import *
from CUBRIDPy.protocol.CUBRIDProtocol import CUBRIDProtocol
import logging

class GetDBParameter(CUBRIDProtocol):
    """
    GetDBParameter class implementation
    """

    def __init__(self, sock, CAS_INFO, parameter_id, locale = 'en-US'):
        """
        Class constructor
        :param str parameter_id: parameter id
        :param socket sock: Connection socket.
        :param array CAS_INFO: CAS data.
        :param str locale: localization information
        :return:
        """
        logging.debug('Initializing Get DB Parameter...')
        self._parameter_id = parameter_id
        self.parameter_value = -1

        super(GetDBParameter, self).__init__(sock, CAS_INFO, locale)

        return

    def SendRequest(self):
        """
        Send request to the database engine
        """
        logging.debug('Sending get db parameter mode request...')

        # specific data to be sent (array of: [part_length, part])
        data = [
            # command code
            [DATA_TYPES.BYTE_SIZEOF, CAS_FUNCTION_CODE.CAS_FC_GET_DB_PARAMETER],
            # data length
            [DATA_TYPES.INT_SIZEOF, DATA_TYPES.INT_SIZEOF],
            # data
            [DATA_TYPES.INT_SIZEOF, self._parameter_id]
        ]

        super(GetDBParameter, self).PrepareRequestData(data)
        super(GetDBParameter, self).SendRequest()

        return

    def ProcessResponse(self):
        """
        Process response
        """
        logging.debug('Get db parameter data received successfully.')
        self.parameter_value = self._response_buffer.ReadInt()

        return

    def GetResponse(self):
        """
        Get response from the database engine
        """
        logging.debug('Reading close query response...')
        super(GetDBParameter, self).GetResponse()

        return
