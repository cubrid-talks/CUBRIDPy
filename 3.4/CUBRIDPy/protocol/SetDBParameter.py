from CUBRIDPy.constants.DATA_TYPES import *
from CUBRIDPy.constants.CUBRID import *
from CUBRIDPy.protocol.CUBRIDProtocol import CUBRIDProtocol
import logging

class SetDBParameter(CUBRIDProtocol):
    """
    SetDBParameter class implementation
    """

    def __init__(self, sock, CAS_INFO, parameter_id, parameter_value, locale = 'en-US'):
        """
        Class constructor
        :param socket sock: Connection socket.
        :param array CAS_INFO: CAS data.
        :param parameter_value
        :param locale: localization information
        :return:
        """
        logging.debug('Initializing Set DB Parameter...')
        self._parameter_value = parameter_value
        self._parameter_id = parameter_id

        super(SetDBParameter, self).__init__(sock, CAS_INFO, locale)

        return

    def SendRequest(self):
        """
        Send request to the database engine
        """
        logging.debug('Sending set db parameter mode request...')

        # specific data to be sent (array of: [part_length, part])
        data = [
            # command code
            [DATA_TYPES.BYTE_SIZEOF, CAS_FUNCTION_CODE.CAS_FC_SET_DB_PARAMETER],
            # data length
            [DATA_TYPES.INT_SIZEOF, DATA_TYPES.INT_SIZEOF],
            # data
            [DATA_TYPES.INT_SIZEOF, self._parameter_id],
            # data length
            [DATA_TYPES.INT_SIZEOF, DATA_TYPES.INT_SIZEOF],
            # data
            [DATA_TYPES.INT_SIZEOF, self._parameter_value]
        ]

        super(SetDBParameter, self).PrepareRequestData(data)
        super(SetDBParameter, self).SendRequest()

        return

    def ProcessResponse(self):
        """
        Process response
        """
        logging.debug('Set db parameter data received successfully.')

        return

    def GetResponse(self):
        """
        Get response from the database engine
        """
        logging.debug('Reading close query response...')
        super(SetDBParameter, self).GetResponse()

        return

