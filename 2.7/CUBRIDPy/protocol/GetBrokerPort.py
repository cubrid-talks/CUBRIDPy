from CUBRIDPy.constants.DATA_TYPES import *
from CUBRIDPy.constants.PROTOCOL import PROTOCOL
from CUBRIDPy.localization.ERROR_ID import ERROR_ID
from CUBRIDPy.localization.localization import get_error_message
from CUBRIDPy.protocol.CUBRIDProtocol import CUBRIDProtocol
import logging
from CUBRIDPy.utils import helpers

class GetBrokerPort(CUBRIDProtocol):
    """
    GetBrokerPort class implementation
    """

    def __init__(self, sock, locale = 'en-US'):
        """
        Class constructor
        :param socket sock: Connection socket.
        :param str locale: locale data.
        """
        self.port = -1

        super(GetBrokerPort, self).__init__(sock, None, locale)

        return

    def SendRequest(self):
        """
        Send request to the database engine
        """
        logging.debug('Send broker port request...')
        send_buffer = 'CUBRK%c%c%c%c%c' % (3, chr(PROTOCOL.CAS_VER), 0, 0, 0)

        try:
            self._sock.sendall(send_buffer)
        except:
            raise Exception(get_error_message(self._locale, ERROR_ID.ERROR_SENDING_DATA))

        return

    def GetResponse(self):
        """
        Get response from the broker
        """
        try:
            response = self._sock.recv(DATA_TYPES.INT_SIZEOF)
            self.port = helpers.chr_array_to_num(response, 0, DATA_TYPES.INT_SIZEOF)
        except:
            raise Exception(get_error_message(self._locale, ERROR_ID.ERROR_RECEIVING_DATA))
        else:
            logging.debug('The returned broker port is: %d.' % self.port)

        return self

