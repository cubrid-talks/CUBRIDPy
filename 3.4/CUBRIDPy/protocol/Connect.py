from CUBRIDPy.constants.CUBRID import *
from CUBRIDPy.localization.ERROR_ID import ERROR_ID
from CUBRIDPy.localization.localization import get_error_message
from CUBRIDPy.protocol.CUBRIDProtocol import CUBRIDProtocol
import logging

class Connect(CUBRIDProtocol):
    """
    Connect class implementation
    """

    def __init__(self, sock, database, user, password, locale = 'en-US'):
        """
        Class constructor
        :param socket sock: Connection socket
        :param string database: Database
        :param string user: User id
        :param string password: User password
        """
        logging.debug('Requesting database connection...')
        self._session_id = -1
        self._database = database
        self._user = user
        self._password = password

        super(Connect, self).__init__(sock, None, locale)

        return

    def SendRequest(self):
        """
        Send request to the database engine
        """
        logging.debug('Send database connection request...')
        database = self._database.ljust(32, chr(0))
        user = self._user.ljust(32, chr(0))
        password = self._password.ljust(32, chr(0))
        url = ''.ljust(512, chr(0))
        filler = ''.ljust(20, chr(0))
        send_buffer = '%s%s%s%s%s' % (database, user, password, url, filler)

        try:
            self._sock.sendall(send_buffer)
        except:
            raise Exception(get_error_message(self._locale, ERROR_ID.ERROR_SENDING_DATA))

        return

    def ProcessResponse(self):
        """
        Process response
        """
        self._BROKER_INFO = self._response_buffer.ReadBytesArray(SOCKET.BROKER_INFO_SIZEOF)
        self._session_id = self._response_buffer.ReadInt()
        logging.debug('Session id: %d.' % self._session_id)
        logging.debug('Connected.')

        return

    def GetResponse(self):
        """
        Get response from the database engine
        """
        logging.debug('Reading database engine version response...')
        super(Connect, self).GetResponse()

        return
