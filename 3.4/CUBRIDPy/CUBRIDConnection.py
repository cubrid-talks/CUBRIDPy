import errors
import logging, socket
from CUBRIDPy.CUBRIDCursor import *
from CUBRIDPy.protocol.GetBrokerPort import *
from CUBRIDPy.protocol.Connect import *
from CUBRIDPy.protocol.GetDBSchema import GetDBSchema
from CUBRIDPy.protocol.GetDatabaseVersion import *
from CUBRIDPy.protocol.CloseConnection import *
from CUBRIDPy.protocol.Commit import *
from CUBRIDPy.protocol.Rollback import *
from CUBRIDPy.protocol.SetAutoCommitMode import *
from CUBRIDPy.protocol.SetDBParameter import *
from CUBRIDPy.protocol.GetDBParameter import *

class CUBRIDConnection(object):
    """
    Connection class implementation
    http://www.python.org/dev/peps/pep-0249/
    """

    def __init__(self, broker_address = '127.0.0.1', broker_port = 30000, database = 'demodb', user = 'public',
                 password = ''):
        """
        Class constructor
        :param string broker_address: Broker address.
        :param int broker_port: Broker port.
        :param string database: Database name.
        :param string user: Database user id.
        :param string password: database user password.
        """
        # Protocol data
        self._CAS_INFO = [0, 0, 0, 0]
        self._BROKER_INFO = [0, 0, 0, 0, 0, 0, 0, 0]
        # Connection data
        self._broker_address = broker_address
        self._broker_port = broker_port
        self._database = database
        self._user = user
        self._password = password
        # Connection socket
        self._socket = None
        # Other connection data
        self._db_version = ''
        self._locale = 'en-US'
        self._auto_commit = True
        self._isolation_level = CUBRID_ISOLATION_LEVEL.TRAN_REP_CLASS_UNCOMMIT_INSTANCE
        # Connection state
        self._connection_opened = False
        # Error information
        self.error_code = 0
        self.error_msg = ''

        return

    def _get_self(self):
        """
        Returns self for weakref.proxy.
        http://docs.python.org/2/library/weakref.html
        """
        return self

    @property
    def CAS_INFO(self):
        """
        CAS information
        """
        return self._CAS_INFO

    @property
    def BROKER_INFO(self):
        """
        Broker information
        """
        return self._BROKER_INFO

    @property
    def db_version(self):
        """
        Database engine version
        """
        return self._db_version

    @property
    def locale(self):
        """
        Driver localization language
        """
        return self._locale

    @property
    def auto_commit(self):
        """
        Auto-commit mode
        Returns True or False.
        """
        return self._auto_commit

    def set_locale(self, locale):
        """
        Set driver localization language, to use localized error messages.
        Returns True or False.
        """
        if len(locale) >= 5:
            self._locale = locale
            return True
        else:
            raise errors.Error(ERROR_ID.INCORRECT_LOCALE_VALUE,
                get_error_message('en-US', ERROR_ID.INCORRECT_LOCALE_VALUE))

    def connect(self):
        """
        Connects to database.
        Returns True or False.
        """
        if self._connection_opened:
            raise errors.Error(ERROR_ID.ERROR_CONNECTION_OPENED,
                get_error_message(self._locale, ERROR_ID.ERROR_CONNECTION_OPENED))

        # Open connection to broker
        try:
            self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self._socket.connect((self._broker_address, self._broker_port))
            self._socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        except Exception as ex:
            logging.error(str(ex))
            raise

        try:
            # Request connection port
            _GetBrokerPort = GetBrokerPort(self._socket, self._locale)
            _GetBrokerPort.SendRequest()
            _GetBrokerPort.GetResponse()

            self._broker_port = _GetBrokerPort.port
        except Exception as ex:
            logging.error(str(ex))
            raise

        # Open a new socket connection, using the new port, if needed
        if self._broker_port > 0:
            try:
                # re-connecting using the new port
                self._socket.close()
                self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self._socket.connect((self._broker_address, self._broker_port))
                self._socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            except Exception as ex:
                logging.error(str(ex))
                raise

        _Connect = None

        try:
            self._clear_error()
            # Connect to database
            _Connect = Connect(self._socket, self._database, self.user, self._password,
                self._locale)
            _Connect.SendRequest()
            _Connect.GetResponse()

            self._CAS_INFO = _Connect._CAS_INFO[:]
            self._BROKER_INFO = _Connect._BROKER_INFO[:]
            self._connection_opened = True
        except Exception as ex:
            logging.error(str(ex))
            if _Connect.error_code != 0:
                self._set_error(_Connect.error_code, _Connect.error_msg)
                raise Exception("%d: %s" % (self.error_code, self.error_msg))
            else:
                logging.error(str(ex))
                raise

        if _Connect.error_code != 0:
            return  False
        else:
            self.get_db_version()

            if self.error_code != 0:
                return  False
            else:
                # only versions above 9.x are supported
                if not self._db_version.startswith('9.') and not self._db_version.startswith('10.'):
                    return False
                else:
                    return True

    def get_db_version(self):
        """
        Retrieves the database version.
        Returns database version or None.
        """
        if not self._connection_opened:
            raise errors.Error(ERROR_ID.ERROR_CONNECTION_NOT_OPENED,
                get_error_message(self._locale, ERROR_ID.ERROR_CONNECTION_NOT_OPENED))

        if self._db_version != '':
            return self._db_version

        _GetDatabaseVersion = None
        try:
            self._clear_error()
            # Get database engine version
            _GetDatabaseVersion = GetDatabaseVersion(self._socket, self._CAS_INFO, self._locale)
            _GetDatabaseVersion.SendRequest()
            _GetDatabaseVersion.GetResponse()
            self._db_version = _GetDatabaseVersion.db_version
        except Exception as ex:
            logging.error(str(ex))
            if _GetDatabaseVersion.error_code != 0:
                self._set_error(_GetDatabaseVersion.error_code, _GetDatabaseVersion.error_msg)
                raise Exception("%d: %s" % (self.error_code, self.error_msg))
            else:
                logging.error(str(ex))
                raise

        return self._db_version

    def set_db_parameter(self, parameter_id, parameter_value):
        """
        Set DB (global) parameter.
        """
        if not self._connection_opened:
            raise errors.Error(ERROR_ID.ERROR_CONNECTION_NOT_OPENED,
                get_error_message(self._locale, ERROR_ID.ERROR_CONNECTION_NOT_OPENED))

        _SetDBParameter = None
        try:
            self._clear_error()
            # Get database engine version
            _SetDBParameter = SetDBParameter(self._socket, self._CAS_INFO, parameter_id, parameter_value,
                self._locale)
            _SetDBParameter.SendRequest()
            _SetDBParameter.GetResponse()
        except Exception as ex:
            logging.error(str(ex))
            if _SetDBParameter.error_code != 0:
                self._set_error(_SetDBParameter.error_code, _SetDBParameter.error_msg)
                raise Exception("%d: %s" % (self.error_code, self.error_msg))
            else:
                logging.error(str(ex))
                raise

        return

    def get_db_parameter(self, parameter_id):
        """
        Get DB (global) parameter.
        Returns parameter value.
        """
        if not self._connection_opened:
            raise errors.Error(ERROR_ID.ERROR_CONNECTION_NOT_OPENED,
                get_error_message(self._locale, ERROR_ID.ERROR_CONNECTION_NOT_OPENED))

        _GetDBParameter = None
        try:
            self._clear_error()
            # Get database engine version
            _GetDBParameter = GetDBParameter(self._socket, self._CAS_INFO, parameter_id, self._locale)
            _GetDBParameter.SendRequest()
            _GetDBParameter.GetResponse()
        except Exception as ex:
            logging.error(str(ex))
            if _GetDBParameter.error_code != 0:
                self._set_error(_GetDBParameter.error_code, _GetDBParameter.error_msg)
                raise Exception("%d: %s" % (self.error_code, self.error_msg))
            else:
                logging.error(str(ex))
                raise

        return _GetDBParameter.parameter_value

    def close(self):
        """
        Gets database version.
        Returns True or False.
        """
        if not self._connection_opened:
            raise errors.Error(ERROR_ID.ERROR_CONNECTION_NOT_OPENED,
                get_error_message(self._locale, ERROR_ID.ERROR_CONNECTION_NOT_OPENED))

        _CloseConnection = None
        try:
            self._clear_error()
            # Close connection
            _CloseConnection = CloseConnection(self._socket, self._CAS_INFO, self._locale)
            _CloseConnection.SendRequest()
            _CloseConnection.GetResponse()

            # Close socket
            self._socket.shutdown(socket.SHUT_RDWR)
            self._socket.close()
            self._connection_opened = False
        except Exception as ex:
            logging.error(str(ex))
            if _CloseConnection.error_code != 0:
                self._set_error(_CloseConnection.error_code, _CloseConnection.error_msg)
                raise Exception("%d: %s" % (self.error_code, self.error_msg))
            else:
                logging.error(str(ex))
                raise

        return _CloseConnection.error_code == 0

    def commit(self):
        """
        Commit any pending transaction to the database.
        Returns True or False.
        """
        if not self._connection_opened:
            raise errors.Error(ERROR_ID.ERROR_CONNECTION_NOT_OPENED,
                get_error_message(self._locale, ERROR_ID.ERROR_CONNECTION_NOT_OPENED))

        _Commit = None

        try:
            self._clear_error()
            _Commit = Commit(self._socket, self._CAS_INFO, self._locale)
            _Commit.SendRequest()
            _Commit.GetResponse()
        except Exception as ex:
            logging.error(str(ex))
            if _Commit.error_code != 0:
                self._set_error(_Commit.error_code, _Commit.error_msg)
                raise Exception("%d: %s" % (self.error_code, self.error_msg))
            else:
                logging.error(str(ex))
                raise

        return _Commit.error_code == 0

    def rollback(self):
        """
        This method causes the database to roll back to the start of any pending transaction.
        Returns True or False.
        """
        if not self._connection_opened:
            raise errors.Error(ERROR_ID.ERROR_CONNECTION_NOT_OPENED,
                get_error_message(self._locale, ERROR_ID.ERROR_CONNECTION_NOT_OPENED))

        _Rollback = None

        try:
            self._clear_error()
            _Rollback = Rollback(self._socket, self._CAS_INFO, self._locale)
            _Rollback.SendRequest()
            _Rollback.GetResponse()
        except Exception as ex:
            logging.error(str(ex))
            if _Rollback.error_code != 0:
                self._set_error(_Rollback.error_code, _Rollback.error_msg)
                raise Exception("%d: %s" % (self.error_code, self.error_msg))
            else:
                logging.error(str(ex))
                raise

        return _Rollback.error_code == 0

    def cursor(self):
        """
        Return a new Cursor Object using this connection.
        :rtype : CUBRIDCursor
        """
        if not self._connection_opened:
            raise errors.Error(ERROR_ID.ERROR_CONNECTION_NOT_OPENED,
                get_error_message(self._locale, ERROR_ID.ERROR_CONNECTION_NOT_OPENED))

        return CUBRIDCursor(self)

    def set_autocommit(self, auto_commit_mode):
        """
        Toggle autocommit
        Returns True or False.
        """
        if not self._connection_opened:
            raise errors.Error(ERROR_ID.ERROR_CONNECTION_NOT_OPENED,
                get_error_message(self._locale, ERROR_ID.ERROR_CONNECTION_NOT_OPENED))

        _SetAutoCommitMode = None

        try:
            self._clear_error()
            _SetAutoCommitMode = SetAutoCommitMode(self._socket, self._CAS_INFO, auto_commit_mode,
                self._locale)
            _SetAutoCommitMode.SendRequest()
            _SetAutoCommitMode.GetResponse()
            self._auto_commit = auto_commit_mode
        except Exception as ex:
            logging.error(str(ex))
            if _SetAutoCommitMode.error_code != 0:
                self._set_error(_SetAutoCommitMode.error_code, _SetAutoCommitMode.error_msg)
                raise Exception("%d: %s" % (self.error_code, self.error_msg))
            else:
                logging.error(str(ex))
                raise

        return _SetAutoCommitMode.error_code == 0

    def schema_info(self, schema_type, name_pattern = ''):
        """
        Get DB schema information.
        Returns array of schema information.
        schema_type parameter must be one of these values:
            'tables', 'views', 'columns', 'exported keys', 'imported keys'
        """
        if not self._connection_opened:
            raise errors.Error(ERROR_ID.ERROR_CONNECTION_NOT_OPENED,
                get_error_message(self._locale, ERROR_ID.ERROR_CONNECTION_NOT_OPENED))

        if not schema_type.lower() in ('tables', 'views', 'columns', 'exported keys', 'imported keys', 'primary key'):
            raise errors.Error(ERROR_ID.ERROR_INVALID_SCHEMA_TYPE,
                get_error_message(self._locale, ERROR_ID.ERROR_INVALID_SCHEMA_TYPE))

        _GetDBSchema = None

        try:
            self._clear_error()
            _GetDBSchema = GetDBSchema(self._socket, self._CAS_INFO, schema_type, name_pattern, self._locale)
            _GetDBSchema.SendRequest()
            _GetDBSchema.GetResponse()

            _GetDBSchema.SendFetchRequest()
            _GetDBSchema.GetResponseFetch()
        except Exception as ex:
            logging.error(str(ex))
            if _GetDBSchema.error_code != 0:
                self._set_error(_GetDBSchema.error_code, _GetDBSchema.error_msg)
                raise Exception("%d: %s" % (self.error_code, self.error_msg))
            else:
                logging.error(str(ex))
                raise

        return _GetDBSchema.schema_info

    def set_isolation_level(self, isolation_level):
        assert type(isolation_level) is int
        return self.set_db_parameter(CCI_DB_PARAM.CCI_PARAM_ISOLATION_LEVEL, isolation_level)

    @property
    def is_connected(self):
        """
        Returns the CUBRID database connection state
        Returns True or False.
        """
        return self._connection_opened

    def _clear_error(self):
        """
        Clears the error information
        """
        self.error_code = 0
        self.error_msg = ''

    def _set_error(self, err_code, err_msg):
        """
        Initializes the error information
        """
        self.error_code = err_code
        self.error_msg = err_msg

    def __del__(self):
        """
        Class destructor.
        """
        #logging.debug('Class destructor called.')

        try:
            if self._connection_opened:
                self.close()
            self._socket.shutdown(socket.SHUT_RDWR)
            self._socket.close()
        except:
            # logging.debug('Connection socket is already closed!')
            pass

        return

    @property
    def database(self):
        """Database"""
        return self._database

    @property
    def user(self):
        """User"""
        return self._user

    @property
    def broker_address(self):
        """CUBRID Broker IP address or name"""
        return self._broker_address

    @property
    def broker_port(self):
        """CUBRID Broker TCP/IP port"""
        return self._broker_port

    @property
    def isolation_level(self):
        """CUBRID Isolation level"""
        return self._isolation_level

