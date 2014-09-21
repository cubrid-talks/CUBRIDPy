from ...localization import ERROR_ID as ERR_ID

class ERRORS:
    """
    Error messages
    """
    #Driver custom error messages
    DRIVER_ERRORS = {
        ERR_ID.ERROR_ID.NO_ERROR: 'No error',
        ERR_ID.ERROR_ID.ERROR_NOT_IMPLEMENTED: 'Not implemented yet!',
        ERR_ID.ERROR_ID.ERROR_NOT_SUPPORTED: 'Not supported!',
        ERR_ID.ERROR_ID.INCORRECT_LOCALE_VALUE: 'Incorrect localization value',
        ERR_ID.ERROR_ID.ERROR_RECEIVING_DATA: 'Error receiving socket data!',
        ERR_ID.ERROR_ID.ERROR_SENDING_DATA: 'Error sending socket data!',
        ERR_ID.ERROR_ID.ERROR_CURSOR_ALREADY_CLOSED: 'Cursor is already closed!',
        ERR_ID.ERROR_ID.ERROR_NO_ACTIVE_QUERY: 'No active query!',
        ERR_ID.ERROR_ID.ERROR_CONNECTION_OPENED: 'Connection is already opened - please close it first!',
        ERR_ID.ERROR_ID.ERROR_CONNECTION_NOT_OPENED: 'Connection is not opened!',
        ERR_ID.ERROR_ID.ERROR_INVALID_SCHEMA_TYPE: 'Invalid schema type!',
        ERR_ID.ERROR_ID.ERROR_INVALID_LOB_TYPE: 'Invalid LOB type!',
        }

    #CAS error messages
    CAS_ERRORS = {
        -1000: 'CAS_ER_DBMS',
        -1001: 'CAS_ER_INTERNAL',
        -1002: 'CAS_ER_NO_MORE_MEMORY',
        -1003: 'CAS_ER_COMMUNICATION',
        -1004: 'CAS_ER_ARGS',
        -1005: 'CAS_ER_TRAN_TYPE',
        -1006: 'CAS_ER_SRV_HANDLE',
        -1007: 'CAS_ER_NUM_BIND',
        -1008: 'CAS_ER_UNKNOWN_U_TYPE',
        -1009: 'CAS_ER_DB_VALUE',
        -1010: 'CAS_ER_TYPE_CONVERSION',
        -1011: 'CAS_ER_PARAM_NAME',
        -1012: 'CAS_ER_NO_MORE_DATA',
        -1013: 'CAS_ER_OBJECT',
        -1014: 'CAS_ER_OPEN_FILE',
        -1015: 'CAS_ER_SCHEMA_TYPE',
        -1016: 'CAS_ER_VERSION',
        -1017: 'CAS_ER_FREE_SERVER',
        -1018: 'CAS_ER_NOT_AUTHORIZED_CLIENT',
        -1019: 'CAS_ER_QUERY_CANCEL',
        -1020: 'CAS_ER_NOT_COLLECTION',
        -1021: 'CAS_ER_COLLECTION_DOMAIN',
        -1022: 'CAS_ER_NO_MORE_RESULT_SET',
        -1023: 'CAS_ER_INVALID_CALL_STMT',
        -1024: 'CAS_ER_STMT_POOLING',
        -1025: 'CAS_ER_DBSERVER_DISCONNECTED',
        -1026: 'CAS_ER_MAX_PREPARED_STMT_COUNT_EXCEEDED',
        -1027: 'CAS_ER_HOLDABLE_NOT_ALLOWED',
        -1100: 'CAS_ER_NOT_IMPLEMENTED',
        -1200: 'CAS_ER_IS'
    }

    #CUBRID error messages
    CUBRID_ERRORS = {
        -2001: 'CUBRID_ER_NO_MORE_MEMORY',
        -2002: 'CUBRID_ER_INVALID_SQL_TYPE',
        -2003: 'CUBRID_ER_CANNOT_GET_COLUMN_INFO',
        -2004: 'CUBRID_ER_INIT_ARRAY_FAIL',
        -2005: 'CUBRID_ER_UNKNOWN_TYPE',
        -2006: 'CUBRID_ER_INVALID_PARAM',
        -2007: 'CUBRID_ER_INVALID_ARRAY_TYPE',
        -2008: 'CUBRID_ER_NOT_SUPPORTED_TYPE',
        -2009: 'CUBRID_ER_OPEN_FILE',
        -2010: 'CUBRID_ER_CREATE_TEMP_FILE',
        -2012: 'CUBRID_ER_INVALID_CURSOR_POS',
        -2013: 'CUBRID_ER_SQL_UNPREPARE',
        -2014: 'CUBRID_ER_PARAM_UNBIND',
        -2015: 'CUBRID_ER_SCHEMA_TYPE',
        -2016: 'CUBRID_ER_READ_FILE',
        -2017: 'CUBRID_ER_WRITE_FILE',
        -2018: 'CUBRID_ER_LOB_NOT_EXIST'
    }

