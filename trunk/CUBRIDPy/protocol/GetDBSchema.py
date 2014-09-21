from collections import namedtuple
from CUBRIDPy.constants.DATA_TYPES import *
from CUBRIDPy.constants.CUBRID import *
from CUBRIDPy.protocol.CUBRIDProtocol import CUBRIDProtocol
import logging
from CUBRIDPy.protocol.ColumnMetaData import ColumnMetaData

class GetDBSchema(CUBRIDProtocol):
    """
    GetDBSchema class implementation
    """

    def __init__(self, sock, CAS_INFO, schema_type, name_pattern, locale = 'en-US'):
        """
        Class constructor
        :param socket sock: Connection socket.
        :param array CAS_INFO: CAS data.
        :param locale: localization information
        :return:
        """
        logging.debug('Initializing GetDBSchema...')
        self._schema_type = schema_type
        self._name_pattern = name_pattern
        self._server_handler = None
        self._result_tuple = None
        self._result_tuple = None

        super(GetDBSchema, self).__init__(sock, CAS_INFO, locale)

        return

    def SendRequest(self):
        """
        Send request to the database engine
        """
        logging.debug('Sending GetDBSchema request...')

        # specific data to be sent (array of: [part_length, part])
        schema_type_value = self._encode_schema_type(self._schema_type)
        data = [
            # command code
            [DATA_TYPES.BYTE_SIZEOF, CAS_FUNCTION_CODE.CAS_FC_SCHEMA_INFO],
            # data length
            [DATA_TYPES.INT_SIZEOF, DATA_TYPES.INT_SIZEOF],
            # data
            [DATA_TYPES.INT_SIZEOF, schema_type_value]
        ]
        if self._name_pattern != '':
            data += [
                [DATA_TYPES.INT_SIZEOF, len(self._name_pattern) + 1],
                [DATA_TYPES.VARIABLE_STRING_SIZEOF, self._name_pattern],
                [DATA_TYPES.BYTE_SIZEOF, 0],
            ]
        else:
            data += [[DATA_TYPES.INT_SIZEOF, 0]]
        data += [[DATA_TYPES.INT_SIZEOF, 0]]
        pattern_match_value = CCI_SCHEMA_PATTERN_MATCH_FLAG.CCI_ATTR_NAME_PATTERN_MATCH | CCI_SCHEMA_PATTERN_MATCH_FLAG.CCI_CLASS_NAME_PATTERN_MATCH
        data += [
            # data length
            [DATA_TYPES.INT_SIZEOF, DATA_TYPES.BYTE_SIZEOF],
            # data
            [DATA_TYPES.BYTE_SIZEOF, pattern_match_value]
        ]

        packet_length = sum(row[0] for row in data)
        if self._name_pattern != '':
            packet_length += 1 # revert VARIABLE_STRING_SIZEOF value
            packet_length += len(self._name_pattern)# add SQL length

        super(GetDBSchema, self).PrepareRequestData(data, packet_length)
        super(GetDBSchema, self).SendRequest()

        return

    def SendFetchRequest(self):
        """
        Send request to the database engine
        """
        logging.debug('Sending SendFetchRequest request...')

        # specific data to be sent (array of: [part_length, part])
        data = [
            # command code
            [DATA_TYPES.BYTE_SIZEOF, CAS_FUNCTION_CODE.CAS_FC_FETCH],
            # data length
            [DATA_TYPES.INT_SIZEOF, DATA_TYPES.INT_SIZEOF],
            # data
            [DATA_TYPES.INT_SIZEOF, self.response_code],
            [DATA_TYPES.INT_SIZEOF, DATA_TYPES.INT_SIZEOF],
            [DATA_TYPES.INT_SIZEOF, 1],
            [DATA_TYPES.INT_SIZEOF, DATA_TYPES.INT_SIZEOF],
            [DATA_TYPES.INT_SIZEOF, self._result_tuple],
            [DATA_TYPES.INT_SIZEOF, DATA_TYPES.BYTE_SIZEOF],
            [DATA_TYPES.BYTE_SIZEOF, 0], # is case sensitive
            [DATA_TYPES.INT_SIZEOF, DATA_TYPES.INT_SIZEOF],
            [DATA_TYPES.INT_SIZEOF, 0], # is the ResultSet index...?
        ]

        super(GetDBSchema, self).PrepareRequestData(data)
        super(GetDBSchema, self).SendRequest()

        return

    def ProcessResponse(self):
        """
        Process response
        """
        logging.debug('GetDBSchema data received successfully.')

        self._result_tuple = self._response_buffer.ReadInt()
        num_col_info = self._response_buffer.ReadInt()
        self.info_array = []
        for i in range(0, num_col_info):
            info = ColumnMetaData()
            info.ColumnType = self._response_buffer.ReadByte()
            info.scale = self._response_buffer.ReadShort()
            info.precision = self._response_buffer.ReadInt()
            len = self._response_buffer.ReadInt()
            info.Name = self._response_buffer.ReadNullTerminatedString(len)
            self.info_array.append(info)

        return


    #noinspection PyUnusedLocal
    def ProcessResponseFetch(self):
        """
        Process response
        """
        logging.debug('GetDBSchema fetch data received successfully.')

        self.tuple_count = self._response_buffer.ReadInt()
        self.schema_info = [None] * self.tuple_count
        for i in range(0, self.tuple_count):
            self._response_buffer.ReadInt() # index
            self._response_buffer.ReadOID() # OID
            if self._schema_type == 'tables':
                length = self._response_buffer.ReadInt()
                sch_class_name = self._response_buffer.ReadNullTerminatedString(length)
                length = self._response_buffer.ReadInt()
                sch_class_name_type = self._response_buffer.ReadShort()
                SchemaInfo = namedtuple('SchemaInfo', 'name type')
                self.schema_info[i] = SchemaInfo(sch_class_name, sch_class_name_type)
            elif self._schema_type == 'views':
                length = self._response_buffer.ReadInt()
                sch_vclass_name = self._response_buffer.ReadNullTerminatedString(length)
                length = self._response_buffer.ReadInt()
                sch_vclass_type = self._response_buffer.ReadShort()
                SchemaInfo = namedtuple('SchemaInfo', 'name type')
                self.schema_info[i] = SchemaInfo(sch_vclass_name, sch_vclass_type)
            elif self._schema_type == 'columns':
                length = self._response_buffer.ReadInt()
                sch_attribute_columnName = self._response_buffer.ReadNullTerminatedString(length)
                length = self._response_buffer.ReadInt()
                sch_attribute_domain = self._response_buffer.ReadShort()
                length = self._response_buffer.ReadInt()
                sch_attribute_scale = self._response_buffer.ReadShort()
                length = self._response_buffer.ReadInt()
                sch_attribute_precision = self._response_buffer.ReadInt()
                length = self._response_buffer.ReadInt()
                sch_attribute_indexed = self._response_buffer.ReadShort()
                length = self._response_buffer.ReadInt()
                sch_attribute_non_null = (self._response_buffer.ReadShort() == 1)
                length = self._response_buffer.ReadInt()
                sch_attribute_shared = self._response_buffer.ReadShort()
                length = self._response_buffer.ReadInt()
                sch_attribute_unique = (self._response_buffer.ReadShort() == 1)
                length = self._response_buffer.ReadInt()
                sch_attribute_default = ''
                if length > 0:
                    sch_attribute_default = self._response_buffer.ReadNullTerminatedString(length)
                length = self._response_buffer.ReadInt()
                sch_attribute_attr_order = self._response_buffer.ReadInt()
                length = self._response_buffer.ReadInt()
                sch_attribute_class_name = self._response_buffer.ReadNullTerminatedString(length)
                length = self._response_buffer.ReadInt()
                sch_attribute_source_class = self._response_buffer.ReadNullTerminatedString(length)
                length = self._response_buffer.ReadInt()
                sch_attribute_is_key = (self._response_buffer.ReadShort() == 1)
                SchemaInfo = namedtuple('SchemaInfo',
                    'attr_name domain scale precision indexed not_null shared unique default attr_order table_name source_class is_key')
                self.schema_info[i] = SchemaInfo(
                    sch_attribute_columnName,
                    sch_attribute_domain,
                    sch_attribute_scale,
                    sch_attribute_precision,
                    sch_attribute_indexed,
                    sch_attribute_non_null,
                    sch_attribute_shared,
                    sch_attribute_unique,
                    sch_attribute_default,
                    sch_attribute_attr_order,
                    sch_attribute_class_name,
                    sch_attribute_source_class,
                    sch_attribute_is_key
                )
            elif self._schema_type == 'exported keys':
                length = self._response_buffer.ReadInt()
                sch_exported_keys_pkTableName = self._response_buffer.ReadNullTerminatedString(length)
                length = self._response_buffer.ReadInt()
                sch_exported_keys_pkColumnName = self._response_buffer.ReadNullTerminatedString(length)
                length = self._response_buffer.ReadInt()
                sch_exported_keys_fkTableName = self._response_buffer.ReadNullTerminatedString(length)
                length = self._response_buffer.ReadInt()
                sch_exported_keys_fkColumnName = self._response_buffer.ReadNullTerminatedString(length)
                length = self._response_buffer.ReadInt()
                sch_exported_keys_keySeq = self._response_buffer.ReadShort()
                length = self._response_buffer.ReadInt()
                sch_exported_keys_updateAction = self._response_buffer.ReadShort()
                length = self._response_buffer.ReadInt()
                sch_exported_keys_deleteAction = self._response_buffer.ReadShort()
                length = self._response_buffer.ReadInt()
                sch_exported_keys_fkName = self._response_buffer.ReadNullTerminatedString(length)
                length = self._response_buffer.ReadInt()
                sch_exported_keys_pkName = self._response_buffer.ReadNullTerminatedString(length)
                SchemaInfo = namedtuple('SchemaInfo',
                    'pktable_name pkcolumn_name fktable_name fkcolumn_name key_seq update_action delete_action fk_name pk_name')
                self.schema_info[i] = SchemaInfo(
                    sch_exported_keys_fkTableName,
                    sch_exported_keys_fkColumnName,
                    sch_exported_keys_pkTableName,
                    sch_exported_keys_pkColumnName,
                    sch_exported_keys_keySeq,
                    sch_exported_keys_updateAction,
                    sch_exported_keys_deleteAction,
                    sch_exported_keys_fkName,
                    sch_exported_keys_pkName
                )
            elif self._schema_type == 'imported keys':
                length = self._response_buffer.ReadInt()
                sch_imported_keys_pkTableName = self._response_buffer.ReadNullTerminatedString(length)
                length = self._response_buffer.ReadInt()
                sch_imported_keys_pkColumnName = self._response_buffer.ReadNullTerminatedString(length)
                length = self._response_buffer.ReadInt()
                sch_imported_keys_fkTableName = self._response_buffer.ReadNullTerminatedString(length)
                length = self._response_buffer.ReadInt()
                sch_imported_keys_fkColumnName = self._response_buffer.ReadNullTerminatedString(length)
                length = self._response_buffer.ReadInt()
                sch_imported_keys_keySeq = self._response_buffer.ReadShort()
                length = self._response_buffer.ReadInt()
                sch_imported_keys_updateAction = self._response_buffer.ReadShort()
                length = self._response_buffer.ReadInt()
                sch_imported_keys_deleteAction = self._response_buffer.ReadShort()
                length = self._response_buffer.ReadInt()
                sch_imported_keys_fkName = self._response_buffer.ReadNullTerminatedString(length)
                length = self._response_buffer.ReadInt()
                sch_imported_keys_pkName = self._response_buffer.ReadNullTerminatedString(length)
                SchemaInfo = namedtuple('SchemaInfo',
                    'pktable_name pkcolumn_name fktable_name fkcolumn_name key_seq update_action delete_action fk_name pk_name')
                self.schema_info[i] = SchemaInfo(
                    sch_imported_keys_fkTableName,
                    sch_imported_keys_fkColumnName,
                    sch_imported_keys_pkTableName,
                    sch_imported_keys_pkColumnName,
                    sch_imported_keys_keySeq,
                    sch_imported_keys_updateAction,
                    sch_imported_keys_deleteAction,
                    sch_imported_keys_fkName,
                    sch_imported_keys_pkName
                )
            elif self._schema_type == 'primary key':
                length = self._response_buffer.ReadInt()
                sch_primary_key_className = self._response_buffer.ReadNullTerminatedString(length)
                length = self._response_buffer.ReadInt()
                sch_primary_key_columnName = self._response_buffer.ReadNullTerminatedString(length)
                length = self._response_buffer.ReadInt()
                sch_primary_key_keySeq = self._response_buffer.ReadInt()
                length = self._response_buffer.ReadInt()
                sch_primary_key_keyName = self._response_buffer.ReadNullTerminatedString(length)
                SchemaInfo = namedtuple('SchemaInfo',
                    'table_name attr_name key_seq key_name')
                self.schema_info[i] = SchemaInfo(
                    sch_primary_key_className,
                    sch_primary_key_columnName,
                    sch_primary_key_keySeq,
                    sch_primary_key_keyName
                )

        return


    def GetResponse(self):
        """
        Get response from the database engine
        """
        logging.debug('Reading GetDBSchema response...')
        super(GetDBSchema, self).GetResponse()

        return


    def GetResponseFetch(self):
        """
        Get response from the database engine
        """
        logging.debug('Reading GetDBSchema fetch response...')
        super(GetDBSchema, self).GetResponseFetch()

        return


    def _encode_schema_type(self, schema_type):
        return {
                   'tables': CUBRID_SCHEMA_TYPE.CCI_SCH_CLASS,
                   'views': CUBRID_SCHEMA_TYPE.CCI_SCH_VCLASS,
                   'columns': CUBRID_SCHEMA_TYPE.CCI_SCH_ATTRIBUTE,
                   'primary key': CUBRID_SCHEMA_TYPE.CCI_SCH_PRIMARY_KEY,
                   'exported keys': CUBRID_SCHEMA_TYPE.CCI_SCH_EXPORTED_KEYS,
                   'imported keys': CUBRID_SCHEMA_TYPE.CCI_SCH_IMPORTED_KEYS
               }[schema_type]

