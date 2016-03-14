# -*- coding: utf8 -*-


from ots2.error import *
from ots2.metadata import *
import ots2.protobuf.ots_protocol_2_pb2 as pb2

INT32_MAX = 2147483647
INT32_MIN = -2147483648


class OTSProtoBufferEncoder:

    def __init__(self, encoding):
        self.encoding = encoding

    def _make_column_value(self, proto, value):
        # you have to put 'int' under 'bool' in the switch case
        # because a bool is also a int !!!

        if isinstance(value, str) or isinstance(value, unicode):
            string = value
            proto.type = pb2.STRING
            proto.v_string = string
        elif isinstance(value, bool):
            proto.type = pb2.BOOLEAN
            proto.v_bool = value
        elif isinstance(value, int) or isinstance(value, long):
            proto.type = pb2.INTEGER
            proto.v_int = value
        elif isinstance(value, float):
            proto.type = pb2.DOUBLE
            proto.v_double = value
        elif isinstance(value, bytearray):
            proto.type = pb2.BINARY
            proto.v_binary = bytes(value)
        elif value is INF_MIN:
            proto.type = pb2.INF_MIN
        elif value is INF_MAX:
            proto.type = pb2.INF_MAX
        else:
            raise OTSClientError(
                "expect str, unicode, int, long, bool or float for colum value, not %s"
                % value.__class__.__name__
            )

    def _make_columns_with_dict(self, proto, column_dict):
        for name, value in column_dict.iteritems():
            item = proto.add()
            item.name = name
            self._make_column_value(item.value, value)

    def _make_put_row_item(self, proto, put_row_item):
        proto.condition.row_existence = pb2.IGNORE
        self._make_columns_with_dict(proto.primary_key, put_row_item.primary_key)
        print proto.primary_key[0].value.v_string
        self._make_columns_with_dict(proto.attribute_columns, put_row_item.attribute_columns)

    def _make_batch_write_row(self, proto, batch_list):
        for table_dict in batch_list:
            table_item = proto.tables.add()
            table_item.table_name = table_dict.get('table_name')

            for key,row_list in table_dict.iteritems():
                if key is 'table_name':
                    continue
                for row_item in row_list:
                    if key is 'put':
                        row = table_item.put_rows.add()
                        self._make_put_row_item(row, row_item)

    def _encode_batch_write_row(self, batch_list):
        proto = pb2.BatchWriteRowRequest()
        self._make_batch_write_row(proto, batch_list)
        return proto

    def encode_request(self, batch_list):
        return self._encode_batch_write_row(batch_list)
