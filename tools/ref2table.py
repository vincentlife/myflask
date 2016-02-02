# -*- coding: utf-8 -*-
# !/usr/bin/python
# Create Date 2016/2/1 0001
__author__ = 'wubo'
from ots2 import *
import time

ENDPOINT = "http://samplechr.cn-beijing.ots.aliyuncs.com/"
ACCESSID = "QFmrMPB18qNx9KYc"
ACCESSKEY = "IuAdh4qL9noDf0UnMOO977HSgZSc0E"
INSTANCENAME = "samplechr"
TABLE_NAME = "sample_chr_info"
REF_TABLE = "ref_seq"

ots_client = OTSClient(ENDPOINT, ACCESSID, ACCESSKEY, INSTANCENAME)


def insert2table(item_list):
    def add_batch_write_item(batch_list, table_name, operation, item):
        for table_item in batch_list:
            if table_item.get('table_name') == table_name:
                operation_item = table_item.get(operation)
                if not operation_item:
                    table_item[operation] = [item]
                else:
                    operation_item.append(item)
                return
        # not found
        table_item = {'table_name':REF_TABLE, operation:[item]}
        batch_list.append(table_item)
    table_item1 = {'table_name': REF_TABLE, 'put': item_list, 'update':[], 'delete':[]}
    batch_list = [table_item1]
    batch_write_response = ots_client.batch_write_row(batch_list)

    # 每一行操作都是独立的，需要分别判断是否成功。对于失败子操作进行重试。
    retry_count = 0
    operation_list = ['put', 'update', 'delete']
    while retry_count < 3:
        failed_batch_list = []
        for i in range(len(batch_write_response)):
            table_item = batch_write_response[i]
            for operation in operation_list:
                operation_item = table_item.get(operation)
                if not operation_item:
                    continue
                print u'操作：%s' % operation
                for j in range(len(operation_item)):
                    row_item = operation_item[j]
                    # print u'操作是否成功：%s' % row_item.is_ok
                    if not row_item.is_ok:
                        print "error %s" % row_item.error_code
                        # print u'错误码：%s' % row_item.error_code
                        # print u'错误信息：%s' % row_item.error_message
                        add_batch_write_item(failed_batch_list, batch_list[i]['table_name'], operation, batch_list[i][operation][j])
                    else:
                        print "success CapacityUnit: %s" % row_item.consumed.write
                        # print u'本次操作消耗的写CapacityUnit为：%s' % row_item.consumed.write

        if not failed_batch_list:
            break
        retry_count += 1
        batch_list = failed_batch_list
        batch_write_response = ots_client.batch_write_row(batch_list)
        print batch_write_response


def insert_ref(filepath, chr_no):
    begin_time = time.time()
    with open(filepath) as file:
        print file.readline()
        i = 0
        line_list = []
        ref_p = 0
        item_list = []
        for line in file:
            # 每条包含3000个字符
            if i < 59:
                line_list.append(line.strip("\n"))
                i += 1
            else:
                line_list.append(line.strip("\n"))
                s = "".join(line_list)
                line_list = []
                condition = Condition('EXPECT_NOT_EXIST')

                put_row_item = PutRowItem(condition, {"chr_no": str(chr_no), "index_no": str(ref_p)},
                                          {"start": ref_p*3000, "seq": s})
                item_list.append(put_row_item)
                if len(item_list) > 159:
                    insert2table(item_list)
                    item_list = []
                ref_p += 1
                i = 0

    if i != 0:
        s = "".join(line_list)
        condition = Condition('EXPECT_NOT_EXIST')
        put_row_item = PutRowItem(condition, {"chr_no": str(chr_no), "index_no": str(ref_p)},
                                          {"start": ref_p, "seq": s})
        item_list.append(put_row_item)
    if len(item_list) != 0:
        insert2table(item_list)
    total_time = time.time() - begin_time
    print "------------------------total time is------------------------------"
    print total_time/60


if __name__ == "__main__":
    file = r"C:\Users\guhongjie\Desktop\ref\chr13"
    chr_no = 13
    insert_ref(file, chr_no)
    

