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
filepath = r"C:\Users\guhongjie\Desktop\ref\chr13"

# 10、插入、更新、删除一张表或多张表中的多行
def BatchWriteRow111():
    """
    应用程序可以通过batch_write_row接口插入、更新、删除一张表或多张表中的多行。
    TableStore会将各个子操作的执行结果分别返回给应用程序，可能存在部分请求成功、部分请求失败的现象。
    即使整个请求没有返回错误，应用程序也必须要检查每个子操作返回的结果，从而拿到正确的状态
    :return:
    """
    # 方法
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
        table_item = {'table_name':table_name, operation:[item]}
        batch_list.append(table_item)

    # 加入
    # 1
    primary_key = {'gid':'2', 'uid':205}
    attribute_columns = {'name':'李四', 'address':'中国某地', 'age':20}
    condition = Condition('EXPECT_NOT_EXIST')
    put_row_item1 = PutRowItem(condition, primary_key, attribute_columns)
    # 2
    primary_key = {'gid':'2', 'uid':206}
    attribute_columns = {'name':'李四', 'address':'中国某地', 'age':20}
    condition = Condition('EXPECT_NOT_EXIST')
    put_row_item2 = PutRowItem(condition, primary_key, attribute_columns)
    # 3
    primary_key = {'gid':'2', 'uid':207}
    attribute_columns = {'name':'李四', 'address':'中国某地', 'age':20}
    condition = Condition('EXPECT_NOT_EXIST')
    put_row_item3 = PutRowItem(condition, primary_key, attribute_columns)

    # 删除
    # 1
    primary_key = {'gid':'2', 'uid':202}
    condition = Condition('IGNORE')
    delete_row_item1 = DeleteRowItem(condition, primary_key)

    # 2
    primary_key = {'gid':'2', 'uid':203}
    condition = Condition('IGNORE')
    delete_row_item2 = DeleteRowItem(condition, primary_key)

    # 更新
    # 1
    primary_key = {'gid':'2', 'uid':205}
    condition = Condition('IGNORE')
    update_of_attribute_columns = {
        'put' : {'name':'李四', 'address':'北京'},
        'delete' : ['age'],
    }
    update_row_item1 = UpdateRowItem(condition, primary_key, update_of_attribute_columns)

    # 2
    primary_key = {'gid':'2', 'uid':206}
    condition = Condition('IGNORE')
    update_of_attribute_columns = {
        'put' : {'name':'李四', 'address':'紫禁城'},
        'delete' : ['age'],
    }
    update_row_item2 = UpdateRowItem(condition, primary_key, update_of_attribute_columns)

    table_item1  = {'table_name':'geneac', 'put':[put_row_item1,put_row_item2,put_row_item3], 'update':[], 'delete':[]}
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
                    print u'操作是否成功：%s' % row_item.is_ok
                    if not row_item.is_ok:
                        print u'错误码：%s' % row_item.error_code
                        print u'错误信息：%s' % row_item.error_message
                        add_batch_write_item(failed_batch_list, batch_list[i]['table_name'], operation, batch_list[i][operation][j])
                    else:
                        print u'本次操作消耗的写CapacityUnit为：%s' % row_item.consumed.write

        if not failed_batch_list:
            break
        retry_count += 1
        batch_list = failed_batch_list
        batch_write_response = ots_client.batch_write_row(batch_list)
        print batch_write_response

# BatchWriteRow111()


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


def insert_ref():
    begin_time = time.time()
    with open(filepath) as file:
        print file.readline()
        i = 0
        line_list = []
        ref_p = 0
        chr_no = 13
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
    insert_ref()
    

