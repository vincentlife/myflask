# -*- coding: utf-8 -*-
# !/usr/bin/python
# Create Date 2016/2/2 0002
__author__ = 'wubo'
from ots2 import *
import re

ENDPOINT = "http://samplechr.cn-beijing.ots.aliyuncs.com/"
ACCESSID = "QFmrMPB18qNx9KYc"
ACCESSKEY = "IuAdh4qL9noDf0UnMOO977HSgZSc0E"
INSTANCENAME = "samplechr"
TABLE_NAME = "sample_chr_info"

ots_client = OTSClient(ENDPOINT, ACCESSID, ACCESSKEY, INSTANCENAME)

def DescribeTable(table_name):
    '''
    获得表的信息
    :param table_name:
    :return:
    '''
    describe_response = ots_client.describe_table(table_name)
    print u'表的名称: %s' % describe_response.table_meta.table_name
    print u'表的主键: %s' % describe_response.table_meta.schema_of_primary_key
    print u'表的预留读吞吐量：%s' % describe_response.reserved_throughput_details.capacity_unit.read
    print u'表的预留写吞吐量：%s' % describe_response.reserved_throughput_details.capacity_unit.write
    print u'最后一次上调预留读写吞吐量时间：%s' % describe_response.reserved_throughput_details.last_increase_time
    print u'最后一次下调预留读写吞吐量时间：%s' % describe_response.reserved_throughput_details.last_decrease_time
    print u'UTC自然日内总的下调预留读写吞吐量次数：%s' % describe_response.reserved_throughput_details.number_of_decreases_today


def GetRange(table_name):
    """
    将指定主键范围内的数据返回给应用程序。
    :return:
    """
    # 查询区间：[(1, INF_MIN), (4, INF_MAX))，左闭右开。
    inclusive_start_primary_key = {'sample_no_chr': 101, 'bin_no': 600, 'qname': INF_MIN, 'flag': INF_MIN}
    exclusive_end_primary_key = {'sample_no_chr': 101, 'bin_no': 600, 'qname': INF_MAX, 'flag': INF_MAX}
    columns_to_get = ['bin_no','rname','pos', 'mapq', 'cigar', 'rnext',
                                 'pnext', 'tlen', 'seq', 'qual','row_no']
    consumed, next_start_primary_key, row_list = ots_client.get_range(
                table_name, 'FORWARD',
                inclusive_start_primary_key, exclusive_end_primary_key,
                columns_to_get, 26
    )
    for row in row_list:
        attribute_columns = row[1]
        print attribute_columns
        print u'bin_no信息为：%s' % attribute_columns.get('bin_no')
        print u'rname信息为：%s' % attribute_columns.get('rname')
        print u'seq信息为：%s' % attribute_columns.get('seq')
    print u'本次操作消耗的读CapacityUnit为：%s' % consumed.read
    print u'下次开始的主键：%s' % next_start_primary_key
    print len(row_list)


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


if __name__ == "__main__":
    DescribeTable(TABLE_NAME)
    

