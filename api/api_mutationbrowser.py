# -*- coding: utf-8 -*-
# Create Date 2016/1/30 0030
__author__ = 'wubo'

from ots2 import *

ENDPOINT = "http://samplechr.cn-beijing.ots.aliyuncs.com/"
ACCESSID = "QFmrMPB18qNx9KYc"
ACCESSKEY = "IuAdh4qL9noDf0UnMOO977HSgZSc0E"
INSTANCENAME = "samplechr"
TABLE_NAME = "sample_chr_info"

ots_client = OTSClient(ENDPOINT, ACCESSID, ACCESSKEY, INSTANCENAME)

# 获得表的信息
def DescribeTable(table_name):
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




if __name__ == "__main__":
    #DescribeTable(TABLE_NAME)
    GetRange(TABLE_NAME)