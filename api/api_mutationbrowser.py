# -*- coding: utf-8 -*-
# Create Date 2016/1/30 0030
__author__ = 'wubo'

from ots2 import *
from operator import itemgetter

ENDPOINT = "http://samplechr.cn-beijing.ots.aliyuncs.com/"
ACCESSID = "QFmrMPB18qNx9KYc"
ACCESSKEY = "IuAdh4qL9noDf0UnMOO977HSgZSc0E"
INSTANCENAME = "samplechr"
TABLE_NAME = "sample_chr_info"

ots_client = OTSClient(ENDPOINT, ACCESSID, ACCESSKEY, INSTANCENAME)

binOffsets = [512+64+8+1, 64+8+1, 8+1, 1, 0]
binFirstShift = 17
binNextShift = 3
# file = 'test_reads1.txt'
fwp = open('results.txt', 'w')

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

def GetBinReads(table_name,sample_no_chr,bin_no):
    """
    将指定主键范围内的数据返回给应用程序。
    :return:
    """
    # 查询区间：[(1, INF_MIN), (4, INF_MAX))，左闭右开。
    #print sample_no_chr
    inclusive_start_primary_key = {'sample_no_chr': int(sample_no_chr), 'bin_no': bin_no, 'qname': INF_MIN, 'flag': INF_MIN}
    exclusive_end_primary_key = {'sample_no_chr': int(sample_no_chr), 'bin_no': bin_no, 'qname': INF_MAX, 'flag': INF_MAX}
    columns_to_get = ['bin_no', 'attribute_qname', 'start', 'end', 'cigar', 'seq', 'row_no','strand','attribute_flag']
    consumed, next_start_primary_key, row_list = ots_client.get_range(
                table_name, 'FORWARD',
                inclusive_start_primary_key, exclusive_end_primary_key,
                columns_to_get, 1000
    )
    return row_list

# 根据range生成binlist
def one_bin(start, end):
    startBin = start - 1
    endBin = end - 1
    startBin >>= binFirstShift
    endBin >>= binFirstShift
    bin_list = []
    for i in range(len(binOffsets)):
        bin_list.extend([binOffsets[i] + startBin, binOffsets[i] + endBin])
        startBin >>= binNextShift
        endBin >>= binNextShift
    bin_set = set(bin_list)
    bin_list = list(bin_set)
    return bin_list


def query_reads(sample_no, chr, start, end):
    bin_list = one_bin(start, end)
    reads_list = []
    sample_no_chr =str(sample_no) + str(chr).zfill(2)
    for bin_no in bin_list:
        row_list = GetBinReads(TABLE_NAME, sample_no_chr, bin_no)
        for row in row_list:
            attribute_columns = row[1]
            rstart = attribute_columns.get('start')
            rend = attribute_columns.get('end')
            qname = attribute_columns.get('attribute_qname')
            mate = attribute_columns.get('seq')
            cigar = attribute_columns.get('cigar')
            strand = attribute_columns.get('strand')
            if int(rstart) <= int(end) and int(rend) >= int(start):
                reads_list.append([qname, mate, cigar, strand, rstart, rend])
    return reads_list


def GetLevel(sample_no, chr, start, end):
    reads_list = query_reads(sample_no, chr, start, end)
    sort_reads = sorted(reads_list, key=itemgetter(4, 5))#start,end整形
    level_list = []
    level_list.append([sort_reads[0]])
    length = len(sort_reads)
    for i in range(1, length):
        for j in range(len(level_list)):
            if level_list[j][-1][-1] < sort_reads[i][-2]:
                level_list[j].append(sort_reads[i])
                break
            elif j+1 == len(level_list):
                level_list.append([sort_reads[i]])
                break
    #total = time.time() - begin
    #print level_dict
    print level_list
    for level in level_list:
        for item in level:
            #print item
            fwp.write(item[-2])
            fwp.write(item[1])
            fwp.write(item[-1])
            fwp.write('\t')
        fwp.write('\n')
    fwp.close()
    # for i in range(len(level_dict[key])):
    #     length = int(level_dict[key][i][-1]) - int(level_dict[key][i][-2]) - 1
    #     fwp.write(str(level_dict[key][i][1]))
    #     for j in range(length):
    #         fwp.write('*')
    #     fwp.write(str(level_dict[key][i][2]))
    #     if i<len(level_dict[key])-1:
    #         length2 = int(level_dict[key][i+1][1]) - int(level_dict[key][i][2]) + 1
    #         for k in range(length2):
    #             fwp.write('_')
    # fwp.write('\n')
    return level_list


if __name__ == '__main__':
    sample_no = 7
    chr = 13
    start = 19254500
    end =   19255700
    level_dict = GetLevel(sample_no, chr, start, end)
    #list = query_reads(sample_no,chr,start,end)
    # print "-------------------------------"
    # print ("len(list)",len(list))