# -*- coding: utf-8 -*-
# Create Date 2016/1/30 0030
__author__ = 'wubo'

from ots2 import *
from operator import itemgetter
import json
from api import app
from flask import request,Blueprint

ENDPOINT = "http://samplechr.cn-beijing.ots.aliyuncs.com/"
ACCESSID = "QFmrMPB18qNx9KYc"
ACCESSKEY = "IuAdh4qL9noDf0UnMOO977HSgZSc0E"
INSTANCENAME = "samplechr"
TABLE_NAME = "sample_chr_info"
REF_TABLE = "ref_seq"

ots_client = OTSClient(ENDPOINT, ACCESSID, ACCESSKEY, INSTANCENAME)

binOffsets = [512+64+8+1, 64+8+1, 8+1, 1, 0]
binFirstShift = 17
binNextShift = 3

pgb_api = Blueprint("pgb_api",__name__)


def init_bin(start, end):
    '''
    根据range生成binlist
    :param start: int型
    :param end:  int型
    :return: bin_list
    '''
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


def GetBinReads(sample_no_chr, bin_no):
    '''
    将指定主键范围内的数据返回给应用程序。
    :param sample_no_chr:
    :param bin_no:
    :return:
    '''
    # 查询区间：[(1, INF_MIN), (4, INF_MAX))，左闭右开。
    #print sample_no_chr
    result_list = []
    inclusive_start_primary_key = {'sample_no_chr': int(sample_no_chr), 'bin_no': bin_no, 'qname': INF_MIN, 'flag': INF_MIN}
    exclusive_end_primary_key = {'sample_no_chr': int(sample_no_chr), 'bin_no': bin_no, 'qname': INF_MAX, 'flag': INF_MAX}
    columns_to_get = ['bin_no', 'attribute_qname', 'start', 'end', 'cigar', 'seq', 'row_no','strand','attribute_flag']
    consumed, next_start_primary_key, row_list = ots_client.get_range(
                TABLE_NAME, 'FORWARD',
                inclusive_start_primary_key, exclusive_end_primary_key,
                columns_to_get, 1000
    )
    result_list.extend(row_list)
    while next_start_primary_key:
        consumed, next_start_primary_key, row_list = ots_client.get_range(
                TABLE_NAME, 'FORWARD',
                next_start_primary_key, exclusive_end_primary_key,
                columns_to_get, 1000
         )
        result_list.extend(row_list)
    return result_list

def read2dic(item):
    read_dic = {}
    read_dic["QNAME"] = item[0]
    read_dic["CIGAR"] = item[2]
    read_dic["strand"] = item[3]
    read_dic["start"] = item[4]
    read_dic["end"] = item[5]
    return read_dic


def query_reads(sample_no, chr, start, end):
    bin_list = init_bin(start, end)
    reads_list = []
    sample_no_chr =str(sample_no) + str(chr).zfill(2)
    # 获取range包含的所有bin中的reads
    for bin_no in bin_list:
        row_list = GetBinReads(sample_no_chr, bin_no)
        for row in row_list:
            attribute_columns = row[1]
            rstart = int(attribute_columns.get('start'))
            rend = int(attribute_columns.get('end'))
            qname = attribute_columns.get('attribute_qname')
            mate = attribute_columns.get('seq')
            cigar = attribute_columns.get('cigar')
            strand = attribute_columns.get('strand')
            print rstart,rend
            if int(rstart) <= int(end) and int(rend) >= int(start):
                reads_list.append([qname, mate, cigar, strand, rstart, rend])

    # 对reads_list 进行排序并得出level
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
    # 整理格式
    result_list = []
    for level in level_list:
        lev_list = []
        for item in level:
            lev_list.append(read2dic(item))
        result_list.append(lev_list)
    return result_list


def query_ref(chr, start, end):
    '''
    根据范围返回参考序列
    :param sample_no:
    :param chr:
    :param start:
    :param end:
    :return:
    '''
    s_index_no = start/3000
    e_index_no = end/3000
    inclusive_start_primary_key = {'chr_no': str(chr), 'index_no': str(s_index_no)}
    exclusive_end_primary_key = {'chr_no': str(chr), 'index_no': str(e_index_no+1)}
    columns_to_get = ['seq']
    consumed, next_start_primary_key, row_list = ots_client.get_range(
            REF_TABLE, 'FORWARD',
            inclusive_start_primary_key, exclusive_end_primary_key,
            columns_to_get
    )
    result_list = []
    for row in row_list:
        attribute_columns = row[1]
        seq = attribute_columns.get('seq')
        result_list.append(seq)
    start_point = s_index_no*3000
    return "".join(result_list)[start-start_point:end-start_point].upper()


@pgb_api.route("/pgb/", methods=['GET'])
def get_pgb():
    req = json.loads(request.data)
    start,end = req['range']
    sample_no = req["sample_no"]
    chr = req["chr"]
    reads_list = query_reads(sample_no, chr, start, end)
    ref_seq = query_ref(chr, start, end)
    return json.dumps({"ref":{"rname":chr,"seq":ref_seq},"reads":reads_list})

if __name__ == '__main__':
    sample_no = 3908
    chr = 1
    start = 66040
    end = 66300
    reads_list = query_reads(sample_no, chr, start, end)
    ref_seq = query_ref(1, start, end)
    print reads_list
    print ref_seq