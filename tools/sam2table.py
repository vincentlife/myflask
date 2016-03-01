# -*- coding: utf-8 -*-
# Create Date 2016/1/30 0030
__author__ = 'wubo'
from ots2 import *
import re

ENDPOINT = "http://samplechr.cn-beijing.ots.aliyuncs.com/"
ACCESSID = "QFmrMPB18qNx9KYc"
ACCESSKEY = "IuAdh4qL9noDf0UnMOO977HSgZSc0E"
INSTANCENAME = "samplechr"
TABLE_NAME = "sample_chr_info"
REF_TABLE = "ref_seq"

REF_UNIT_SIZE = 3000
ots_client = OTSClient(ENDPOINT, ACCESSID, ACCESSKEY, INSTANCENAME)


def query_ref(chr, start, end):
    '''
    根据范围返回参考序列
    :param sample_no:
    :param chr:
    :param start:
    :param end:
    :return:
    '''
    s_index_no = start/REF_UNIT_SIZE
    e_index_no = end/REF_UNIT_SIZE
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
    start_point = s_index_no*REF_UNIT_SIZE
    return "".join(result_list)[start-start_point:end-start_point].upper()


def alter_cigar(original_cigar, seq, ref):
    # 比对序列指针
    seq_p = 0
    # 参考序列指针
    ref_p = 0
    # 新的cigar
    new_cigar = ""
    # 将原cigar拆分
    unit_list = re.findall(r'[0-9]+[MIDNS]', original_cigar)
    for unit in unit_list:
        type_flag = unit[-1]
        offset = int(unit[0:-1])
        # 若为M则查看有无错配情况 错配记为X
        if type_flag == "M":
            t = 0
            flag = True
            x_seq = ""
            for i in range(0,offset):
                # 若当前位置正确 则判断上个位置是否正确
                if seq[seq_p+t] == ref[ref_p+t]:
                    if flag: t+=1
                    else:
                        new_cigar = new_cigar + str(t) + "X" + x_seq
                        x_seq = ""; t = 1; flag = False
                else:
                    if flag:
                        new_cigar = new_cigar + str(t) + "M"
                        seq_p += t; ref_p += t;
                        x_seq += seq[seq_p+t]
                    else: t+=1

        elif type_flag == "I":
            new_cigar = new_cigar + unit + seq[seq_p:seq_p+offset]
            seq_p += offset
        elif type_flag == "D":
            ref_p += offset


def insert2table(TableName, item_list):
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
        table_item = {'table_name': table_name, operation: [item]}
        batch_list.append(table_item)
    table_item1 = {'table_name': TableName, 'put': item_list, 'update':[], 'delete':[]}
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


def sam2table(filepath,sample_no,rname):
    chr_no_swich = {
        "X":'23',
        "Y":'24',
        "MT":'25',
        "*": '26'
    }
    ref_seq = ""
    with open(filepath, "r") as file:
        i = 0
        item_list = []
        for line in file:
            line_list = line.split('\t')
            row_no = i
            bin_no = line_list[0]
            qname = line_list[1]
            flag = line_list[2]
            rname = line_list[3]
            pos = line_list[4]
            mapq = line_list[5]
            cigar = line_list[6]
            rnext = line_list[7]
            pnext = line_list[8]
            tlen = line_list[9]
            seq = line_list[10]
            qual = line_list[11]
            chr = chr_no_swich.get(rname, str(rname).zfill(2))
            ref_seq
            sample_no_chr = int(str(sample_no)+chr)
            condition = Condition('EXPECT_NOT_EXIST')
            primary_key = {u"sample_no_chr": sample_no_chr, u"bin_no": bin_no, u'qname': qname, 'flag': flag}
            attribute_columns = {'rname': rname, 'pos': pos, 'mapq': mapq, 'cigar': cigar, 'rnext': rnext,
                         'pnext': pnext, 'tlen': tlen, 'seq': seq, 'qual': qual, 'row_no': row_no,
                         'attribute_qname': qname, 'attribute_flag': flag}
            put_row_item = PutRowItem(condition, primary_key, attribute_columns)
            item_list.append(put_row_item)
            i += 1
            if i > 169:
                insert2table(TABLE_NAME, item_list)
                item_list = []
                i = 0


def sam2table1(file_path, sample_no):
    file = open("d:/wzh_test/bin_wzh.sam","r")
    lines = file.readlines()
    # 行数
    row_nos = len(lines)
    i = 0
    for item in lines:
        i += 1
        line_list = item.split('\t')
        row_no = i
        bin_no = line_list[0]
        qname = line_list[1]
        flag = line_list[2]
        rname = line_list[3]
        pos = line_list[4]
        mapq = line_list[5]
        cigar = line_list[6]
        rnext = line_list[7]
        pnext = line_list[8]
        tlen = line_list[9]
        seq = line_list[10]
        qual = line_list[11]

        chr_no = rname

        # print row_no

        if chr_no == 'X':
            chr_no = '23'
        elif chr_no == 'Y':
            chr_no = '24'
        elif chr_no == 'MT':
            chr_no = '25'
        elif chr_no == '*':
            chr_no = '26'

        if len(chr_no) == 1:
            chr_no = '0'+ chr_no
        sample_no_chr = int(str(sample_no)+chr_no)
        bin_no = int(bin_no)

        try:
            primary_key = {u"sample_no_chr":sample_no_chr,u"bin_no":bin_no,u'qname':qname,'flag':flag}
            # print primary_key
            attribute_columns = {'rname':rname,'pos':pos, 'mapq':mapq, 'cigar':cigar, 'rnext':rnext,
                                 'pnext':pnext, 'tlen':tlen, 'seq':seq, 'qual':qual,'row_no':row_no,
                                 'attribute_qname':qname,'attribute_flag':flag}
            condition = Condition('EXPECT_NOT_EXIST')
            consumed = ots_client.put_row(TABLE_NAME, condition, primary_key, attribute_columns)
            print u'成功插入数据，消耗的写CapacityUnit为：%s' % consumed.write
        except Exception,e:
            print str(e)


def sam2table2():
    file = open(r"C:\Users\code\Desktop\tmp.sam","r")
    lines = file.readlines()
    # 行数
    row_nos = len(lines)
    i = 0
    for item in lines:
        i += 1
        line_list = item.split('\t')
        row_no = i
        bin_no = line_list[0]
        qname = line_list[1]
        rname = line_list[2]
        flag = line_list[3]
        strand = line_list[4]
        cigar = line_list[5]
        start = line_list[6]
        end = line_list[7]
        seq = line_list[8]

        chr_no = rname

        if chr_no == 'X':
            chr_no = '23'
        elif chr_no == 'Y':
            chr_no = '24'
        elif chr_no == 'MT':
            chr_no = '25'
        elif chr_no == '*':
            chr_no = '26'
        sample_no = 7
        if len(chr_no) == 1:
            chr_no = '0'+ chr_no
        sample_no_chr = int(str(sample_no)+chr_no)
        bin_no = int(bin_no)
        try:
            primary_key = {u"sample_no_chr":sample_no_chr,u"bin_no":bin_no,u'qname':qname,'flag':flag}
            # print primary_key
            attribute_columns = {'rname': rname,'start': start, 'cigar':cigar,
                                 'end': end, 'seq': seq, 'row_no':row_no,'strand': strand,
                                 'attribute_qname':qname,'attribute_flag':flag}
            condition = Condition('EXPECT_NOT_EXIST')
            consumed = ots_client.put_row(TABLE_NAME, condition, primary_key, attribute_columns)
            print u'成功插入数据，消耗的写CapacityUnit为：%s' % consumed.write
        except Exception,e:
            print str(e)

if __name__ == "__main__":
    # chr_no_swich = {
    #     "X":'23',
    #     "Y":'24',
    #     "MT":'25',
    #     "*": '26'
    # }
    # rname = 3
    # print chr_no_swich.get(rname, str(rname).zfill(2))
    sam2table2()