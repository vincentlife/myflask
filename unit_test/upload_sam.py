#! /usr/bin/env python
# coding: utf-8
__author__ = 'wubo'
# 2016/3/1 0001
from ots2 import *
import threading, datetime
OTS_ENDPOINT = "http://samplechr.cn-beijing.ots-internal.aliyuncs.com/"
# OTS_ENDPOINT = "http://samplechr.cn-beijing.ots.aliyuncs.com/"
OSS_ENDPOINT = "oss-cn-shenzhen-internal.aliyuncs.com"
ACCESSID = "QFmrMPB18qNx9KYc"
ACCESSKEY = "IuAdh4qL9noDf0UnMOO977HSgZSc0E"
INSTANCENAME = "samplechr"
TABLE_NAME = "sample_chr_info"

chr_no_swich = {
    "X":'23',
    "Y":'24',
    "MT":'25',
    "*": '26'
}


def insert2table(TableName, item_list,ots_client):
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
                # print u'操作：%s' % operation
                for j in range(len(operation_item)):
                    row_item = operation_item[j]
                    # print u'操作是否成功：%s' % row_item.is_ok
                    if not row_item.is_ok:
                        print "error %s" % row_item.error_code
                        # print u'错误码：%s' % row_item.error_code
                        # print u'错误信息：%s' % row_item.error_message
                        add_batch_write_item(failed_batch_list, batch_list[i]['table_name'], operation, batch_list[i][operation][j])
                    # else:
                    #     print "success CapacityUnit: %s" % row_item.consumed.write
                        # print u'本次操作消耗的写CapacityUnit为：%s' % row_item.consumed.write

        if not failed_batch_list:
            break
        retry_count += 1
        batch_list = failed_batch_list
        batch_write_response = ots_client.batch_write_row(batch_list)
        # print batch_write_response
# [start_line,end_line)
def upload_thread(file_path,start_line,end_line,sample_no):
    ots_client = OTSClient(OTS_ENDPOINT, ACCESSID, ACCESSKEY, INSTANCENAME)
    with open(file_path) as file:
        c = 0
        i = 0
        pk_set = set()
        item_list = []
        for line in file:
            if c < start_line:
                c += 1
                continue
            if c >= end_line:
                c += 1
                break
            line_list = line.split('\t')
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
            chr = chr_no_swich.get(rname, str(rname).zfill(2))
            sample_no_chr = int(str(sample_no)+chr)
            # condition = Condition('EXPECT_NOT_EXIST')
            condition = Condition('IGNORE')
            primary_key = {u"sample_no_chr": sample_no_chr, u"bin_no": int(bin_no), u'qname': qname, u'flag': flag}
            if (sample_no_chr,int(bin_no),qname,flag) not in pk_set:
                pk_set.add((sample_no_chr,int(bin_no),qname,flag))
                attribute_columns = {'rname': rname,'start': start, 'cigar':cigar,
                                 'end': end, 'seq': seq, 'row_no':row_no,'strand': strand,
                                 'attribute_qname':qname,'attribute_flag':flag}
                put_row_item = PutRowItem(condition, primary_key, attribute_columns)
                item_list.append(put_row_item)
                i += 1
            if i > 169:
                insert2table(TABLE_NAME, item_list,ots_client)
                pk_set.clear()
                item_list = []
                i = 0
            c += 1
    print datetime.datetime.now()


def sam2table(filepath,sample_no):
    l = 0
    with open(filepath) as file:
        for line in file:
            l += 1
    print l
    step = l/10
    start_line = 0
    end_line = start_line + step
    for i in range(10):
        if i == 9:
            threading.Thread(target=upload_thread,args=[filepath,start_line,l,sample_no]).start()
        else:
            threading.Thread(target=upload_thread,args=[filepath,start_line,end_line,sample_no]).start()
            start_line = end_line
            end_line = start_line + step

if __name__ == '__main__':
    print datetime.datetime.now()
    handle_file = "3908.sam"
    sample_no = 3908
    sam2table(handle_file, sample_no)
