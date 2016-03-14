#! /usr/bin/env python
# coding: utf-8
# author wubo
# 2016/2/25
from ots2 import *
import re,threading,oss2,ots2,subprocess,multiprocessing
from itertools import islice
OTS_ENDPOINT = "http://samplechr.cn-beijing.ots-internal.aliyuncs.com/"
# OTS_ENDPOINT = "http://samplechr.cn-beijing.ots.aliyuncs.com/"
OSS_ENDPOINT = "oss-cn-shenzhen-internal.aliyuncs.com"
ACCESSID = "QFmrMPB18qNx9KYc"
ACCESSKEY = "IuAdh4qL9noDf0UnMOO977HSgZSc0E"
INSTANCENAME = "samplechr"
TABLE_NAME = "sample_chr_info"

ots_client = OTSClient(OTS_ENDPOINT, ACCESSID, ACCESSKEY, INSTANCENAME)
file_pre = "/data/sams/"


def upload_proc(file_path,start_line,end_line,sample_no,id):
    chr_no_swich = {
        "X":'23',
        "Y":'24',
        "MT":'25',
        "*": '26'
    }
    ots_client = OTSClient(OTS_ENDPOINT, ACCESSID, ACCESSKEY, INSTANCENAME)
    # print "process %d start" % id
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
                table_item1 = {'table_name': TABLE_NAME, 'put': item_list, 'update':[], 'delete':[]}
                batch_list = [table_item1]
                batch_write_response = ots_client.batch_write_row(batch_list)
                # print "process %d insert" % id
                pk_set.clear()
                item_list = []
                i = 0
            c += 1


def sam2ts(filepath, sample_no):
    l = 0
    with open(filepath) as file:
        for line in file:
            l += 1
    # l = 98818961
    step = l/20
    start_line = 0
    end_line = start_line + step
    for i in range(19):
        multiprocessing.Process(name="uploader%d" % i,target=upload_proc,args=[filepath,start_line,end_line,sample_no,i]).start()
        start_line = end_line
        end_line = start_line + step
    multiprocessing.Process(name="uploader19", target=upload_proc, args=[filepath,start_line,l+1,sample_no,19]).start()


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
                # print u'操作：%s' % operation
                for j in range(len(operation_item)):
                    row_item = operation_item[j]
                    # print u'操作是否成功：%s' % row_item.is_ok
                    if not row_item.is_ok:
                        # print "error %s" % row_item.error_code
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


def sam2table(filepath,sample_no):
    chr_no_swich = {
        "X":'23',
        "Y":'24',
        "MT":'25',
        "*": '26'
    }
    pk_set = set()
    with open(filepath, "r") as file:
        i = 0
        item_list = []
        for line in file:
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
            condition = Condition('IGNORE')
            primary_key = {u"sample_no_chr": sample_no_chr, u"bin_no": int(bin_no), u'qname': qname, u'flag': flag}
            if (sample_no_chr, int(bin_no), qname, flag) not in pk_set:
                pk_set.add((sample_no_chr, int(bin_no), qname, flag))
                attribute_columns = {'rname': rname,'start': start, 'cigar':cigar,
                                     'end': end, 'seq': seq, 'row_no':row_no,'strand': strand,
                                     'attribute_qname':qname,'attribute_flag':flag}
                put_row_item = PutRowItem(condition, primary_key, attribute_columns)
                item_list.append(put_row_item)
                i += 1
            if i > 169:
                insert2table(TABLE_NAME, item_list)
                pk_set.clear()
                item_list = []
                i = 0

# 处理 cigar 部分
binOffsets = [512+64+8+1, 64+8+1, 8+1, 1, 0]
binFirstShift = 17
binNextShift = 3


def intial_bin(start, end):
    startBin = start - 1
    endBin = end - 1
    startBin >>= binFirstShift
    endBin >>= binFirstShift
    for i in range(len(binOffsets)):
        if startBin == endBin:
            return binOffsets[i] + startBin
        startBin >>= binNextShift
        endBin >>= binNextShift


def get_length(cigar):
    length = 0
    cigar_list = re.findall(r'\d+\w', cigar)
    for item in cigar_list:
        if 'M' in item or 'N' in item or 'H' in item or 'D' in item:
            length += int(re.compile(r'(\d+)\w').search(item).group(1))
    return length


def get_cigar(cigar, seq):
    cigar_list = re.findall(r'\d+\w', cigar)
    new_cigar_list = []
    seq_p = 0
    if len(cigar_list) == 0:
        return "*"
    for item in cigar_list:
        name = item[-1].upper()
        if name == 'S' or name == 'I':
            sl = int(item[0:-1])
            new_cigar_list.append(str(sl)+"I"+seq[seq_p:seq_p+sl])
            seq_p += sl

        if name == 'H' or name == 'D' or name == 'N' or name == 'P':
            new_cigar_list.append(item[0:-1]+"D")

        if name == 'M':
            ml = int(item[0:-1])
            c = 0
            for m in seq[seq_p:seq_p+ml]:
                if m != '=':
                    new_cigar_list.append(str(c)+"M")
                    new_cigar_list.append("1X"+m)
                    c = 0
                else:
                    c += 1
            if c > 0:
                new_cigar_list.append(str(c)+"M")
            seq_p += ml

    return "".join(new_cigar_list)


def handle_sam(filepath, handle_file):
    fwp = open(handle_file, 'w')
    with open(filepath) as file:
        for line in file:
            if line[0] == '@':
                continue
            line = line.strip('\n')
            line_list = line.split('\t')
            qname = line_list[0]
            chr = line_list[2]
            start = line_list[3]
            mate = line_list[9]
            # md = line_list[12]
            #print (line_list[1],bin(line_list[1])
            flag = str(bin(int(line_list[1])))
            strand = int(flag[-5]) + int(flag[-6])
            cigar = line_list[5]
            end = get_length(cigar) + int(start) - 1
            bin_no = intial_bin(int(start), end)
            cigar_str = get_cigar(cigar, mate)
            if cigar_str == "*":
                continue
            # print cigar,cigar_str
            tmp_list=[str(bin_no), qname, chr, line_list[1], str(strand), cigar_str, start, str(end)]
            fwp.write('\t'.join(tmp_list)+'\n')


def handle_thread(account, sample_no, task_id):
    ref_file = "/data/ref/hg19.fa"
    auth = oss2.Auth(ACCESSID, ACCESSKEY)
    bucket = oss2.Bucket(auth,OSS_ENDPOINT, 'jingyun-shenzhen')
    remote_file = "jingyun-output/{account}/{sample_no}/{sample_no}.dedup.bam".format(account=account,sample_no=sample_no)
    local_file = file_pre+"{sample_no}.dedup.bam".format(sample_no=sample_no)
    sam_file = file_pre+"{sample_no}.md.sam".format(sample_no=sample_no)
    handle_file = file_pre+"{sample_no}.hd.sam".format(sample_no=sample_no)
    bucket.get_object_to_file(remote_file, local_file)
    print "download success"
    subprocess.Popen(['samtools', 'calmd','-e',local_file, ref_file, '>', sam_file]).communicate()
    handle_sam(sam_file, handle_file)
    print "handle success"
    sam2table(handle_file, sample_no)
    print "insert success"
    subprocess.Popen(['rm', '-f', local_file]).communicate()
    subprocess.Popen(['rm', '-f', sam_file]).communicate()
    subprocess.Popen(['rm', '-f', handle_file]).communicate()
    print "delete success"


if __name__ == '__main__':
    account = 'geneac'
    sample_no = 3908
    # sample_no = 4
    task_id = 1
    # threading.Thread(target=handle_thread, args=(account, sample_no, task_id)).start()
    handle_file = "{sample_no}.sam".format(sample_no=sample_no)
    # describe_response = ots_client.describe_table('myTable')
    # print u'表的名称: %s' % describe_response.table_meta.table_name
    # handle_file = "C:/Users/code/Desktop/tmp.sam"
    sam2table(handle_file, sample_no)
    print "insert success"
    # sam2table(handle_file, 6)