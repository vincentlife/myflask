#! /usr/bin/env python
# coding: utf-8
# author wubo
# 2016/2/25
import re,threading,oss2,subprocess,os
file_pre = "/data/pgb_data/sams/"
ref_file = "/data/ref/hg19.fa"
pb_pre = "/data/pgb_data/pb/"

# 处理 cigar 部分
binOffsets = [512+64+8+1, 64+8+1, 8+1, 1, 0]
binFirstShift = 17
binNextShift = 3


def intial_bin(start, end):
    '''
    初始化bin
    :param start:
    :param end:
    :return:
    '''
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
    '''
    获取cigar长度
    :param cigar:
    :return:
    '''
    length = 0
    cigar_list = re.findall(r'\d+\w', cigar)
    for item in cigar_list:
        if 'M' in item or 'N' in item or 'H' in item or 'D' in item:
            length += int(re.compile(r'(\d+)\w').search(item).group(1))
    return length


def get_cigar(cigar, seq):
    '''
    从.md.sam文件的cigar和seq中得出所需cigar
    :param cigar:
    :param seq:
    :return:
    '''
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
    '''
    处理.md.sam 文件 生成cigar改变了的.hd.sam文件
    :param filepath:
    :param handle_file:
    :return:
    '''
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
    '''
    任务线程
    :param account:
    :param sample_no:
    :param task_id:
    :return:
    '''
    local_file = file_pre+"{sample_no}.dedup.bam".format(sample_no=sample_no)
    sam_file = file_pre+"{sample_no}.md.sam".format(sample_no=sample_no)
    handle_file = file_pre+"{sample_no}.hd.sam".format(sample_no=sample_no)
    log_file = file_pre+"{sample_no}.log".format(sample_no=sample_no)
    f1 = open(sam_file,"w")
    f2 = open(log_file,"w")
    subprocess.Popen(['samtools', 'calmd','-e',local_file, ref_file],stdout=f1,stderr=f2).communicate()
    f1.close()
    f2.close()
    handle_sam(sam_file, handle_file)

def test_cal():
    packet_proc = "/home/msg/BioMed/service/protoc/BatchWrite"
    upload_proc = "../pgb_upload_proc.py"
    handle_file = "/data/pgb_data/sams/3911.hd.sam"
    sample_no = 3911
    pb_dir = "/data/pgb_data/pb/3911"
    print "start"
    num = subprocess.call([packet_proc, str(sample_no), handle_file, pb_dir, upload_proc])
    print num

if __name__ == '__main__':
    test_cal()
    # account = "geneac"
    # sample_no = 3911
    # task_id = "fef"
    # # threading.Thread(target=handle_thread, args=(account, sample_no, task_id)).start()
    # handle_file = file_pre+"{sample_no}.hd.sam".format(sample_no=sample_no)
    # log_file = file_pre+"{sample_no}.log".format(sample_no=sample_no)
    # packet_proc = "/data/Web2/BioMed/service/protoc/BatchWrite"
    # num = subprocess.call([packet_proc,sample_no, handle_file, pb_dir, upload_proc, sample_no])