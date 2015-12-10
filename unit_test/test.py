# -*- coding: utf-8 -*-
# Create Date 2015/12/9
__author__ = 'wubo'
import MySQLdb
if __name__ == "__main__":
    # 只对应一个账户的病人
    local_mysql = '192.168.120.2'
    remote_mysql = 'rdsikqm8sr3rugdu1muh3.mysql.rds.aliyuncs.com'
    conn = MySQLdb.connect(host=local_mysql, user='gpo', passwd='btlc123',db='clinic',port=3306)
    db = conn.cursor()
    sel_sql_1 = "SELECT patient_no,GROUP_CONCAT(account),COUNT(account) t from patients_user_right group by patient_no HAVING t=1;"
    db.execute(sel_sql_1)
    l = db.fetchall()
    for item in l:
        insert_sql = "UPDATE sys_patients SET account='%s' WHERE patient_no= %d;" % (item[1], item[0])
        # print insert_sql
        db.execute(insert_sql)
    # 对应多个账户的病人
    sel_sql_n = "SELECT patient_no,GROUP_CONCAT(account),COUNT(account) t from patients_user_right group by patient_no HAVING t>1;"
    db.execute(sel_sql_n)
    li = db.fetchall()
    account_list = ["songlei","geneac","JingYun","yantizhen","liyang","gaowen","yangrui"]
    for item in li:
        flag = 0
        accounts = item[1]
        for account in account_list:
            if account in accounts:
                flag = 1
                insert_sql = "UPDATE sys_patients SET account='%s' WHERE patient_no= %d;" % (account, item[0])
                # print "+++"+insert_sql
                break
        if flag == 0:
            insert_sql = "UPDATE sys_patients SET account='%s' WHERE patient_no= %d;" % (accounts.split(',')[0], item[0])
            # print "---"+insert_sql
        db.execute(insert_sql)