# encoding: utf-8

import MySQLdb
import logging
import time
import threading
import ConfigParser
import platform

__author__ = 'wubo'

"""
Usage:
    from Mysql_db import DB
     db = DB()
     db.execute(sql)
     db.fetchone()
     db.fetchall()
     :return same as MySQLdb
"""
my_os = platform.platform().split('-')[0]
if my_os == "Windows":
    pass
else:
    pass
    # current_filename = sys.argv[0][sys.argv[0].rfind(os.sep) + 1:sys.argv[0].rfind(os.extsep)]
    # logging.basicConfig(filename=current_filename + '_DB.log', filemode='w')

# read config
config = ConfigParser.ConfigParser()
config.read("../config.conf")
env = config.get("Env", "env")


local_host = config.get(env, "service_mysql_host")
remote_host = config.get(env, "remote_mysql_host")


class DB(object):
    conn = None
    cursor = None
    _sock_file = ''

    def __init__(self, local=False, readonly=False):
        self.readonly = readonly
        try:
            if local is True:
                self.host = local_host
            else:
                self.host = remote_host
            config = ConfigParser.ConfigParser()
            config.read('/etc/my.cnf')
            self._sock_file = ""  # config.get('mysqld', 'socket')
        except ConfigParser.NoSectionError:
            self._sock_file = ''

    def connect(self):
        logging.info(time.ctime() + " : connect to mysql server..")
        if self.readonly is False:
            sql_user = 'gpo'
            sql_passwd = "btlc123"
        else:
            sql_user = "gener"
            sql_passwd = "gene_ac252"
        if self._sock_file != '':
            self.conn = MySQLdb.connect(host=self.host, port=3306, user=sql_user,
                                        passwd='btlc123', db='test', charset='utf8',
                                        unix_socket=self._sock_file)
            self.cursor = self.conn.cursor()
        else:
            self.conn = MySQLdb.connect(host=self.host, port=3306, user=sql_user,
                                        passwd=sql_passwd, db='test', charset='utf8')
            self.cursor = self.conn.cursor()

        self.conn.autocommit(True)

    # 线程函数
    def thread(self):
        t = threading.Thread(target=self.conn.ping, args=())
        t.setDaemon(True)
        t.start()
        t.join(4)
        if t.isAlive():
            return 0
        else:
            return 1

    def execute(self, sql_query):
        try:
            logging.info(time.ctime() + " : " + sql_query)
            # 重启超过五次则不再重启
            i = 0
            while i < 3 and self.thread() != 1:
                self.close()
                self.connect()
                self.cursor = self.conn.cursor()
                i += 1
            if i == 3:
                return logging.error(time.ctime() + "execute failed")
            handled_item = self.cursor.execute(sql_query)
        except Exception, e:
            logging.error(e.args)
            logging.info("Reconnecting..---------------")
            self.connect()
            self.cursor = self.conn.cursor()
            logging.info(time.ctime() + " : " + sql_query)
            handled_item = self.cursor.execute(sql_query)
        return handled_item

    def fetchone(self):
        try:
            logging.info(time.ctime() + " : fetchone")
            one_item = self.cursor.fetchone()
        except Exception, e:
            logging.error(e.args)
            logging.info(time.ctime() + " : fetchone failed, return ()")
            one_item = ()
        return one_item

    def fetchall(self):
        try:
            logging.info(time.ctime() + " : fetchall")
            all_item = self.cursor.fetchall()
        except Exception, e:
            logging.error(e.args)
            logging.info(time.ctime() + " : fetchall failed, return ()")
            all_item = ()
        return all_item

    def close(self):
        logging.info(time.ctime() + " : close connect")
        if self.cursor:
            self.cursor.close()
        self.conn.close()

    def check_table(self, table_name, table_desc):
        try:
            check_sql = "DESC %s;" % table_name
            self.execute(check_sql)
            return True
        except Exception, e:
            error_message = str(e.args)
            print(error_message)
            return False
