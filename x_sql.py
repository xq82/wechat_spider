import pymysql
from setting import MYSQL_SETTING


def Singleton(cls):
    _instance = {}
    def _singleton(*args, **kargs):
        if cls not in _instance:
            _instance[cls] = cls(*args, **kargs)
        return _instance[cls]
    return _singleton


@Singleton
class my_sql(object):
    def __init__(self):
        self.user = MYSQL_SETTING["user"]
        self.host = MYSQL_SETTING["host"]
        self.password = MYSQL_SETTING["password"]
        self.db = MYSQL_SETTING["db"]
        self.charset = MYSQL_SETTING["charset"]
        self.port = MYSQL_SETTING["port"]

        self.conn = pymysql.Connect(host=self.host,
                                  user=self.user,
                                  passwd=self.password,
                                  db=self.db,
                                  port=self.port,
                                  charset=self.charset)
        self.cursor = self.conn.cursor()

    def create_table(self):
        """创建存储数据的表，待抓取的url表，以及抓取过的id表去重"""
        try:
            sql = "CREATE TABLE json_info(id varchar(255) primary key,url VARCHAR(255), title text,article text, images text, update_time date,author varchar(255))"
            self.cursor.execute(sql)
            print("CREATE TABLE json_info OK")
        except:
            print("json_info existence")
        try:
            sql = "CREATE TABLE id_info(id varchar(255) primary key)"
            self.cursor.execute(sql)
            print("CREATE TABLE id_info OK")
        except:
            print("id_info existence")
        try:
            sql = "CREATE TABLE url_info(url varchar(255) primary key)"
            self.cursor.execute(sql)
            print("CREATE TABLE url_info OK")
        except:
            print("url_info existence")

    def find_info(self, sql):
        """查数据"""
        return  self.cursor.execute(sql).fetchall()

    def inser(self, sql):
        """插入数据"""
        self._sql(sql)

    def delete(self, sql):
        self._sql(sql)

    def update(self, sql):
        self._sql(sql)

    def _sql(self, sql):
        """执行sql"""
        try:
            self.cursor.execute(sql)
            self.conn.commit()
            print("执行%s成功" % sql)
        except:
            self.conn.rollback()
            print("%s失败" % sql)

    def close(self):
        self.conn.close()