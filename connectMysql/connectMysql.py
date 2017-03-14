# -*-coding:utf8-*-
#                       _oo0oo_
#                      o8888888o
#                      88" . "88
#                      (| -_- |)
#                      0\  =  /0
#                    ___/`---'\___
#                  .' \\|     |// '.
#                 / \\|||  :  |||// \
#                / _||||| -:- |||||- \
#               |   | \\\  -  /// |   |
#               | \_|  ''\---/''  |_/ |
#               \  .-\__  '-'  ___/-. /
#             ___'. .'  /--.--\  `. .'___
#          ."" '<  `.___\_<|>_/___.' >' "".
#         | | :  `- \`.;`\ _ /`;.`/ - ` : | |
#         \  \ `_.   \_ __\ /__ _/   .-` /  /
#     =====`-.____`.___ \_____/___.-`___.-'=====
#                       `=---='
#
#
#     ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#
#               佛祖保佑         永无BUG
#
#
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
import mysql.connector


def test_connect():
    ''''''

    config = {
        "host": "127.0.0.1",
        "user": "root",
        "password": "abc123",
        "port": 3306,
        "database": "test",
        "charset": "utf8",
    }
    try:
        cnn = mysql.connector.connect(**config)
        print("success connected!")
        return cnn
    except mysql.connector.Error as e:
        print('connected failed {}!'.format(e))


def create_table(_cnn):
    '''
    :param _cnn:连接器
    :return
    :param
    建表语句中的名称均可以用``来括起来，注意此处不是单引号
    '''
    sql = 'CREATE TABLE students2' \
          '(id INT(10) NOT NULL AUTO_INCREMENT,' \
          'name VARCHAR(20) DEFAULT NULL,' \
          'PRIMARY KEY(id))' \
          'DEFAULT CHARSET=utf8'
    cursor = _cnn.cursor()
    try:
        cursor.execute(sql)
    except mysql.connector.Error as e:
        print('created failed {}!'.format(e))


def insert_data(_cnn):

    cursor = _cnn.cursor()
    try:
        # 直接字符串插入方式
        sql_insert1 = 'insert into students(id, name) VALUES(2, "xie")'
        cursor.execute(sql_insert1)

        # 元组插入方式,此处%s是占位符不是格式化字符串所以id也对应%s
        sql_insert2 = "insert into students(id, name) VALUES(%s, %s)"
        datas = (11, "hello")
        cursor.execute(sql_insert2, datas)

        # 字典插入方式
        sql_insert3 = "insert into students(id, name) VALUES(%(id)s, %(name)s)"
        data_dic = {'id': 12,
                    'name': 'hi'}
        cursor.execute(sql_insert3, data_dic)

        #多次插入
        stmt = "insert into students(id, name) VALUES (%s, %s)"
        datas = [(5, 'many'),
                 (6, 'happy'),
                 (7, 'just a test')]
        cursor.executemany(stmt, datas)

        print('insert completed!')
    except mysql.connector.Error as e:
        print('inserted failed {}!'.format(e))
    finally:
        cursor.close()
        # 此处的数据库引擎是Innodb,执行完后需要执行commit进行事务提交
        _cnn.commit()
        _cnn.close()


def query_data(_cnn):
    cursor = _cnn.cursor()
    try:
        sql = 'select name from students where id=%s'
        cursor.execute(sql, (2,))
        for name in cursor:
            print(name)
    except mysql.connector.Error as e:
        print('query failed {}!'.format(e))
    finally:
        cursor.close()
        _cnn.close()


def delete_data(_cnn):
    cursor = _cnn.cursor()
    try:
        sql = 'delete from students where id=%s'
        cursor.execute(sql, (2,))
        print('delete complete!')
    except mysql.connector.Error as e:
        print('deleted failed {}!'.format(e))
    finally:
        cursor.close()
        _cnn.commit()
        _cnn.close()


if __name__ == "__main__":
    cnn = test_connect()
    # create_table(cnn)
    # insert_data(cnn)
    # query_data(cnn)
    delete_data(cnn)

