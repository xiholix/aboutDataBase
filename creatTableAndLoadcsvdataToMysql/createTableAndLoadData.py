#--*--coding:utf8--*--
import pandas as pd
import re
import MySQLdb
type_dic = {'int':'int', 'object':'varchar(100)', 'datetime':'datetime', 'float':'float'}

def test_read_csv():
    text = pd.read_csv("dump.csv", header=None)
    print text.columns
    print text.dtypes



def get_create_table_sql_from_csvfile(path, table_name, _field_types):
    '''
    从要输入数据库的文件形式构造出建表语句，以及一个去掉表头的文件
    :param path: 要输入的文件路径
    :param table_name: 要建立的表的名称
    :param _field_types: 数据库中每列的类型组成的字典
    :return: 一个建表的sql语句
    '''
    text = pd.read_csv(path)
    path +='1';
    columns = text.columns
    i = 0
    sql = "create table "+table_name+"("
    for header in columns:
        sql += header+" "
        sql += _field_types[i]+","
        i += 1
    sql = sql[:-1]
    sql += ');'
    print sql

    text.to_csv(path, index=False, header=False)
    return sql


def generate_loadfile_sql(_infile, _table_name):
    '''
    生成载入数据到数据库的sql语句
    :param _infile: 与get_create_table_sql_from_csvfile函数中的path一样
    :param _table_name: 与get_create_table_sql_from_csvfile函数中的table_name要一致
    :return: 一个载入数据到数据库语句
    '''
    sql = 'load data local infile ' +"'"+_infile+"1"+"' into table " + _table_name
    sql += " COLUMNS TERMINATED BY ',' "
    sql += "LINES TERMINATED BY '\\n';"
    print sql
    return sql


def test():
    type = ['int', 'float', 'varchar(20)', 'datetime']
    get_create_table_sql_from_csvfile("/home/xie/code/aboutDataBase/creatTableAndLoadcsvdataToMysql/test.csv", "train", type)
    generate_loadfile_sql("/home/xie/code/aboutDataBase/creatTableAndLoadcsvdataToMysql/test.csv", "train")


def generate_type_list(_pd_data, _datetime_indices=None):
    dtypes = _pd_data.dtypes
    type_list = []
    for t in dtypes:
        t = str(t)
        t = re.sub("\d*", "", t)
        type_list.append(type_dic[t])
    if _datetime_indices!=None:
        type_list[_datetime_indices] = 'datetime'
    print type_list
    return type_list

def test2():
    text = pd.read_csv("test.csv")
    generate_type_list(text, 3)

def generate_sql(_infile, _tableName, _datetime_index=None):
    data = pd.read_csv(_infile)
    typeList = generate_type_list(data, _datetime_index)
    createTableSql = get_create_table_sql_from_csvfile(_infile, _tableName, typeList)
    loadFileSql = generate_loadfile_sql(_infile, _tableName)
    return createTableSql, loadFileSql

def get_connect(_dbName,  _user, _passwd, _host='localhost'):
    try:
        t = MySQLdb.connect(db= _dbName, host=_host, user=_user, passwd=_passwd)
        return t
    except Exception as e:
        print e


def generate_select_sql_by_range_date(_begin_date, _end_date, _table_name, _columnName):
    sql = 'select * from ' + _table_name +" where " + _columnName +" between '"+_begin_date+"' and '"+_end_date+"';";
    return sql


def generate_select_sql_dumpfile(_path, _tableName):
    '''
    该sql语句只能导出到服务器，不能导出到本地
    :param _path:
    :param _tableName:
    :return:
    '''
    sql = 'select * into local outfile \'' +_path +'\' '
    sql += " FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"' "
    sql += " LINES TERMINATED BY '\\n' "
    sql += ' from ' + _tableName +";"
    print sql
    return sql

def execute_sql(_con, _sql):
    cursor = _con.cursor()
    cursor.execute(_sql)
    cursor.close()
    _con.commit()


def execute_query(_con, _sql):
    cursor = _con.cursor()
    cursor.execute(_sql)
    for row in cursor.fetchall():
        print row
        for r in row:
            print r


def dump_file(_con, _sql, _path):
    cursor = _con.cursor()
    cursor.execute(_sql)
    table =''
    for row in cursor.fetchall():
        line = ''
        for r in row:
            line += str(r)+","
        line = line[:-1] +'\n'
        table += line

    f = open(_path, 'wb')
    f.write(table)
    f.close()
    cursor.close()


def test3():
    createSql, loadSql = generate_sql("test.csv", 'train', 3)
    print createSql
    print loadSql
    # con = get_connect("test", "root", "password")
    con = get_connect("xie", "root", "password", "139.199.32.123")
    print loadSql
    execute_sql(con, 'set global max_allowed_packet=67108864')  #用来改变max_allowed_packet
    #解决报错 'Lost connection to MySQL server during query'
    execute_sql(con, createSql)
    execute_sql(con, loadSql)

def test4():
    sql = generate_select_sql_by_range_date('2012-12-02', '2013-02-02', 'train', 'header4')
    con = get_connect("xie", "root", "password", "139.199.32.123")
    execute_query(con, sql)
    dump_file(con, sql, "dump.csv")

def test5():
    sql = generate_select_sql_dumpfile("dump.csv", "train")
    con = get_connect("xie", "root", "password", "139.199.32.123")
    execute_query(con, sql)

if __name__ == "__main__":
    # generate_sql("test.csv", 'train', 3)
    # get_connect()
    # test3()
    # test4()
    test_read_csv()