#--*--coding:utf8--*--
import pandas as pd
import re
import MySQLdb
type_dic = {'int':'int', 'object':'varchar(100)', 'datetime':'datetime', 'float':'float'}

def test_read_csv():
    text = pd.read_csv("test.csv")
    print text.columns
    print text.dtypes



def get_create_table_sql_from_csvfile(path, table_name, _field_types):
    text = pd.read_csv(path)
    path +='1';
    columns = text.columns
    dtypes = text.dtypes
    # print type(columns[0])
    # print type(dtypes[columns[0]])
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


def execute_sql(_con, _sql):
    cursor = _con.cursor()
    cursor.execute(_sql)
    cursor.close()
    _con.commit()


def test3():
    createSql, loadSql = generate_sql("train.csv", 'train')
    print createSql
    print loadSql
    # con = get_connect("test", "root", "password")
    con = get_connect("xie", "root", "password", "139.199.32.123")
    print loadSql
    execute_sql(con, 'set global max_allowed_packet=67108864')  #用来改变max_allowed_packet
    #解决报错 'Lost connection to MySQL server during query'
    execute_sql(con, createSql)
    execute_sql(con, loadSql)


if __name__ == "__main__":
    # generate_sql("test.csv", 'train', 3)
    # get_connect()
    test3()