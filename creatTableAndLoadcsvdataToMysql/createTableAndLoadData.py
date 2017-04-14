import pandas as pd
import re
type_dic = {'int':'int', 'object':'varchar(100)', 'datetime':'datetime', 'float':'float'}

def test_read_csv():
    text = pd.read_csv("test.csv")
    print text.columns
    print text.dtypes



def get_create_table_sql_from_csvfile(path, table_name, _field_types):
    text = pd.read_csv(path)
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
    return sql


def generate_loadfile_sql(_infile, _table_name):
    sql = 'load data local infile ' +"'"+_infile+"' into table " + _table_name
    sql += " COLUMNS TERMINATED BY ',' "
    sql += "LINES TERMINATED BY '\\n';"
    print sql


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

def test2():
    text = pd.read_csv("test.csv")
    generate_type_list(text, 3)
if __name__ == "__main__":
    test2()