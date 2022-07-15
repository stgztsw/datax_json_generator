#!/usr/bin/env python
# -*- coding:utf-8 -*-

import datetime
import json as js
import os
from dateutil import relativedelta
import sys, getopt
import re
import pymysql
from pyhive import hive

# -------------------配置项---------------------

partition = "pt"
env = "prd"

# reader 支持hive, mysql
reader_db_type = "mysql"
reader_db_name = "robotbi"
reader_table_name = "robot_fae_dzb"  # 注意oracle的识别大小写，请确保库表名的大小写和实际一致

# writer 支持hive, doris, mysql
writer_db_type = "doris"
writer_db_name = "robotbi"
writer_table_name = "robot_fae_dzb"

haveKerberos = "false"
starrock_dynamic_partition_time_unit = "MONTH"  # 可以指定为: DAY/WEEK/MONTH


# -------------------配置项---------------------

def usage():
    print('请输入如下格式的参数：\r')
    print('-e --env=  环境配置\r')
    print('-i --readertype=  输入数据库类型\r')
    print('-t --readertable=  输入数据库和表\r')
    print('-o --writertype=  输出数据库类型\r')
    print('-x --writertable=  输出数据库和表\r')


try:
    options, args = getopt.getopt(sys.argv[1:], '-e:-i:-t:-o:-x:',
                                  ['env=', 'readertype=', 'readertable=', 'writertype=', 'writertable='])
    for name, value in options:
        if name in ('-e', '--env'):
            env = value
        elif name in ('-i', '--readertype'):
            reader_db_type = value
        elif name in ('-t', '--readertable'):
            reader_db_table_name = value
        elif name in ('-o', '--writertype'):
            writer_db_type = value
        elif name in ('-x', '--writertable'):
            writer_table_name = value
except:
    usage()

print("任务配置:")
print(
    "env=%s, reader_db_type=%s, reader_db_name=%s, reader_table_name=%s, writer_db_type=%s, writer_db_name=%s, writer_table_name=%s"
    % (env, reader_db_type, reader_db_name, reader_table_name, writer_db_type, writer_db_name, writer_table_name))

db_infos = {
    "mysql": {
        "prd": {
            "incident": {
                "host": "rr-uf60mutizl4hh289a.mysql.rds.aliyuncs.com",
                "user": "readonly",
                "password": "FD9Rmaw5uvQ4P6aAjfaa",
                "port": "3306"
            },
            "robotbi": {
                "host": "rm-uf6sg7am50o523c9a.mysql.rds.aliyuncs.com",
                "user": "robotbi",
                "password": "hXfNbXxd09Gi3vULIel1kgV8",
                "port": "3306"
            },
        }
    },
    "hive": {
        "prd": {
            "host": "172.19.151.79",
            "user": "developer",
            "password": "Developer_001",
            "port": "10009"
        }
    },
    "doris": {
        "prd": {
            "feLoadUrl": ["172.19.24.157:8030"],
            "beLoadUrl": ["172.19.24.157:8040", "172.19.24.156:8040", "172.19.24.155:8040"],
            "jdbcUrl": "jdbc:mysql://172.19.24.157:9030/",
            "host": "172.19.24.157",
            "user": "developer",
            "password": "Developer_001",
            "port": "9130",
            "maxBatchRows": 50000,
            "maxBatchByteSize": 10485760,
            "labelPrefix": "my_prefix",
            "lineDelimiter": "\n"
        }
    }
}
hdfs_infos = {
    "prd": {
        "defaultFS": "oss://cloud-emr-prod.cn-shanghai.oss-dls.aliyuncs.com",
    }
}

if reader_db_type == "mysql":
    reader_db_info = db_infos.get(reader_db_type).get(env).get(reader_db_name)
else:
    reader_db_info = db_infos.get(reader_db_type).get(env)
if writer_db_type == "mysql":
    writer_db_info = db_infos.get(writer_db_type).get(env).get(writer_db_name)
else:
    writer_db_info = db_infos.get(writer_db_type).get(env)

hdfs_info = hdfs_infos.get(env)
workdir = os.path.join(os.getcwd(), reader_db_name, reader_table_name)
# reader字段信息缓存，writer处理中复用到
column_type_list = []


def create_dir():
    if not os.path.exists(workdir):
        os.makedirs(workdir)


def create_setting_json(reader_db_type, writer_db_type, channel=1):
    setting = {"speed": {"channel": channel}, "errorLimit": {"record": 0}}
    return setting


# def create_oracle_reader_json():
#     """创建oracle reader"""
#     global column_type_list
#     conn = cx_Oracle.connect(reader_db_info.get("user"), reader_db_info.get("password"),
#                              reader_db_info.get("listener"))  # type: Connection
#     cursor = conn.cursor()  # type: cx_Oracle.Cursor
#     db_name = get_reader_db_table_name()[0].upper()
#     table_name = get_reader_db_table_name()[1].upper()
#     sql = "select col.COLUMN_NAME, col.DATA_TYPE, com.COMMENTS " \
#           "from all_tab_columns col " \
#           "left join ALL_COL_COMMENTS com on col.OWNER = com.OWNER and col.TABLE_NAME = com.TABLE_NAME and col.COLUMN_NAME = com.COLUMN_NAME " \
#           "where col.OWNER='%s' and col.TABLE_NAME='%s' order by col.COLUMN_ID" % (db_name, table_name)
#
#     print("oracle schema sql = %s" % sql)
#     cursor.execute(sql)
#     count = 0
#     for x in cursor.fetchall():
#         s = (count, x[0], x[1], x[2])
#         count = count + 1
#         column_type_list.append(s)
#     if len(column_type_list) == 0:
#         raise Exception("can not found the column info in %s, please check if the config of is right",
#                         reader_db_table_name)
#     column_list = list(map(lambda x: x[1], column_type_list))
#     connection = [
#         {"table": [reader_db_table_name], "jdbcUrl": ["jdbc:oracle:thin:@//" + reader_db_info.get("listener")]}]
#     oracle_reader = {"name": "oraclereader", "parameter": {"username": reader_db_info.get("user"),
#                                                            "password": reader_db_info.get("password"),
#                                                            "column": column_list, "splitPk": column_list[0],
#                                                            "connection": connection}}
#     return oracle_reader


def reader_type_mapping(data_type: str):
    #  reader type映射
    global reader_db_type
    data_type = data_type.upper()
    index = data_type.find("(")
    rtv_data_type = None
    data_len = None
    if index > 0:
        data_len = data_type[index + 1:len(data_type) - 1]
        data_type = data_type[0:index]
    if reader_db_type.lower() == "hive":
        if data_type in ("TINYINT", "SMALLINT", "INT", "BIGINT"):
            rtv_data_type = "Long"
        elif data_type in ("FLOAT", "DOUBLE"):
            rtv_data_type = "Double"
        elif data_type in ("STRING", "CHAR", "VARCHAR", "STRUCT", "MAP", "ARRAY", "UNION"):
            rtv_data_type = "String"
        elif data_type == "BOOLEAN":
            rtv_data_type = "Boolean"
        elif data_type == "DATE":
            rtv_data_type = "Date"
        elif data_type == "TIMESTAMP":
            rtv_data_type = "Timestamp"
        elif data_type == "BINARY":
            rtv_data_type = "Binary"
    elif reader_db_type.lower() == "mysql":
        if data_type in ("INT", "TINYINT", "SMALLINT", "MEDIUMINT", "INT", "BIGINT", "INT UNSIGNED"):
            rtv_data_type = "Long"
        elif data_type in ("FLOAT", "DOUBLE", "DECIMAL"):
            rtv_data_type = "Double"
        elif data_type in ("VARCHAR", "CHAR", "TINYTEXT", "TEXT", "MEDIUMTEXT", "LONGTEXT", "YEAR", "JSON"):
            rtv_data_type = "String"
        elif data_type in ("DATE", "DATETIME", "TIMESTAMP", "TIME"):
            rtv_data_type = "Date"
        elif data_type in ("BIT", "BOOL"):
            rtv_data_type = "Boolean"
        elif data_type == "BINARY":
            rtv_data_type = "Binary"
        elif data_type in ("TINYBLOB", "MEDIUMBLOB", "BLOB", "LONGBLOB", "VARBINARY"):
            rtv_data_type = "Bytes"
    return rtv_data_type, data_len


def writer_type_mapping(reader_data_type: str):
    """writer type映射"""
    global writer_db_type
    datax_data = reader_type_mapping(reader_data_type)
    datax_type = datax_data[0]
    datax_len = datax_data[1]
    if writer_db_type.lower() == "hive":
        if datax_type == "Long":
            return "BIGINT"
        if datax_type == "Double":
            return "DOUBLE"
        if datax_type == "String":
            return "STRING"
        if datax_type == "Boolean":
            return "BOOLEAN"
        if datax_type == "Date":
            # return "time" 修复hive 类型只显示日期的问题
            return "timestamp"
        if datax_type == "Binary":
            return "BINARY"
    elif writer_db_type.lower() == "starrock" or writer_db_type.lower() == "doris":
        if datax_type == "Long":
            return "BIGINT"
        if datax_type == "Double":
            return "DOUBLE"
        if datax_type == "String":
            if datax_len is None:
                return "String"
            else:
                return "VARCHAR(%s)" % (int(datax_len, 10)*2)
        if datax_type == "Boolean":
            return "BOOLEAN"
        if datax_type == "Date":
            return "DATETIME"
    elif writer_db_type.lower() == "mysql":
        if datax_type == "Long":
            return "bigint"
        if datax_type == "Double":
            return "double"
        if datax_type == "String":
            return "varchar"
        if datax_type == "Boolean":
            return "bool"
        if datax_type == "Date" or datax_type == "Timestamp":
            # return "time" 修复hive 类型只显示日期的问题
            return "datetime"
        if datax_type == "Binary":
            return "binary"


def create_hive_reader_json():
    conn = hive.Connection(host=reader_db_info.get("host"), port=reader_db_info.get("port"),
                           username=reader_db_info.get("user"),
                           auth="CUSTOM", password=reader_db_info.get("password"))  # type: hive.Connection
    global partition, haveKerberos, column_type_list, reader_db_name, reader_table_name
    cursor = conn.cursor()  # type: hive.Cursor
    cursor.execute("desc %s.%s" % (reader_db_name, reader_table_name))
    count = 0
    # column json
    for x in cursor.fetchall():
        if x[0] == partition or x[1] is None or x[0].startswith("#"):
            continue
        data_type = x[1]
        s = (count, x[0], data_type, x[2])
        count = count + 1
        column_type_list.append(s)
    column_list = list(map(lambda y: {"index": y[0], "type": reader_type_mapping(y[2])[0]}, column_type_list))
    path = os.path.join("/user/hive/warehouse/", reader_db_name + ".db/", reader_table_name + "/", partition + "=${biz_date}")
    hive_reader = {"name": "hdfsreader", "parameter": {"path": path, "column": column_list, "fileType": "orc",
                                                       "encoding": "UTF-8", "fieldDelimiter": "\t",
                                                       "haveKerberos": haveKerberos,
                                                       "defaultFS": hdfs_info.get("defaultFS"),
                                                       "hadoopConfig": hdfs_info.get("hadoopConfig")}}
    return hive_reader


def process_col_name(col_name):
    """字段 """
    col_name = col_name.strip()
    if col_name.startswith("`"):
        col_name = col_name[1:]
    if col_name.endswith("`"):
        col_name = col_name[:len(col_name) - 1]
    return col_name


def parse_table_schema(table_schema_file_path):
    """获取元数据信息"""
    global column_type_list
    # 修复windows下unicode报错
    if os.name == 'nt':
        with open(table_schema_file_path, 'r', encoding='UTF-8') as file_to_read:
            context = file_to_read.readlines()
            count = 0
            for line in context:
                if re.search("create\s+table\s+", line.lower()) or re.search("primary\s+key\s+", line.lower()) \
                        or re.search("unique\s+key\s+", line.lower()) or re.search("key\s+", line.lower()) \
                        or re.search("engine\s*=innodb\s+", line.lower()):
                    continue
                if line.upper().find(' COMMENT ') > -1:
                    match_obj = re.match(r'(.*?)\s+(.*?)\s+.*(COMMENT\s+\'(.*?)\')', line.upper().strip())
                    s = (
                        count, process_col_name(match_obj.group(1).lower()), match_obj.group(2).lower(),
                        match_obj.group(4))
                else:
                    match_obj = re.match(r'(.*?)\s+(.*?)\s+.*', line.upper().strip())
                    s = (count, process_col_name(match_obj.group(1).lower()), match_obj.group(2).lower(), None)
                count = count + 1
                column_type_list.append(s)
    else:
        with open(table_schema_file_path, 'r') as file_to_read:
            context = file_to_read.readlines()
            count = 0
            for line in context:
                if re.search("create\s+table\s+", line.lower()) or re.search("primary\s+key\s+", line.lower()) \
                        or re.search("unique\s+key\s+", line.lower()) or re.search("key\s+", line.lower()) \
                        or re.search("engine\s*=innodb\s+", line.lower()):
                    continue
                if line.upper().find(' COMMENT ') > -1:
                    match_obj = re.match(r'(.*?)\s+(.*?)\s+.*(COMMENT\s+\'(.*?)\')', line.upper().strip())
                    s = (
                        count, process_col_name(match_obj.group(1).lower()), match_obj.group(2).lower(),
                        match_obj.group(4))
                else:
                    match_obj = re.match(r'(.*?)\s+(.*?)\s+.*', line.upper().strip())
                    s = (count, process_col_name(match_obj.group(1).lower()), match_obj.group(2).lower(), None)
                count = count + 1
                column_type_list.append(s)


def create_mysql_reader_json():
    global reader_db_name, reader_table_name
    host = reader_db_info.get("host")
    port = reader_db_info.get("port")
    user = reader_db_info.get("user")
    password = reader_db_info.get("password")
    connect = pymysql.connect(host=host, user=user, password=password, db=reader_db_name, charset='utf8')
    cursor = connect.cursor()  # type: pymysql.cursors.Cursor
    sql = "select COLUMN_NAME, COLUMN_TYPE, COLUMN_COMMENT " \
          "from information_schema.`COLUMNS` " \
          "where TABLE_SCHEMA = '%s' and TABLE_NAME ='%s' " \
          "order by ORDINAL_POSITION" % (reader_db_name, reader_table_name)
    print("mysql schema sql = %s" % sql)
    cursor.execute(sql)
    count = 0
    for x in cursor.fetchall():
        s = (count, x[0], x[1], x[2])
        count = count + 1
        column_type_list.append(s)
    if len(column_type_list) == 0:
        raise Exception("can not found the column info in %s, please check if the config of is right",
                        reader_db_table_name)
    column_list = list(map(lambda x: x[1], column_type_list))
    connection = [
        {"table": [reader_table_name], "jdbcUrl": ["jdbc:mysql://%s:%s/%s" % (host, port, reader_db_name)]}]
    mysql_reader = {"name": "mysqlreader", "parameter": {"username": user, "password": password,
                                                         "column": column_list, "splitPk": column_list[0],
                                                         "connection": connection}}
    return mysql_reader


def create_reader_json(reader_type: str):
    if reader_type.lower() == "hive":
        return create_hive_reader_json()
    elif reader_type.lower() == "mysql":
        return create_mysql_reader_json()


def create_hive_writer_json():
    global column_type_list, haveKerberos, writer_db_name, writer_table_name
    column_list = list(map(lambda x: {"name": x[1], "type": writer_type_mapping(x[2])}, column_type_list))
    path = "/".join(["/user/hive/warehouse", writer_db_name + ".db", writer_table_name, partition + "=${biz_date}"])
    hive_writer = {"name": "hdfswriter", "parameter": {"defaultFS": hdfs_info.get("defaultFS"), "compress": "NONE",
                                                       "fileType": "orc", "writeMode": "truncate", "fieldDelimiter": "\t",
                                                       "path": path, "fileName": writer_table_name, "column": column_list,
                                                       "haveKerberos": haveKerberos}}
    return hive_writer


def create_doris_writer_json():
    global column_type_list, writer_db_name, writer_table_name
    column_list = list(map(lambda x: x[1], column_type_list))
    doris_writer = {"name": "doriswriter", "parameter": {"feLoadUrl": writer_db_info.get("feLoadUrl"),
                                  "beLoadUrl": writer_db_info.get("beLoadUrl"), "jdbcUrl": writer_db_info.get("jdbcUrl"),
                                  "database": writer_db_name, "table": writer_table_name, "column": column_list,
                                  "username": writer_db_info.get("user"), "password": writer_db_info.get("password"),
                                  "maxBatchRows": writer_db_info.get("maxBatchRows"), "maxBatchByteSize": writer_db_info.get("maxBatchByteSize"),
                                  "labelPrefix": writer_db_info.get("labelPrefix"), "lineDelimiter": writer_db_info.get("lineDelimiter")}}
    return doris_writer


def create_mysql_writer_json():
    global column_type_list, haveKerberos, writer_db_name, writer_table_name
    user = writer_db_info.get("user")
    password = writer_db_info.get("password")
    host = reader_db_info.get("host")
    port = reader_db_info.get("port")
    column_list = list(map(lambda x: x[1], column_type_list))
    path = "/".join(["/user/hive/warehouse", writer_db_name + ".db", writer_table_name, partition + "=${biz_date}"])
    hive_writer = {"name": "mysqlwriter", "parameter": {"writeMode": "replace", "username": user, "password": password,
                                                        "column": column_list, "connection": [{"jdbcUrl": "jdbc:mysql://%s:%s/%s?useUnicode=true&characterEncoding=utf8" % (host, port, writer_db_name), "table": [writer_table_name]}]}}
    return hive_writer


def create_writer_json(writer_type):
    if writer_type == "hive":
        return create_hive_writer_json()
    elif writer_type == "doris":
        return create_doris_writer_json()
    elif writer_type == "mysql":
        return create_mysql_writer_json()


def create_datax_json(table_schema_file_path=None, file_name=None):
    global reader_db_type, writer_db_type
    setting_json = create_setting_json(reader_db_type=reader_db_type, writer_db_type=writer_db_type)
    # reader类型，元数据路径，create文件路径
    reader_json = create_reader_json(reader_type=reader_db_type)
    writer_json = create_writer_json(writer_db_type)
    datax_json = {"job": {"content": [{"reader": reader_json, "writer": writer_json}], "setting": setting_json}}
    datax_json = js.dumps(datax_json, indent=4, ensure_ascii=False)
    file_name = "%s2%s_%s.json" % (reader_db_type, writer_db_type, writer_table_name)
    with open(os.path.join(workdir, file_name), "w+", encoding="utf-8") as f:
        f.write(datax_json)


def strip(s: str):
    if s is not None:
        if "'" in s:
            print("COMMENT '%s'" % s.replace("'", "\\'"))
            return "COMMENT '%s'" % s.replace("'", "\\'")
        else:
            return "COMMENT '%s'" % s.strip()

    return ""


def create_writer_hive_table_sql():
    global column_type_list, writer_db_name, writer_table_name
    file_name = "create_table_%s_sql" % writer_table_name
    with open(os.path.join(workdir, file_name), "w+", encoding="utf-8") as f:
        f.write("create table IF NOT EXISTS %s.%s (\r" % (writer_db_name, writer_table_name))
        length = len(column_type_list)
        count = 1
        for x in column_type_list:
            if count == length:
                f.write("%s %s %s\r" % (x[1], writer_type_mapping(x[2]), strip(x[3])))
            else:
                f.write("%s %s %s,\r" % (x[1], writer_type_mapping(x[2]), strip(x[3])))
            count = count + 1
        f.write(") comment ''\r")
        f.write("partitioned by (pt string comment '')\r")
        f.write("STORED AS ORC\r")


def create_writer_mysql_table_sql():
    global column_type_list, writer_db_name, writer_table_name
    file_name = "create_table_%s_sql" % writer_table_name
    with open(os.path.join(workdir, file_name), "w+", encoding="utf-8") as f:
        f.write("create table %s.%s (\r" % (writer_db_name, writer_table_name))
        length = len(column_type_list)
        count = 1
        for x in column_type_list:
            if count == length:
                f.write("%s %s %s\r" % (x[1], writer_type_mapping(x[2]), strip(x[3])))
            else:
                f.write("%s %s %s,\r" % (x[1], writer_type_mapping(x[2]), strip(x[3])))
            count = count + 1
        f.write(")\r")


def starrock_dynamic_partition(f):
    f.write("PARTITION BY RANGE(请在括号内填写分区字段)(\r")
    curr_time = datetime.datetime.now()
    if starrock_dynamic_partition_time_unit.lower() == "day":
        for x in range(-3, 3):
            delta = datetime.timedelta(days=x)
            n_day = curr_time + delta
            n_day_plus_1 = n_day.strftime('%Y-%m-%d')
            delta = datetime.timedelta(days=-1)
            n_day = n_day + delta
            n_day_ = n_day.strftime('%Y%m%d')
            if x == 2:
                f.write("PARTITION p%s VALUES LESS THAN (\"%s\")\r" % (n_day_, n_day_plus_1))
            else:
                f.write("PARTITION p%s VALUES LESS THAN (\"%s\"),\r" % (n_day_, n_day_plus_1))
    elif starrock_dynamic_partition_time_unit.lower() == "month":
        for x in range(-3, 3):
            n_day = curr_time + relativedelta.relativedelta(months=x)
            the_month_start_day = datetime.datetime(n_day.year, n_day.month, 1)
            the_month_start_day_str = the_month_start_day.strftime('%Y-%m-%d')
            month_before = the_month_start_day + relativedelta.relativedelta(months=-1)
            month_before_str = month_before.strftime('%Y%m')
            if x == 2:
                f.write("PARTITION p%s VALUES LESS THAN (\"%s\")" % (month_before_str, the_month_start_day_str))
            else:
                f.write("PARTITION p%s VALUES LESS THAN (\"%s\"),\r" % (month_before_str, the_month_start_day_str))
    f.write(") DISTRIBUTED BY HASH(请在括号内填写分桶字段) BUCKETS 5\r")


def create_writer_doris_starrock_table_sql():
    global column_type_list, writer_db_name, writer_table_name
    file_name = "create_table_%s_sql" % writer_table_name
    with open(os.path.join(workdir, file_name), "w+", encoding="utf-8") as f:
        f.write("CREATE TABLE IF NOT EXISTS %s.%s (\r" % (writer_db_name, writer_table_name))
        length = len(column_type_list)
        count = 1
        for x in column_type_list:
            if count == length:
                f.write("%s %s %s\r" % (x[1], writer_type_mapping(x[2]), strip(x[3])))
            else:
                f.write("%s %s %s,\r" % (x[1], writer_type_mapping(x[2]), strip(x[3])))
            count = count + 1
        f.write(") DUPLICATE KEY(请在括号内填写key)\r")
        f.write("DISTRIBUTED BY HASH(请在括号内填写key) BUCKETS 5")
        # starrock_dynamic_partition(f)
        # f.write("PROPERTIES(\r")
        # f.write("\"replication_num\" = \"1\",\r")
        # f.write("\"dynamic_partition.enable\" = \"true\",\r")
        # f.write("\"dynamic_partition.time_unit\" = \"DAY\",\r")
        # f.write("\"dynamic_partition.start\" = \"-10\",\r")
        # f.write("\"dynamic_partition.end\" = \"10\",\r")
        # f.write("\"dynamic_partition.prefix\" = \"p\",\r")
        # f.write("\"dynamic_partition.buckets\" = \"5\"\r")
        # f.write(")")


def create_writer_table_sql():
    if writer_db_type == "hive":
        create_writer_hive_table_sql()
    elif writer_db_type == "starrock" or writer_db_type == "doris":
        create_writer_doris_starrock_table_sql()
    elif writer_db_type == 'mysql':
        create_writer_mysql_table_sql()


def do_batch():
    global mysql_file_path, reader_db_table_name
    for root, dirs, files in os.walk(mysql_file_path):
        for file in files:
            path = os.path.join(root, file)
            reader_db_table_name = file
            create_dir()
            create_datax_json(table_schema_file_path=path, file_name=file)
            create_writer_table_sql()


if __name__ == '__main__':
    create_dir()
    create_datax_json()
    create_writer_table_sql()
