'''
mysql related things
'''

import logging,pymysql
import input

#mysql connection and cursor
db_src = None
db_src_cursor = None
db_dest = None
db_dest_cursor = None
#mysql table structure
db_columns = None
db_column_names = None
db_column_names_str = None
db_group_column_index = None
db_ori_table_create_sql = None
db_new_table = {}

#make mysql connection
def make_conn(args, is_dest):
    global db_src, db_dest
    if isinstance(args, dict):
        if is_dest:
            db_dest = pymysql.connect(**args)
        else:
            db_src = pymysql.connect(**args)
#execute a statement
def execute(sql, is_dest):
    global db_src_cursor, db_dest_cursor
    if is_dest:
        if db_dest_cursor is None:
            db_dest_cursor = db_dest.cursor()
        db_dest_cursor.execute(sql)
        return db_dest_cursor
    else:
        if db_src_cursor is None:
            db_src_cursor = db_src.cursor()
            db_src_cursor.arraysize = 1000
        db_src_cursor.execute(sql)
        return db_src_cursor
#get column names of a src mysql table
def get_table_structure(database, table):
    global db_columns, db_column_names, db_column_names_str, db_group_column_index, db_ori_table_create_sql
    db_column_names = []
    cursor = db_src.cursor()
    cursor.execute('show columns from `'+database+'`.`'+table+'`')
    db_columns = cursor.fetchall()
    for one in db_columns:
        db_column_names.append(one[0])
    cursor.close()
    db_column_names_str = '`'+'`,`'.join(db_column_names)+'`'
    need_group_column = input.task['rule']['group_method']!='all'
    if need_group_column:
        group_column_name = input.task['rule']['group_column']
        db_group_column_index = db_column_names.index(group_column_name)
    cursor = db_src.cursor()
    cursor.execute('show create table `'+database+'`.`'+table+'`')
    row = cursor.fetchone()
    db_ori_table_create_sql = row[1]
    cursor.close()
#create the new table
def create_new_table(new_table_name):
    sql = db_ori_table_create_sql.replace(input.task['src']['table'], new_table_name, 1)
    sql = sql.replace('CREATE TABLE', 'CREATE TABLE IF NOT EXISTS', 1)
    sql = 'use `'+input.task['dest']['database']+'`; '+sql
    logging.debug('creating table '+new_table_name+' with sql: '+sql)
    if 'mysql' in input.task['dest']:
        cur = db_dest.cursor()
        cur.execute(sql)
        db_dest.commit()
        cur.close()
    else:
        cur = db_src.cursor()
        cur.execute(sql)
        db_src.commit()
        cur.close()
    db_new_table[new_table_name] = 1
#insert a row into new table(will make sure the new table exists), without commiting
def replace_into_new_table(cursor, new_table_name, row):
    if new_table_name not in db_new_table:
        create_new_table(new_table_name)
    sql = 'replace into `'+input.task['dest']['database']+'`.`'+new_table_name+'` values('
    params = []
    column_idx = 0
    for column in db_columns:
        if column_idx!=0:
            sql += ','
        sql += '%s'
        params.append(row[column_idx])
        column_idx = column_idx+1
    sql = sql+')'
    cursor.execute(sql, params)
#replace into new table the value from original table
def replace_into_new_table(cursor, new_table_name, row):
    if new_table_name not in db_new_table:
        create_new_table(new_table_name)
    sql = 'replace into `'+input.task['dest']['database']+'`.`'+new_table_name+'` values('
    params = []
    column_idx = 0
    for column in db_columns:
        if column_idx!=0:
            sql += ','
        sql += '%s'
        params.append(row[column_idx])
        column_idx = column_idx+1
    sql = sql+')'
    cursor.execute(sql, params)