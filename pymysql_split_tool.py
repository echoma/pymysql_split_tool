import logging,time

import input,db


_last_step = 0
_step_size = 10000
def output_step(row_count, final=False):
    '''Output task progress'''
    global _last_step
    step = int(row_count/_step_size)
    if final or (row_count!=0 and step!=_last_step):
        text = str(row_count)+' rows splited, '+str(len(db.db_new_table))+' new tables effected'
        if final:
            text = "Task Over, "+text
        else:
            text = "until now, "+text
        _last_step = step
        logging.info(text)

def do_work():
    '''The main work flow'''
    src_database_name = input.task['src']['database']
    src_table_name = input.task['src']['database']
    dest_database_name = input.task['dest']['database']
    if input.action=='split':
        total_insert_count = 0
        if 'mysql' in input.task['dest']:
            db.make_conn(args=input.task['src']['mysql'], is_dest=False)
            db.make_conn(args=input.task['dest']['mysql'], is_dest=True)
            db.get_table_structure(src_database_name, src_table_name)
            if 'group_int' not in input.task['rule']:
                logging.debug("Action=split, WorkFlow=A(dest.mysql is set, rule.group_int not set)")
                offset = 0
                while True:
                    paging = False
                    sql = "select "+db.db_column_names_str+" from `"+src_database_name+"`.`"+src_table_name+"`"
                    if 'filter' in input.task['rule']:
                        sql += " where "+input.task['rule']['filter']
                    if 'order_by' in input.task['rule']:
                        sql += " order by "+input.task['rule']['order_by']
                    if 'page_size' in input.task['rule']:
                        sql += " limit "+str(offset)+","+str(input.task['rule']['page_size'])
                        paging = True
                    logging.debug("\tfetching data with sql: "+sql)
                    cursor = db.execute(sql=sql, is_dest=False)
                    if cursor.rowcount==0:
                        break
                    insert_cursor = db.execute('select unix_timestamp()', is_dest=True)
                    while True:
                        many = cursor.fetchmany()
                        if len(many)==0:
                            break
                        for one in many:
                            n = input.group_func(one)
                            db.replace_into_new_table(insert_cursor, input.compose_new_table_name(n), one)
                        db.db_dest.commit()
                    offset += cursor.rowcount
                    total_insert_count = offset
                    output_step(total_insert_count)
                    if not paging:
                        break
                    if 'page_sleep' in input.task['rule']:
                        time.sleep(int(input.task['rule']['page_sleep']))
            else:
                logging.debug("Action=split, WorkFlow=B(dest.mysql is set, rule.group_int is set)")
                for group_int_n in input.group_int_list:
                    logging.info('start group_int '+str(group_int_n))
                    offset = 0
                    insert_cursor = db.execute('select unix_timestamp()', is_dest=True)
                    while True:
                        paging = False
                        sql = "select "+db.db_column_names_str+" from `"+src_database_name+"`.`"+src_table_name+"`"
                        if 'filter' in input.task['rule']:
                            sql += " where "+input.task['rule']['filter']+" and "
                        else:
                            sql += " where "
                        sql += input.compose_group_filter_sql(group_int_n)
                        if 'order_by' in input.task['rule']:
                            sql += " order by "+input.task['rule']['order_by']
                        if 'page_size' in input.task['rule']:
                            sql += " limit "+str(offset)+","+str(input.task['rule']['page_size'])
                            paging = True
                        logging.debug("\tfetching data with sql: "+sql)
                        cursor = db.execute(sql=sql, is_dest=False)
                        if cursor.rowcount==0:
                            break
                        new_table_name = input.compose_new_table_name(group_int_n)
                        while True:
                            many = cursor.fetchmany()
                            if len(many)==0:
                                break
                            for one in many:
                                db.replace_into_new_table(insert_cursor, new_table_name, one)
                            db.db_dest.commit()
                        offset += cursor.rowcount
                        output_step(total_insert_count+offset)
                        if not paging:
                            break
                        total_insert_count += offset
                        output_step(total_insert_count)
                        if 'page_sleep' in input.task['rule']:
                            time.sleep(int(input.task['rule']['page_sleep']))
                    total_insert_count += offset
                    output_step(total_insert_count)
        else:
            db.make_conn(args=input.task['src']['mysql'], is_dest=False)
            db.get_table_structure(src_database_name, src_table_name)
            if 'group_int' not in input.task['rule']:
                logging.debug("Action=split, WorkFlow=C(dest.mysql not set, rule.group_int not set)")
                sql = 'select '+input.compose_group_field_sql()+" from `"+src_database_name+"`.`"+src_table_name+"` "+input.compose_group_by_sql()
                logging.debug("query group_int_list with sql: "+sql)
                cursor = db.execute(sql=sql, is_dest=False)
                for row in cursor.fetchall():
                    input.group_int_list.append(int(row[0]))
                logging.info("query group_int_list from server ok, group_int_list.size="+str(len(input.group_int_list)))
            else:
                logging.debug("Action=split, WorkFlow=D(dest.mysql not set, rule.group_int is set)")
            if 'page_size' in input.task['rule']:
                logging.warning("rule.page_size is defined, but ignored when dest.mysql not defined. continue...")
            for group_int_n in input.group_int_list:
                logging.info('start moving data for group_int '+str(group_int_n))
                new_table_name = input.compose_new_table_name(group_int_n)
                db.create_new_table(new_table_name)
                sql = "replace into `"+dest_database_name+"`.`"+new_table_name+"` select * from `"+src_database_name+"`.`"+src_table_name+"`"
                sql += " where "+input.task['rule']['filter']+" and " if 'filter' in input.task['rule'] else " where "
                sql += input.compose_group_filter_sql(group_int_n)
                logging.debug("\tmoving data directly between two tables inside one server with sql: "+sql)
                cursor = db.execute(sql=sql, is_dest=False)
                db.db_src.commit()
                logging.info("\tok, "+str(cursor.rowcount)+" rows moved")
                total_insert_count += cursor.rowcount
                if 'page_sleep' in input.task['rule']:
                    time.sleep(int(input.task['rule']['page_sleep']))
        output_step(total_insert_count, final=True)
    elif input.action=='check':
        has_dest_mysql = ('mysql' in input.task['dest'])
        db.make_conn(args=input.task['src']['mysql'], is_dest=False)
        if has_dest_mysql:
            db.make_conn(args=input.task['dest']['mysql'], is_dest=True)
        src_database_name = input.task['src']['database']
        src_table_name = input.task['src']['database']
        if 'group_int' not in input.task['rule']:
            sql = 'select '+input.compose_group_field_sql()+" from `"+src_database_name+"`.`"+src_table_name+"` "+input.compose_group_by_sql()
            logging.debug("query group_int_list with sql: "+sql)
            cursor = db.execute(sql=sql, is_dest=False)
            for row in cursor.fetchall():
                input.group_int_list.append(int(row[0]))
            logging.info("query group_int_list from server ok, group_int_list.size="+str(len(input.group_int_list)))
        for group_int_n in input.group_int_list:
            logging.info('start check data for group_int '+str(group_int_n))
            new_table_name = input.compose_new_table_name(group_int_n)
            if 'count' in input.task['check'] and int(input.task['check']['count'])==1:
                sql = "select count(*) from `"+src_database_name+"`.`"+src_table_name+"` "
                sql += " where "+input.task['rule']['filter']+" and " if 'filter' in input.task['rule'] else " where "
                sql += input.compose_group_filter_sql(group_int_n)
                logging.debug("\tcounting records in src table with sql: "+sql)
                cursor = db.execute(sql=sql, is_dest=False)
                count_src = cursor.fetchone()[0]
                sql2 = "select count(*) from `"+dest_database_name+"`.`"+new_table_name+"` "
                sql2 += " where "+input.task['rule']['filter']+" and " if 'filter' in input.task['rule'] else " where "
                sql2 += input.compose_group_filter_sql(group_int_n)
                logging.debug("\tcounting records in dest table with sql: "+sql2)
                cursor = db.execute(sql=sql2, is_dest=has_dest_mysql)
                count_dest = cursor.fetchone()[0]
                if count_src != count_dest:
                    logging.error('Count not match, src='+str(count_src)+', dest='+str(count_dest))
                    logging.error('\tsql_src='+sql)
                    logging.error('\tsql_dest='+sql2)
                    exit(-1)
            if 'sum' in input.task['check'] and isinstance(input.task['check']['sum'], list):
                sql = "select sum(`"+('`),sum(`'.join(input.task['check']['sum']))+"`) from `"+src_database_name+"`.`"+src_table_name+"` "
                sql += " where "+input.task['rule']['filter']+" and " if 'filter' in input.task['rule'] else " where "
                sql += input.compose_group_filter_sql(group_int_n)
                logging.debug("\tSum records in src table with sql: "+sql)
                cursor = db.execute(sql=sql, is_dest=False)
                sum_src = cursor.fetchone()
                sql2 = "select sum(`"+('`),sum(`'.join(input.task['check']['sum']))+"`) from `"+dest_database_name+"`.`"+new_table_name+"` "
                sql2 += " where "+input.task['rule']['filter']+" and " if 'filter' in input.task['rule'] else " where "
                sql2 += input.compose_group_filter_sql(group_int_n)
                logging.debug("\tSum records in dest table with sql: "+sql2)
                cursor = db.execute(sql=sql2, is_dest=has_dest_mysql)
                sum_dest = cursor.fetchone()
                if sum_src != sum_dest:
                    logging.error('Sum not match, src='+str(sum_src)+', dest='+str(sum_dest))
                    logging.error('\tsql_src='+sql)
                    logging.error('\tsql_dest='+sql2)
                    exit(-1)
    elif input.action=='remove':
        db.make_conn(args=input.task['src']['mysql'], is_dest=False)
        src_database_name = input.task['src']['database']
        src_table_name = input.task['src']['database']
        if 'group_int' not in input.task['rule']:
            sql = 'select '+input.compose_group_field_sql()+" from `"+src_database_name+"`.`"+src_table_name+"` "+input.compose_group_by_sql()
            logging.debug("query group_int_list with sql: "+sql)
            cursor = db.execute(sql=sql, is_dest=False)
            for row in cursor.fetchall():
                input.group_int_list.append(int(row[0]))
            logging.info("query group_int_list from server ok, group_int_list.size="+str(len(input.group_int_list)))
        for group_int_n in input.group_int_list:
            logging.info('start removing data for group_int '+str(group_int_n))
            sql = "delete from `"+src_database_name+"`.`"+src_table_name+"` "
            sql += " where "+input.task['rule']['filter']+" and " if 'filter' in input.task['rule'] else " where "
            sql += input.compose_group_filter_sql(group_int_n)
            logging.debug("\tdeleting records in src table with sql: "+sql)
            db.execute(sql=sql, is_dest=False)
            db.db_src.commit()

if __name__ == '__main__':
    input.init_by_cmd_line_args()
    do_work()
