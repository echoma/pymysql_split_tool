import argparse,json,logging,sys,pymysql

parser = argparse.ArgumentParser()
parser.add_argument('--task', required=True)
parser.add_argument('--action', choices=["split","check","remove"], required=True)
parser.add_argument('--debug', action='store_true')
parser.add_argument('--monkey_patch', action='store_true')
args = parser.parse_args()

logging.basicConfig(level=(logging.DEBUG if args.debug else logging.INFO), format='%(asctime)-15s: %(message)s [%(filename)s:%(lineno)s]')

task_file = open(args.task+'.json')
task = json.loads(task_file.read())
task_file.close()

sys.path.append("../pymysql_split_tool")
import __init__

if args.monkey_patch:
    import re
    import db
    def new_get_table_structure(database, table):
        '''Change the table creation sql'''
        db.old_get_table_structure(database, table)
        db.db_ori_table_create_sql = re.sub('AUTO_INCREMENT\=\d+', '', db.db_ori_table_create_sql)
        db.db_ori_table_create_sql = db.db_ori_table_create_sql.replace('AUTO_INCREMENT','')
        db.db_ori_table_create_sql = db.db_ori_table_create_sql.replace('PRIMARY KEY (`id`)','PRIMARY KEY (`data_int`)')
        logging.debug(db.db_ori_table_create_sql)
    db.old_get_table_structure = db.get_table_structure
    db.get_table_structure = new_get_table_structure

__init__.init(args.action, task)
__init__.do_work()