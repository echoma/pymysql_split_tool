'''
parse input args, parse task file
'''
import argparse,json,logging,sys
import db

#command line arguments
action = None
task = None

#parse the command line arguments
_parser = argparse.ArgumentParser()
_parser.add_argument('--action', choices=['split','check','remove'], required=True, help='type of task')
_parser.add_argument('--task', required=True, help='path to task file')
_parser.add_argument('--debug', action='store_true', help='print debug logs')
_args = _parser.parse_args()
action = _args.action
#logger
_log_format_str = '%(asctime)-15s: %(message)s [%(filename)s:%(lineno)s]'
if _args.debug:
    logging.basicConfig(level=logging.DEBUG, format=_log_format_str)
else:
    logging.basicConfig(level=logging.INFO, format=_log_format_str)
_task_file = open(_args.task)
task = json.loads(_task_file.read())
_task_file.close()

#json to arg list
def json2args(jsondict):
    ret = []
    for key in jsondict:
        ret.append('--'+key)
        if not isinstance(jsondict[key], dict):
            ret.append(str(jsondict[key]))
        else:
            ret.append('@dict')
    return ret
#our arg parser
class MyArgParser(argparse.ArgumentParser):
    level_name = ''
    def error(self, message):
        logging.error('In '+self.level_name+', '+message.replace('argument', 'field').replace('--',''))
        exit(-1)

#check task json level-1 fields
_arg_str = json2args(task)
_parser = MyArgParser()
_parser.level_name = 'task file'
_parser.add_argument('--src', required=True)
_parser.add_argument('--dest', required=True)
_parser.add_argument('--rule', required=True)
_args = _parser.parse_args(_arg_str)
#check task.src json level-2 fields
_arg_str = json2args(task['src'])
_parser = MyArgParser()
_parser.level_name = 'task.src'
_parser.add_argument('--mysql', required=True)
_parser.add_argument('--database', required=True, type=str)
_parser.add_argument('--table', required=True, type=str)
_args = _parser.parse_args(_arg_str)
#check task.dest json level-2 fields
_arg_str = json2args(task['dest'])
_parser = MyArgParser()
_parser.level_name = 'task.dest'
_parser.add_argument('--mysql')
_parser.add_argument('--database', type=str)
_parser.add_argument('--table', required=True, type=str)
_parser.add_argument('--create_table_first', choices=[0,1], type=int)
_parser.add_argument('--create_table_sql', type=str)
_args = _parser.parse_args(_arg_str)
if 'database' not in task['dest']:
    task['dest']['database'] = task['src']['database']
#check task.rule json level-2 fields
_arg_str = json2args(task['rule'])
_parser = MyArgParser()
_parser.level_name = 'task.rule'
_parser.add_argument('--filter',type=str)
_parser.add_argument('--page_size', type=int)
_parser.add_argument('--page_sleep', type=int)
_parser.add_argument('--order_by', type=str)
_parser.add_argument('--group_method', required=True, choices=['modulus','devide','all'], type=str)
_parser.add_argument('--group_base', type=int)
_parser.add_argument('--group_column', type=str)
_parser.add_argument('--group_int', type=str)
_args = _parser.parse_args(_arg_str)

group_int_list = []
if 'group_int' in task['rule']:
    for n in task['rule']['group_int']:
        if isinstance(n,int):
            group_int_list.append(n)
        elif isinstance(n,list):
            if len(n)!=2:
                raise Exception('rule.group_int list element length must be 2')
            for i in range(n[0],n[1]+1):
                group_int_list.append(i)
    if len(group_int_list)==0:
        logging.warning("rule.group_int defined, but contains no effective data. continue...")

group_func = None
group_base = None
#get the group integer for a result row with method "modulus"
def row_int_modulus(row):
    return row[db.db_group_column_index]%group_base
#get the group integer for a result row with method "devide"
def row_int_devide(row):
    return int(row[db.db_group_column_index] / group_base)
#get the group integer for a result row with method "all"
def row_int_all(row):
    return 0

def cal_group_func(group_method):
    global group_func, group_base
    if 'group_base' in task['rule']:
        group_base = int(task['rule']['group_base'])
    if group_method=='modulus':
        group_func = row_int_modulus
    elif group_method=='devide':
        group_func = row_int_devide
    elif group_method=='all':
        group_func = row_int_all
    else:
        raise Exception('unsupported method: '+group_method)
def cal_group_filter_sql(group_method, group_int_n):
    if 'group_base' in task['rule']:
        group_base = int(task['rule']['group_base'])
    if group_method=='modulus':
        return task['rule']['group_column']+'%'+str(int(task['rule']['group_base']))+'='+str(group_int_n)
    elif group_method=='devide':
        return 'floor('+task['rule']['group_column']+'/'+str(int(task['rule']['group_base']))+')='+str(group_int_n)
    elif group_method=='all':
        return '1'
def cal_group_by_sql(group_method):
    if 'group_base' in task['rule']:
        group_base = int(task['rule']['group_base'])
    if group_method=='modulus':
        return "group by "+task['rule']['group_column']+'%'+str(int(task['rule']['group_base']))
    elif group_method=='devide':
        return 'group by floor('+task['rule']['group_column']+'/'+str(int(task['rule']['group_base']))+')'
    elif group_method=='all':
        return 'group by 1'
def cal_group_field_sql(group_method):
    if 'group_base' in task['rule']:
        group_base = int(task['rule']['group_base'])
    if group_method=='modulus':
        return task['rule']['group_column']+'%'+str(int(task['rule']['group_base']))+" as n"
    elif group_method=='devide':
        return 'floor('+task['rule']['group_column']+'/'+str(int(task['rule']['group_base']))+') as n'
    elif group_method=='all':
        return '1'

def cal_new_table_name(n):
    return task['dest']['table'].replace('[n]', str(n))