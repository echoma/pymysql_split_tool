'''
Parse input args, parse task file
'''


import argparse,json,logging,sys
import db


'''Variables whose values will be set by the following init() or init_by_cmd_line_args() function.'''

action = None   #what action to do
task = None     #task infomation
group_int_list = []     #group integer list of task

''' Functions for initialization

By default, if no comment was made, all functions in this file will raise an exception if any error occurred
'''

def init(param_action, param_task, param_debug=False):
    '''Basical initialization according to the given parameters
    
    @param param_action -- a str whose value is one of ['split','check','remove']
    @param param_task   -- a dict containing the task infomation
    @param param_debug  -- a boolean whether to print debug log
    '''
    global action, task
    if param_action not in ['split','check','remove']:
        raise Exception('Invalid "action" value')
    action = param_action
    if not isinstance(param_task, dict):
        raise Exception('Invalid "task", should be a dict')
    task = param_task
    _init_logging(logging.DEBUG if param_debug else logging.INFO)
    _check_task()
    _init_group_func()
    
def init_by_cmd_line_args():
    '''Parse command line arguments and do basical initialization'''
    global action, task
    _parser = argparse.ArgumentParser()
    _parser.add_argument('--action', choices=['split','check','remove'], required=True, help='type of task')
    _parser.add_argument('--task', required=True, help='path to task file')
    _parser.add_argument('--debug', action='store_true', help='print debug logs')
    _args = _parser.parse_args()
    action = _args.action
    _task_file = open(_args.task)
    task = json.loads(_task_file.read())
    _task_file.close()
    _init_logging(logging.DEBUG if _args.debug else logging.INFO)
    _check_task()
    _init_group_func()
    
def _init_logging(level):
    '''Initialize logging'''
    logging.basicConfig(level=level, format='%(asctime)-15s: %(message)s [%(filename)s:%(lineno)s]')
    
def _check_task():
    '''Check task infomation'''
    global task
    #check task json level-1 fields
    _parser = MyArgParser()
    _parser.add_argument('--src', required=True)
    _parser.add_argument('--dest', required=True)
    _parser.add_argument('--rule', required=True)
    _parser.add_argument('--check', required=(action=='check'))
    _parser.parse_dict(task, 'task file')
    #check task.src json level-2 fields
    _parser = MyArgParser()
    _parser.add_argument('--mysql', required=True)
    _parser.add_argument('--database', required=True, type=str)
    _parser.add_argument('--table', required=True, type=str)
    _parser.parse_dict(task['src'], 'task.src')
    #check task.dest json level-2 fields
    _parser = MyArgParser()
    _parser.add_argument('--mysql')
    _parser.add_argument('--database', type=str)
    _parser.add_argument('--table', required=True, type=str)
    _parser.add_argument('--create_table_first', choices=[0,1], type=int)
    _parser.add_argument('--create_table_sql', type=str)
    _args = _parser.parse_dict(task['dest'], 'task.dest')
    if 'database' not in task['dest']:
        task['dest']['database'] = task['src']['database']
    #check task.rule json level-2 fields
    _parser = MyArgParser()
    _parser.add_argument('--filter',type=str)
    _parser.add_argument('--page_size', type=int)
    _parser.add_argument('--page_sleep', type=int)
    _parser.add_argument('--order_by', type=str)
    _parser.add_argument('--group_method', required=True, choices=['modulus','devide','all'], type=str)
    _parser.add_argument('--group_base', type=int)
    _parser.add_argument('--group_column', type=str)
    _parser.add_argument('--group_int', type=str)
    _parser.parse_dict(task['rule'], 'task.rule')
    global group_int_list
    group_int_list[:] = []
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

class MyArgParser(argparse.ArgumentParser):
    '''Our argument parser
    
    Differences from default parser:
    1. It will print different message with default when error occurs.
    2. It can parse a dict object
    '''
    
    _level_name = ''
    
    def parse_dict(self, jsondict, level_name):
        '''Parse a dict as arguments list'''
        self._level_name = level_name
        arg_str = self.json2args(jsondict)
        return self.parse_args(arg_str)
        
    def error(self, message):
        '''Override default error behivior'''
        logging.error('In '+self._level_name+', '+message.replace('argument', 'field').replace('--',''))
        exit(-1)
    
    def json2args(self, jsondict):
        '''Convert a dict into arguments list'''
        ret = []
        for key in jsondict:
            ret.append('--'+key)
            if not isinstance(jsondict[key], dict):
                ret.append(str(jsondict[key]))
            else:
                ret.append('@dict')
        return ret


'''Variables for group integer calculation'''

group_func = None   #function to calculate the group integer for a record
_group_base = None  #cached value of the base number used in for group method
_group_method = None    #cached value of the group method string
_group_column = None    #cached value of the group cloumn name
_new_table_pat = None   #cached value of the new table pattern

'''Functions for gruop integer calculation'''

def _row_int_modulus(row):
    '''Get the group integer for a result row with method "modulus"'''
    return row[db.db_group_column_index]%_group_base

def _row_int_devide(row):
    '''Get the group integer for a result row with method "devide"'''
    return int(row[db.db_group_column_index] / _group_base)

def _row_int_all(row):
    '''Get the group integer for a result row with method "all"'''
    return 0

def _init_group_func():
    '''Initialize the grouping function according to task infomation'''
    global group_func, _group_base, _group_method, _group_column, _new_table_pat
    _group_method = task['rule']['group_method']
    if 'group_base' in task['rule']:
        _group_base = int(task['rule']['group_base'])
    if 'group_column' in task['rule']:
        _group_column = task['rule']['group_column']
    if _group_method=='modulus':
        group_func = _row_int_modulus
    elif _group_method=='devide':
        group_func = _row_int_devide
    elif _group_method=='all':
        group_func = _row_int_all
    else:
        raise Exception('unsupported method: '+str(_group_method))
    _new_table_pat = task['dest']['table']
        
def compose_group_filter_sql(group_int_n):
    '''return a sql-where-clause for filtering records'''
    if _group_method=='modulus':
        return _group_column+'%'+str(_group_base)+'='+str(group_int_n)
    elif _group_method=='devide':
        return 'floor('+_group_column+'/'+str(_group_base)+')='+str(group_int_n)
    elif _group_method=='all':
        return '1'
def compose_group_by_sql():
    '''return a sql-group-by-clause'''
    if _group_method=='modulus':
        return "group by "+_group_column+'%'+str(_group_base)
    elif _group_method=='devide':
        return 'group by floor('+_group_column+'/'+str(_group_base)+')'
    elif _group_method=='all':
        return 'group by 1'
def compose_group_field_sql():
    '''return a sql-select-filed-clause'''
    if _group_method=='modulus':
        return _group_column+'%'+str(_group_base)+" as n"
    elif _group_method=='devide':
        return 'floor('+_group_column+'/'+str(_group_base)+') as n'
    elif _group_method=='all':
        return '1'

def compose_new_table_name(group_int_n):
    '''return the new table name according to the given group integer'''
    return _new_table_pat.replace('[n]', str(group_int_n))