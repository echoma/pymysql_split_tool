name = 'pymysql_split_tool'

__version__ = '0.5.6'

def init(action, task, debug=False):
    import input
    input.init(action, task, debug)

def init_by_cmd_line_args():
    import input
    input.init_by_cmd_line_args()

def do_work():
    import controller
    controller.do_work()
