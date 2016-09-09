import input
import controller

__version__ = '0.5.5'

def init(action, task, debug=False):
    input.init(action, task, debug)

def init_by_cmd_line_args():
    input.init_by_cmd_line_args()

def do_work():
    controller.do_work()
