import argparse,json,logging,sys,pymysql

parser = argparse.ArgumentParser()
parser.add_argument('--task', required=True)
parser.add_argument('--action', choices=["split","check","remove"], required=True)
parser.add_argument('--debug')
args = parser.parse_args()

logging.basicConfig(level=(logging.DEBUG if args.debug else logging.INFO), format='%(asctime)-15s: %(message)s [%(filename)s:%(lineno)s]')

task_file = open(args.task+'.json')
task = json.loads(task_file.read())
task_file.close()


sys.path.append("..")
import pymysql_split_tool
import input

input.init(args.action, task)
pymysql_split_tool.do_work()