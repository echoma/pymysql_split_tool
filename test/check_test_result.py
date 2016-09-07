import argparse,json,logging,sys,pymysql

parser = argparse.ArgumentParser()
parser.add_argument('--case', required=True)
parser.add_argument('--debug')
args = parser.parse_args()
case_name = args.case

logging.basicConfig(level=(logging.DEBUG if args.debug else logging.INFO), format='%(asctime)-15s: %(message)s [%(filename)s:%(lineno)s]')

result_file = open(case_name+'_result.json')
result = json.loads(result_file.read())
result_file.close()

db = pymysql.connect(host='127.0.0.1', port=3306, user='test', password='test', database='test')
cursor = db.cursor()

def check_tables(result_tables):
    #tables list must match
    real_tables = []
    sql = 'show tables like "test_%"'
    cursor.execute(sql)
    for row in cursor.fetchall():
        real_tables.append(row[0])
    logging.debug("real tables: "+str(real_tables))
    logging.debug("good result: "+str(result_tables.keys()))
    for name in result_tables.keys():
        if name in real_tables:
            real_tables.remove(name)
        else:
            raise Exception('The table '+name+' should not exist')
    if len(real_tables)>0:
        raise Exception('The following tables(s) not found: '+str(real_tables))
    #table checksum and record count must match
    for name in result_tables.keys():
        cursor.execute('checksum table '+name)
        row = cursor.fetchone()
        real_checksum = int(row[1])
        cursor.execute('select count(*) from '+name)
        row = cursor.fetchone()
        real_count = int(row[0])
        good_result = result_tables[name]
        good_checksum = good_result['checksum']
        good_count = good_result['count']
        logging.debug('real checksum='+str(real_checksum)+', count='+str(real_count))
        logging.debug('good checksum='+str(good_checksum)+', count='+str(good_count))
        if real_checksum!=good_checksum:
            raise Exception('The table '+name+' checksum('+str(real_checksum)+') not good('+str(good_checksum)+')')
        if real_count!=good_count:
            raise Exception('The table '+name+' count('+str(real_count)+') not good('+str(good_count)+')')

if 'tables' in result:
    check_tables(result['tables'])

logging.info("Sucess");