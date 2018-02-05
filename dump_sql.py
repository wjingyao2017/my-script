import click
import subprocess
import os
current_dictionary = os.getcwd()
dl_from_db_name = 'ptv_working'
login_host = 'panda'
local_loginpath = 'local'
import_db_name = 'ptv_working'
ssp_order_id = ''
file_extention = '.txt'
is_debug_mode = False
dump_file_folder = "dumpfiles/"
CONTEXT_SETTINGS = dict(help_option_names=['-h', '--help'])
save_to_one_file = True

@click.command(context_settings=CONTEXT_SETTINGS)
@click.argument('ssporderid_ordernumber')
@click.option('--download/--skip_download', default=True, help='whether download file from server. default true')
@click.option('--debug', is_flag=True, help='whether print all debug info.default is false')
@click.option("--incremental_import/--delete_first", default=False,
              help='whether delete corresponding ssp_order in local firstly')
@click.option('--import_file/--skip_import_file', default=True, help='whether import txt file into target database')
@click.option('--save_to_single_file/--save_to_multi_file', default=True, help='whether import txt file into one file,default true')
@click.option('--download_fsm_table/--skip_download_fsm_table', default=False, help='whether download fsm')
@click.option('--host', '-ht', '-hs', default='qa', type=click.Choice(['qa', 'dev', 'panda', 'prod']),
              help='choose enviroment to download.Default is panda')
@click.option('--to_host', '-tht', '-ths', default='local', type=click.Choice(['qa', 'dev', 'panda', 'local']),
              help='choose enviroment to save order.Default is local')
def letsgo(download, host, ssporderid_ordernumber, debug, incremental_import, import_file, to_host, download_fsm_table, save_to_single_file):
    """This script download remote ssp_order,then import into local.

       \b
       python dump_sql.py 0099384b29be7b3b78fa94b2a7f15712 \b
       python dump_sql.py 5582 --host=panda 
       python dump_sql.py 5582 --host=qa --to_host=panda
       \b
       1. need to install dependency for python framework:
          easy_install call
       2. need to persist the usr and password info by:
       use mysql_config_editor set --login-path=local --host=localhost --user=username --port=portnumber --password
       use mysql_config_editor set --login-path=qa --host=qa.xxx.com --user=username --port=portnumber --password
       and then set password in prompt
       """
    click.echo("current working dictionary:"+ current_dictionary)
    global is_debug_mode
    is_debug_mode = debug
    global save_to_one_file
    save_to_one_file = save_to_single_file
    setup_db_login(host)
    setup_import_db_login(to_host)
    
    if host == to_host :     
        errmsg = "download host:{} can not be the same with save to host:{}".format(host,to_host)
        click.echo(errmsg)
        return
   
    
    setup_ssporder_id(ssporderid_ordernumber)
    if download:
        download_from_remote(host,download_fsm_table)
        #replace_pmpid_to_local_pmpid()
    else:
        click.echo('skip download')
    if incremental_import:
        click.echo('incremental importing')
    else:
        delete_local()
    if import_file:
        import_all_to_local()
    else:
        click.echo('skip import file')


def delete_local():
    click.echo('delete ssp_order by: %s' % ssp_order_id)
    query_sql_to_local("DELETE from ssp_order_ranker where ssp_order_id='{}'".format(ssp_order_id))
    query_sql_to_local("DELETE from ssp_weekly_order where ssp_order_id='{}'".format(ssp_order_id))
    query_sql_to_local("DELETE from ssp_order where id='{}'".format(ssp_order_id))


def dump_sql_to_remote(table, sql):
    
    download_cmd = 'mysqldump --login-path={} --skip-add-drop-table -n -t  -B {} --tables {}' \
                   ' --lock-tables=false --where="{}" --skip-comments --compact '.format(login_host, dl_from_db_name, table, sql)
    if (save_to_one_file):
        filename = createFileName("SINGLE")
        download_cmd = download_cmd + " >> " + dump_file_folder + filename
    else:
        filename = createFileName(table)
        full_path_name = dump_file_folder +filename
        download_cmd = download_cmd + " >  " + full_path_name
    click.echo('download from remote: %s' % table)
    if is_debug_mode:
        click.echo('download from: %s' % download_cmd)
    subprocess.call(download_cmd, shell=True)


def download_from_remote(host,download_fsm_table):
    click.echo('download from %s' % host)

    avt_sql = "id in (select atv_campaign_id from ssp_order where id ='{}')".format(ssp_order_id)
    ssp_order_sql = "id='{}'".format(ssp_order_id)
    ssp_week_sql = "ssp_order_id = '{}'".format(ssp_order_id)
    ssp_rank_sql = "ssp_order_id = '{}'".format(ssp_order_id)

    dump_sql_to_remote('atv_campaign', avt_sql)
    dump_sql_to_remote('ssp_order', ssp_order_sql)
    dump_sql_to_remote('ssp_weekly_order', ssp_week_sql)
    dump_sql_to_remote('ssp_order_ranker', ssp_rank_sql)
    if (download_fsm_table):
        fsm_transaction_sql = "id in (select transaction_id from fsm_orders_transactions where order_id='{}')".format(ssp_order_id)
        dump_sql_to_remote('fsm_transaction', fsm_transaction_sql)

        fsm_orders_transactions_sql = "order_id = '{}'".format(ssp_order_id)
        dump_sql_to_remote('fsm_orders_transactions', fsm_orders_transactions_sql)
        
        fsm_transaction_log_sql = "transaction_id in (select a.transaction_id from fsm_orders_transactions as a where order_id='{}')".format(ssp_order_id)
        dump_sql_to_remote('fsm_transaction_log', fsm_transaction_log_sql)

def createFileName(fileName):
    return ssp_order_id + "_" + fileName + file_extention

def replace_pmpid_to_local_pmpid():
    pmp_id = query_sql_to_remote("select pmp_id from ssp_order where id='{}'".format(ssp_order_id))
    click.echo('find pmp_id in remote:[%s]' % pmp_id)
    mnemonic = query_sql_to_remote("select mnemonic from ptv_pmps where id='{}'".format(pmp_id))
    click.echo('find mnemonic in remote:[%s]' % mnemonic)

    local_pmp_id = query_sql_to_local("select id from ptv_pmps where mnemonic='{}'".format(mnemonic))
    click.echo('find pmp_id in local:[{}],by mnemonic:{}'.format(local_pmp_id, mnemonic))
    click.echo('replace pmp from {} to local:{}'.format(pmp_id, local_pmp_id))
    with open(createFileName("ssp_order"), "rt") as fin:
        with open(createFileName("ssp_order_replaced"), "wt") as fout:
            for line in fin:
                fout.write(line.replace(pmp_id, local_pmp_id))


def import_all_to_local():
    click.echo('import into local')
    if (save_to_one_file):
        import_file_to_local("SINGLE")
    else:
        import_file_to_local("atv_campaign")
        import_file_to_local("ssp_order")
        import_file_to_local("ssp_weekly_order")
        import_file_to_local("ssp_order_ranker")


def import_file_to_local(table_name):
    filename = createFileName(table_name)
    # load_sql = "load data local infile '/Users/yanyan/my-script/dumpfiles/{}' into table {};".format(filename,table_name)
    # query_id_sql = 'echo $(mysql --login-path={}  --database={} -s --local-infile --execute="{}") '.format(local_loginpath, import_db_name,load_sql)
    query_id_sql = "echo $(mysql --login-path={}  --database={} -s --local-infile < {};)".format(local_loginpath, import_db_name, current_dictionary + "/" + dump_file_folder + filename)

    # query_id_sql = "mysql --login-path={} --database={} -e 'load data local infile '/Users/yanyan/my-script/{}'' ".format(local_loginpath, import_db_name, filename)
    # query_id_sql = 'mysqlimport --login-path={}  -L {}  {}'.format(local_loginpath, import_db_name, filename)

    click.echo('import {} to {} '.format(filename,local_loginpath))
    if is_debug_mode:
        click.echo('import to local %s' % query_id_sql)
    subprocess.call([query_id_sql], shell=True)


def query_sql_to_local(sql):
    return execute_query(local_loginpath, import_db_name, sql)


def query_sql_to_remote(sql):
    return execute_query(login_host, dl_from_db_name, sql)


def execute_query(logpath, database, sql):
    query_id_sql = 'echo $(mysql --login-path={} --database={} -s --execute="{}") '.format(logpath, database, sql)
    if is_debug_mode:
        click.echo('using sql to query:%s' % query_id_sql)
    val = subprocess.check_output([query_id_sql], shell=True)
    return val[:-1]


def setup_ssporder_id(ssporderid):
    global ssp_order_id
    ssp_order_id = ssporderid

    if len(ssporderid) > 5:
        click.echo('using ssp_order_id:[%s]' % ssp_order_id)
    else:
        sql = "select id from ssp_order where order_number={}".format(ssporderid)
        ssp_order_id = query_sql_to_remote(sql)
        click.echo('using order_number to query ssp_order_id:[%s]' % ssp_order_id)


def setup_db_login(host):
    global login_host
    login_host = host
    click.echo('download from %s!' % host)
def setup_import_db_login(host):
    global local_loginpath
    local_loginpath = host
    click.echo('import to %s!' % host)

if __name__ == '__main__':
   letsgo()
