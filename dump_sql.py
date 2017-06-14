import click
import subprocess

dl_from_db_name = 'ptv_working'
login_host = 'panda'
local_loginpath = 'local'
import_db_name = 'ptv_working'
ssp_order_id = ''

is_debug_mode = False

CONTEXT_SETTINGS = dict(help_option_names=['-h', '--help'])


@click.command(context_settings=CONTEXT_SETTINGS)
@click.argument('ssporderid_ordernumber')
@click.option('--download/--skip_download', default=True, help='whether download file from server. default true')
@click.option('--debug', is_flag=True, help='whether print all debug info.default is false')
@click.option("--incremental_import/--delete_first", default=True,
              help='whether delete corresponding ssp_order in local firstly')
@click.option('--import_file/--skip_import_file', default=True, help='whether import txt file into local database')
@click.option('--host', '-ht', '-hs', default='qa', type=click.Choice(['qa', 'dev', 'panda', 'prod']),
              help='choose enviroment.default is panda')
def letsgo(download, host, ssporderid_ordernumber, debug, incremental, import_file):
    """This script download remote ssp_order,then import into local.

       \b
       python dump_sql.py 0099384b29be7b3b78fa94b2a7f15712 \b
       python dump_sql.py 5582 --host=panda

       \b
       use mysql_config_editor set --login-path=local --host=localhost --user=username --port=portnumber --password
       and then set password in prompt
       """
    global is_debug_mode
    is_debug_mode = debug
    setup_db_login(host)

    setup_ssporder_id(ssporderid_ordernumber)
    if download:
        download_from_remote(host)
        replace_pmpid_to_local_pmpid()
    else:
        click.echo('skip download')
    if incremental:
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
    filename = table + '.txt'
    download_cmd = 'mysqldump --login-path={} --skip-add-drop-table -n -t  -B {} --tables {}' \
                   ' --lock-tables=false --where="{}" > {}'.format(login_host, dl_from_db_name, table, sql, filename)
    click.echo('download from remote: %s' % table)
    if is_debug_mode:
        click.echo('download from: %s' % download_cmd)
    subprocess.call(download_cmd, shell=True)


def download_from_remote(host):
    click.echo('download from %s' % host)

    avt_sql = "id in (select atv_campaign_id from ssp_order where id ='{}')".format(ssp_order_id)
    ssp_order_sql = "id='{}'".format(ssp_order_id)
    ssp_week_sql = "ssp_order_id = '{}'".format(ssp_order_id)
    ssp_rank_sql = "ssp_order_id = '{}'".format(ssp_order_id)

    dump_sql_to_remote('atv_campaign', avt_sql)
    dump_sql_to_remote('ssp_order', ssp_order_sql)
    dump_sql_to_remote('ssp_weekly_order', ssp_week_sql)
    dump_sql_to_remote('ssp_order_ranker', ssp_rank_sql)


def replace_pmpid_to_local_pmpid():
    pmp_id = query_sql_to_remote("select pmp_id from ssp_order where id='{}'".format(ssp_order_id))
    click.echo('find pmp_id in remote:[%s]' % pmp_id)
    mnemonic = query_sql_to_remote("select mnemonic from ptv_pmps where id='{}'".format(pmp_id))
    click.echo('find mnemonic in remote:[%s]' % mnemonic)

    local_pmp_id = query_sql_to_local("select id from ptv_pmps where mnemonic='{}'".format(mnemonic))
    click.echo('find pmp_id in local:[{}],by mnemonic:{}'.format(local_pmp_id, mnemonic))
    click.echo('replace pmp from {} to local:{}'.format(pmp_id, local_pmp_id))
    with open("ssp_order.txt", "rt") as fin:
        with open("ssp_order_replaced.txt", "wt") as fout:
            for line in fin:
                fout.write(line.replace(pmp_id, local_pmp_id))


def import_all_to_local():
    click.echo('import into local')
    import_file_to_local("atv_campaign.txt")
    import_file_to_local("ssp_order_replaced.txt")
    import_file_to_local("ssp_weekly_order.txt")
    import_file_to_local("ssp_order_ranker.txt")


def import_file_to_local(filename):
    query_id_sql = 'mysql --login-path={} --database={} -s < {}'.format(local_loginpath, import_db_name, filename)
    click.echo('import %s to local ' % filename)
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
    click.echo('host is %s!' % host)


if __name__ == '__main__':
    letsgo()
