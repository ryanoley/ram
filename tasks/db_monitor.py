import os
import datetime as dt
import numpy as np
import pypyodbc
import smtplib
from email.MIMEMultipart import MIMEMultipart
from email.MIMEText import MIMEText


def update_qad_monitor(cursor):
    '''
    Run sql script to retrieve most recent QAD update values and write
    them to qad_monitor.
    '''
    db_update_path = os.path.join(os.getenv('GITHUB'), 'ram', 'ram', 'data',
                                  'qad', 'qad_monitor.sql')
    with open(db_update_path, 'r') as sqlfile:
        sqlscript = sqlfile.read()
    cursor.execute(sqlscript)
    cursor.commit()
    return

def update_ram_monitor(cursor):
    '''
    Run sql script to retrieve most recent RAM table values and write
    them to table_monitor.
    '''
    db_update_path = os.path.join(os.getenv('GITHUB'), 'ram', 'ram', 'data',
                                  'qad', 'table_monitor.sql')
    with open(db_update_path, 'r') as sqlfile:
        sqlscript = sqlfile.read()
    cursor.execute(sqlscript)
    cursor.commit()
    return

def _get_prior_buisiness_date(cursor):
    '''
    Get prior business date from today using trading_dates table
    '''
    today = dt.date.today()
    SQLCommandDate = ("select distinct(T0) from ram.dbo.trading_dates")
    cursor.execute(SQLCommandDate)
    sqlres = cursor.fetchall()
    business_dates = np.array([x[0].date() for x in sqlres])
    business_dates.sort()
    prior_biz_dt = max(business_dates[business_dates < today])
    return prior_biz_dt


def _poll_qad_monitor(cursor):
    '''
    Select QAD related update information from the qad_monitor table.
    '''
    SQLCommandSys = ("select top 1 UPDDate, UPDNumber "
                     "from ram.dbo.qad_monitor "
                     "where TableName = 'MQASys' "
                     "order by MonitorDate desc")
    cursor.execute(SQLCommandSys)
    sqlres2 = cursor.fetchall()
    sys_upd_dt = sqlres2[0][0].date()
    sys_upd_num = sqlres2[0][1]

    SQLCommandSys = ("select top 1 UPDDate, UPDNumber, StartTime, EndTime "
                     "from ram.dbo.qad_monitor "
                     "where TableName = 'QADLog' "
                     "order by MonitorDate desc")
    cursor.execute(SQLCommandSys)
    sqlres3 = cursor.fetchall()
    log_upd_dt = sqlres3[0][0].date()
    log_upd_num = sqlres3[0][1]
    log_tstart = sqlres3[0][2]
    log_tend = sqlres3[0][3]

    return {'SysUpdDt': sys_upd_dt, 'SysUpdNum': sys_upd_num,
            'LogUpdDt': log_upd_dt, 'LogUpdNum': log_upd_num,
            'LogUpdStart': log_tstart, 'LogUpdEnd': log_tend}


def _poll_table_monitor(cursor):
    '''
    Select RAM related update information from the table_monitor table.
    '''
    SQLCommandRAM = ("select TableName, MaxTableDate "
                     "from ram.dbo.table_monitor "
                     "where MonitorDateTime = ("
                     "select max(MonitorDateTime) "
                     "from ram.dbo.table_monitor)")
    cursor.execute(SQLCommandRAM)
    sqlres4 = cursor.fetchall()
    tables = [x[0] for x in sqlres4]
    up_dts = [x[1].date() for x in sqlres4]
    return {'RAMTables': tables, 'RAMUpdDts': up_dts}


def _build_logging_msg(prior_bdate, qad, ram):
    '''
    Compile string that can be written to log and/or emailed as an alert
    notification.
    '''
    today = dt.date.today()
    last_upd_diff = int((dt.datetime.now() - qad['LogUpdEnd']
                         ).total_seconds() / 60)
    status_str = ('Today: {0}'
                  '\nPrior Business Date: {1}'
                  '\nQAD Last Update: {2}'
                  '\n\tUPD #: {3}'
                  '\nCurrent UPD Date: {4}'
                  '\n\tUPD #: {5}'
                  '\n\tStarted: {6}'
                  '\n\tEnded: {7}'
                  '\n\tProcessed min ago: {8}'
                  '\nRAM Tables:').format(today, prior_bdate, qad['SysUpdDt'],
                                          qad['SysUpdNum'], qad['LogUpdDt'],
                                          qad['LogUpdNum'], qad['LogUpdStart'],
                                          qad['LogUpdEnd'], last_upd_diff)

    for (x, y) in zip(ram['RAMTables'], ram['RAMUpdDts']):
        status_str += ('\n\t{0}: {1}'.format(x, y))

    return status_str


def send_email(msg_body, subject, toaddr="ryan@roundaboutam.com"):
    '''
    Send an email with msg in the body from the notifications@roundaboutam.com
    address.
    '''
    fromaddr = "notifications@roundaboutam.com"
    passwd = "ramalerts"

    msg = MIMEMultipart()
    msg['From'] = fromaddr
    msg['To'] = toaddr
    msg['Subject'] = subject

    body = msg_body
    msg.attach(MIMEText(body, 'plain'))
    server = smtplib.SMTP('smtp.gmail.com', 587)
    server.starttls()
    server.login(fromaddr, passwd)
    text = msg.as_string()
    server.sendmail(fromaddr, toaddr, text)
    server.quit()
    return


def main():

    import argparse

    ###########################################################################
    #   Command line arguments
    ###########################################################################
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-update_db_stats', '--update_db_stats', action='store_true',
        help='Update values in qad_monitor table in database')
    parser.add_argument(
        '-check_qad', '--check_qad', action='store_true',
        help='Verify status of QAD updates and tables')
    parser.add_argument(
        '-check_ram', '--check_ram', action='store_true',
        help='Verify status of RAM core tables')
    parser.add_argument(
        '-log_to_console', '--log_to_console', action='store_true',
        help='Write output to log file. If not, output will write to console')
    parser.add_argument(
        '-log_path', '--log_path',
        default=os.path.join(os.getenv('DATA'), 'log', 'qad_ram_db.log'),
        help='default log directory')
    args = parser.parse_args()

    ##################################
    # UPDATE QAD TABLE/GET TABLE STATS
    #################################
    connection = pypyodbc.connect('Driver={SQL Server};Server=QADIRECT;'
                                  'Database=ram;uid=ramuser;pwd=183madison')
    cursor = connection.cursor()

    if args.update_db_stats:
        update_qad_monitor(cursor)
        update_ram_monitor(cursor)

    prior_bdate = _get_prior_buisiness_date(cursor)
    qad_status = _poll_qad_monitor(cursor)
    ram_status = _poll_table_monitor(cursor)
    connection.close()

    ##################
    # STATUS CHECKS
    ##################
    # QAD tables updated
    if args.check_qad:
        qad_chk1 = qad_status['SysUpdDt'] >= prior_bdate
        # QAD processing updates
        prior_upd_lim = 90
        last_upd_diff = int((dt.datetime.now() - qad_status['LogUpdEnd']
                             ).total_seconds() / 60)
        qad_chk2 = last_upd_diff <= prior_upd_lim
    else:
        qad_chk1, qad_chk2 = True, True
    # RAM tables udpated
    if args.check_ram:
        check_tables = ['univ_filter_data_etf', 'univ_filter_data',
                        'sm_SmartEstimate_eps', 'sm_ShortInterest', 'sm_ARM',
                        'ram_sector', 'ram_master_etf', 'ram_master_equities',
                        'pead_event_dates_live', 'ern_event_dates_live']
        ram_chk = np.all([y >= prior_bdate for (x, y) in
                          zip(ram_status['RAMTables'], ram_status['RAMUpdDts'])
                          if x in check_tables])
    else:
        ram_chk = True

    #####################
    # EMAIL ALERT AND LOG
    #####################
    log_str = _build_logging_msg(prior_bdate, qad_status, ram_status)

    if ~np.all([qad_chk1, qad_chk2, ram_chk]):
        subject = "** RAM/QAD Database ALERT {0} **".format(dt.date.today())
        send_email(log_str, subject)

    if args.log_to_console:
        print log_str
    else:
        # Write log_str to log file
        with open(args.log_path, 'a') as file:
            file.write('\n \n----------------------\n')
            file.write(str(dt.datetime.now()))
            file.write('\n' + log_str)


if __name__ == '__main__':
    try:
        main()
    except Exception, e:
        with open('./RAMErrorLog.log', 'a') as file:
            file.write('\n \n----------------------\n')
            file.write(str(dt.datetime.now()))
            file.write('\n' + str(e))
        subject = "** RAM/QAD Database Alert {0} **".format(dt.date.today())
        send_email(str(e), subject)
