import os
import smtplib
import numpy as np
import pandas as pd

from email.MIMEMultipart import MIMEMultipart
from email.MIMEText import MIMEText

from gearbox import read_sql

from ram.data.data_handler_sql import DataHandlerSQL


###########
#  EMAIL  #
###########

def send_email(msg_body, subject, toaddr="analysts@roundaboutam.com"):
    """
    Send an email with msg in the body from the notifications@roundaboutam.com
    address.
    """
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


#################
#  IMPORT DATA  #
#################

# Get handled list
fpath = os.path.join(os.getenv('GITHUB'), 'ram', 'data', 'ram_sql_tables',
                     'starmine', 'mapping', 'handled_mappings.csv')

handled = pd.read_csv(fpath)
handled = handled[['SecCode', 'SecId', 'StartDate', 'EndDate']]

# Get todays problem list
dpath = os.path.join(os.getenv('DATA'), 'ram', 'data', 'starmine')

# First, delete problem file
try:
    os.remove(os.path.join(dpath, 'PROBLEMS.csv'))
except:
    pass

problems = read_sql(os.path.join(dpath, max(os.listdir(dpath))))


########################
#  WRITE PROBLEM FILE  #
########################

# Extract SecCodes that have been handled
problems = problems[~problems.SecCode.isin(handled.SecCode)]

if len(problems) > 0:
    problems.to_csv(os.path.join(dpath, 'PROBLEMS.csv'), index=None)
    msg = "There are {} problem mappings for Starmine.".format(len(problems))
    msg += "\nLocate securities in DATA/ram/data/starmine/PROBLEMS.csv"
    send_email(msg, subject='[DATABASE] Starmine Mapping Problem')


############################
#  CHECK HANDLED SECCODES  #
############################

dh = DataHandlerSQL()

sql = "select distinct SecCode from ram.dbo.ram_starmine_map;"

seccodes = dh.sql_execute(sql)
seccodes = [x[0] for x in seccodes]

problems = handled[handled.SecCode.isin(seccodes)]
handled = handled[~handled.SecCode.isin(seccodes)]

if len(problems) > 0:
    problems.to_csv(os.path.join(dpath, 'PROBLEMS_HANDLED.csv'), index=None)
    msg = "The following handled SecCodes are already in the database:\n\n"
    msg += str(problems.SecCode.unique().tolist())
    msg += "\n\nLocate securities in DATA/ram/data/starmine/PROBLEMS_HANDLED.csv"
    send_email(msg, subject='[DATABASE] Starmine Mapping Problem - Handled SecCodes')


##################################
#  UPDATE DATABASE WITH HANDLED  #
##################################

def insert_mapping_into_db(mapping):
    dh = DataHandlerSQL()
    formatted_rows = _format_rows(mapping)
    if len(formatted_rows):
        vals = ','.join(formatted_rows)
        sql = "insert into ram.dbo.ram_starmine_map (SecCode, SecId, StartDate, EndDate) values "
        sql = sql + vals + ';'
        dh.sql_execute_no_return(sql, time_constrained=False)
    return


def _format_rows(mapping):
    output = []
    for _, x in mapping.iterrows():
        output.append("({}, {}, '{}', '{}')".format(
            x['SecCode'], x['SecId'], x['StartDate'], x['EndDate']))
    return output


insert_mapping_into_db(handled)
