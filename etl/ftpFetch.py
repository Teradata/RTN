import datetime as dt
import ftplib
from os.path import abspath, dirname

import pandas as pd
import teradatasql as td

import params
from CUST_RTN_ETL_STG_TO_CORE import stgToCore

outputDir = rf'{dirname(abspath(__file__))}\..\data'
files = []


try:
    FTP = params.FTP
except AttributeError:
    FTP = ftplib.FTP


def isoNowTime():
    return dt.datetime.now().isoformat(sep=' ', timespec='seconds')


def printUpd(s):
    print(f'{s}: {isoNowTime()}')


def cleanAndAppend(line):
    if line.endswith('.txt'):
        files.append(line[line.rindex(' ')+1:])


with FTP('ftp.teradata.com', user=params.ftpUsr, passwd=params.ftpPwd) as ftp:
    ftp.cwd('xfer')
    ftp.dir(cleanAndAppend)

    for csv in files:
        with open(rf'{outputDir}\{csv}', 'wb') as f:
            ftp.retrbinary(f'RETR {csv}', f.write)
        printUpd(f'{csv} Downloaded')

input('Please connect to VPN and press enter.')

with td.connect(
    host=params.MyHost,
    user=params.MyUser,
    password=params.Password,
    logmech=params.LogMech
) as con:
    for fname in files:
        tbl = f'{params.SchemaName}.{fname[:fname.rindex(".")]}'
        with open(rf'{outputDir}\{fname}') as f:
            data = [
                [
                    None if not value.strip('"\n') else value.strip('"\n')
                    for value in row.split('|')
                ] for row in f.readlines()
            ]
        header = data.pop(0)
        colList = ','.join(f'"{col}"' for col in header)
        paramList = ','.join(['?'] * len(header))

        flSetupStmnt =  "{fn teradata_nativesql}{fn teradata_autocommit_off}"
        delStmnt = f'delete from {tbl}'
        insStmnt = (
            f'{"{fn teradata_try_fastload}" if len(data) > 1e6 else ""}'
            f'insert into {tbl} ({colList}) values ({paramList})'
        )
        with con.cursor() as cur:
            cur.execute(flSetupStmnt)
            cur.execute(delStmnt)
            cur.execute(insStmnt, data)
            con.commit()
        printUpd(f'{fname} Inserted')
stgToCore()
