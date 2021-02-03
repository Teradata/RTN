import csv
import ftplib
import time
from os.path import abspath, dirname
from subprocess import run

import schedule as skd
import teradatasql as td

import params
from common import print_complete, headText
from CUST_RTN_ETL_STG_TO_CORE import stgToCore

outputDir = rf'{dirname(abspath(__file__))}\..\data\ftp'


try:
    FTP = params.FTP
except AttributeError:
    FTP = ftplib.FTP


def ftpMain():
    files = []
    print(headText('Downloading from FTP'))
    with FTP() as ftp:
        ftp.connect(params.ftpPxy, params.ftpPrt)
        ftp.login(f'{params.ftpUsr}@ftp.teradata.com', params.ftpPwd)
        ftp.cwd('xfer')

        def cleanAndAppend(fname):
            if fname.endswith('.txt') and '-' not in fname:
                files.append(fname)
        ftp.retrlines('NLST', cleanAndAppend)

        for fname in files:
            with open(rf'{outputDir}\{fname}', 'wb') as f:
                def writeLine(data):
                    for line in data.split(b'\n'):
                        if not line.isascii():
                            line = b''.join(
                                bytes([c]) for c in line if bytes([c]).isascii()
                            )
                        f.write(line)
                        f.write(b'\n')
                ftp.retrbinary(f'RETR {fname}', writeLine)
            print_complete(f'{fname} Downloaded')

    print(f'\n\n{headText("Uploading to TD")}')
    with td.connect(
        host=params.MyHost,
        user=params.MyUser,
        password=params.Password,
        logmech=params.LogMech
    ) as con:
        for fname in files:
            tbl = f'{params.SchemaName}.{fname[:fname.rindex(".")]}'
            with open(rf'{outputDir}\{fname}', newline='') as f:
                data = [
                    [None if not item else item for item in row]
                    for row in csv.reader(f, delimiter='|')
                    if row
                ]
            header = data.pop(0)
            colList = ','.join(f'"{col}"' for col in header)
            paramList = ','.join(['?'] * len(header))

            flSetupStmnt = "{fn teradata_nativesql}{fn teradata_autocommit_off}"
            delStmnt = f'delete from {tbl}'
            insStmnt = (
                f'{"{fn teradata_try_fastload}" if len(data) > 1e6 else ""}'
                f'insert into {tbl} ({colList}) values ({paramList})'
            )
            try:
                with con.cursor() as cur:
                    cur.execute(flSetupStmnt)
                    cur.execute(delStmnt)
                    cur.execute(insStmnt, data)
                    con.commit()
                print_complete(f'{fname} Inserted')
            except td.OperationalError as e:
                if 'does not exist' in str(e):
                    print_complete(f'--> {fname} Skipped')
                else:
                    raise e

    print(f'\n\n{headText("Running Transformation Procedures")}')
    stgToCore()

if __name__ == "__main__":
    run('title TD Covid Resiliency ETL', shell=True)
    print('Waiting for TD Covid Resiliency ETL to begin...')

    skd.every().day.at('15:05').do(ftpMain)
    while True:
        skd.run_pending()
        time.sleep(skd.idle_seconds())
