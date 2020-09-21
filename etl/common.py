import datetime as dt

def print_complete(dsName):
    print('{} Finished! {}'.format(
        dsName, dt.datetime.now().strftime("%d-%b-%Y (%H:%M:%S.%f)")
    ))
