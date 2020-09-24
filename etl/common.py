import datetime as dt

def print_complete(s):
    print(f'{s}: {dt.datetime.now().isoformat(sep=" ", timespec="seconds")}')

def headText(s):
    return s.center(80, '-')
