"""this module conditions and converts the data into different
formats such as from google sheets to sqlite
"""
#-----------------------------------------------------------
#dependencis
#-----------------------------------------------------------
import datetime as dt
import pandas as pd
from sqlgsheet import database as db

#-----------------------------------------------------------
#dynamic variables
#-----------------------------------------------------------
DATASETS = {}

#-----------------------------------------------------------
#load
#-----------------------------------------------------------
def update():
    global DATASETS
    db.load()
    gs_data = db.get_sheet('sleep', 'events')
    sql_data = format_events(gs_data)
    db.update_table(sql_data, 'sleep', False)
    DATASETS['events'] = sql_data


def format_events(gs_data):
    time_format = db.GSHEET_CONFIG['sleep']['sheets']['events']['time_format']
    sql_data = gs_data.copy()
    sql_data['date'] = sql_data['date'].apply(lambda x: x.date())
    sql_data['time'] = sql_data['time'].apply(
        lambda x: dt.datetime.strptime(x, time_format))
    return sql_data



# -----------------------------------------------------
# Command line
# -----------------------------------------------------
if __name__ == "__main__":
    update()
