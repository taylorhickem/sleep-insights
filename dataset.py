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
    time_format = db.GSHEET_CONFIG['sleep']['sheets']['dataset']['time_format']

    gs_data = db.get_sheet('sleep', 'dataset')
    gs_data['date'] = gs_data['date'].apply(lambda x: x.date())
    gs_data['time'] = gs_data['time'].apply(
        lambda x: dt.datetime.strptime(x, time_format))
    db.update_table(gs_data, 'sleep', False)
    DATASETS['gs_data'] = gs_data

# -----------------------------------------------------
# Command line
# -----------------------------------------------------
if __name__ == "__main__":
    update()
