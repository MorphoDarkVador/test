import os
import re
from datetime import datetime, timedelta
from time import time
import numpy as np
import pandas as pd
import morpho_toolbox as mpt

def read_calibrated_files(folderpath, substructure, start_dt, stop_dt, delta=1):

    if stop_dt < start_dt:
        raise Exception("stop_dt < start_dt")

    # list files in folder
    # in period
    # substructure
    filepath_list = list_files_in_folder(folderpath, substructure, start_dt, stop_dt, delta=delta)

    # check continuity
    if check_continuity(filepath_list) is False:
        raise Exception("problem of continuity")

    # read files
    df_list = []
    for filepath in filepath_list:
        df = read_calibrated_file(filepath)
        df_list.append(df)

    # concatenate files
    df = pd.concat(df_list, axis=0)
    # remove duplicate index if any
    bool_serie = df.index.duplicated(keep="first")
    df = df[~bool_serie]

    # select data on period
    df = df.iloc[((df.index >= start_dt) & (df.index < stop_dt))]

    return df

def read_calibrated_file(filepath, date_format = "%Y-%m-%dT%H:%M:%S.%fZ"):

    custom_date_parser = lambda x: datetime.strptime(x, date_format)
    df = pd.read_csv(filepath, index_col=0, delimiter=",", decimal=".", parse_dates=["time"],
                     date_parser=custom_date_parser)

    return df

def check_continuity(filepath_list):

    dt_list = []
    for filepath in filepath_list:
        filename = os.path.basename(filepath)
        m = re.search("(?P<year>\d{4})_(?P<month>\d{2})_(?P<day>\d{2})_(?P<hour>\d{2})h(?P<minute>\d{2})m", filename)
        if m is not None:
            year = int(m["year"])
            month = int(m["month"])
            day = int(m["day"])
            hour = int(m["hour"])
            minute = int(m["minute"])

            dt = datetime(year, month, day, hour, minute)
            dt_list.append(dt)

    dt_array = np.array(dt_list)
    delta_array = np.diff(dt_array)
    delta_min = np.min(delta_array)
    indexes = np.argwhere(delta_array > delta_min)
    if len(indexes) > 0:
        indexes = indexes[0, :]
        return False

    return True

def list_files_in_folder(folderpath, substructure, start_dt, stop_dt, delta=1):

    filepath_list = []

    delta_dt = timedelta(minutes=delta)

    folder_filename_list = sorted(os.listdir(folderpath))
    for filename in folder_filename_list:

        if os.path.isdir(mpt.pjoin(folderpath, filename)) is True and filename not in ['.', '..']:
            m = re.search("(?P<year>\d{4})_(?P<month>\d{2})_(?P<day>\d{2})",
                          filename)
            if m is not None:
                year = int(m["year"])
                month = int(m["month"])
                day = int(m["day"])
                subfolder_dt = datetime(year, month, day)
                subfolder_start_dt = subfolder_dt - delta_dt
                subfolder_stop_dt = subfolder_dt + timedelta(days=1) + delta_dt
                if (subfolder_start_dt < start_dt and subfolder_stop_dt > stop_dt) or (subfolder_start_dt >= start_dt and subfolder_start_dt < stop_dt) or (subfolder_stop_dt > start_dt and subfolder_stop_dt <= stop_dt):
                    subfolder_filepath_list = list_files_in_folder(mpt.pjoin(folderpath, filename), substructure, start_dt, stop_dt, delta=delta)
                    filepath_list.extend(subfolder_filepath_list)
        else:
            m = re.search("_(?P<duration>\d{1,2})M_(?P<year>\d{4})_(?P<month>\d{2})_(?P<day>\d{2})_(?P<hour>\d{2})h(?P<minute>\d{2})m_(?P<substructure>\d)_CALIBRATED.csv$",
                          filename)
            if m is not None:
                duration = int(m["duration"])
                ss = int(m["substructure"])

                year = int(m["year"])
                month = int(m["month"])
                day = int(m["day"])
                hour = int(m["hour"])
                minute = int(m["minute"])

                file_dt = datetime(year, month, day, hour, minute)
                file_start_dt = file_dt - delta_dt
                file_stop_dt = file_dt + timedelta(minutes=duration) + delta_dt

                if ((file_start_dt < start_dt and file_stop_dt > stop_dt) or (file_start_dt >= start_dt and file_start_dt < stop_dt) or (file_stop_dt > start_dt and file_stop_dt <= stop_dt))\
                    and ss == substructure:
                    filepath_list.append(mpt.pjoin(folderpath, filename))
                elif file_start_dt > stop_dt:
                    break

    return filepath_list

if __name__ == "__main__":

    folderpath = r"C:\Users\md104209\Desktop\Nouveau dossier"
    substructure = 0
    start_dt = datetime(2023,12,15,10,5)
    stop_dt = datetime(2023,12,15,10,10)

    df = read_calibrated_files(folderpath, substructure, start_dt, stop_dt, delta=0)

    # filename
    duration = int((stop_dt - start_dt).total_seconds()/60)
    date_str = start_dt.strftime("%Y_%m_%d_%Hh%Mm")
    filename = f"bidon_{duration}M_{date_str}_{substructure}_CALIBRATED.csv"

    df.to_csv(filename, float_format = "%.8e",date_format="%Y-%m-%dT%H:%M:%S.%fZ")
