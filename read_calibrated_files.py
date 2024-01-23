import os
import re
from datetime import datetime, timedelta
from time import time
import numpy as np
import pandas as pd
import morpho_toolbox as mpt

def read_calibrated_files_task(date_dt, input_folderpath, output_folderpath, substructure, delta=1, root_filename=""):

    df = read_calibrated_files(input_folderpath, substructure, date_dt[0], date_dt[1], delta=delta, root_filename=root_filename)

    duration = int((date_dt[1] - date_dt[0]).total_seconds()/60)
    date_str = date_dt[0].strftime("%Y_%m_%d_%Hh%Mm")
    if root_filename == "":
        root_filename = "file"
    filename = f"{root_filename}_{duration}M_{date_str}_{substructure}_CALIBRATED.csv"

    df.to_csv(mpt.pjoin(output_folderpath, filename), float_format = "%.8e",date_format="%Y-%m-%dT%H:%M:%S.%fZ")


def read_calibrated_files(folderpath, substructure, start_dt, stop_dt, delta=1, root_filename=""):
    """
    lecture et concaténation de fichiers de données calibrées compris entre les dates start_dt et stop_dt

    exemple nom fichier = "mpsintegrationcontinue_1M_2024_01_01_00h00m_0_CALIBRATED.csv
    mpsintegrationcontinue -> root_filename
    0 -> substructure

    les fichiers peuvent être organisés en sous-dossiers (plus efficace lorsque le nombre de fichiers est important)

    exemple nom sous-dossier = "2024_01_01"

    le paramètre delta (marge) permet de gérer les éventuels décalages entre le timestamp du fichier et les timestamps des données dans le fichier

    df = read_calibrated_files(folderpath, substructure, start_dt, stop_dt, delta=1, root_filename="")

    Entrée :
        - folderpath : chemin dossier comportant les fichiers (ou sous-dossiers) ; string
        - substructure : sous-structure ; int
        - start_dt : date de début ; datetime
        - stop_dt : date de fin ; datetime
        - delta : marge en minute ; int
        - root_filename : nom racine des fichiers ; string

    Sortie :
        - df : données ; dataframe
    """

    df = pd.DataFrame()

    if stop_dt < start_dt:
        start_dt, stop_dt = stop_dt, start_dt

    # list files in folder
    # in period
    # substructure
    filepath_list = list_files_in_folder(folderpath, substructure, start_dt, stop_dt, delta=delta, root_filename=root_filename)
    if len(filepath_list) == 0:
        return df

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

    # check duration
    duration = int((stop_dt - start_dt).total_seconds()/60)
    true_duration = int((df.index.max() - df.index.min()).total_seconds()/60)
    if (true_duration > duration*1.1) or (true_duration < duration*0.9):
        raise Exception("problem of duration")

    return df

def read_calibrated_file(filepath, date_format = "%Y-%m-%dT%H:%M:%S.%fZ", delimiter=",", decimal="."):
    """
    lecture d'un fichier de données calibrées

    df = read_calibrated_file(filepath, date_format = "%Y-%m-%dT%H:%M:%S.%fZ", delimiter=",", decimal=".")

    le fichier doit comporter une colonne "time" avec des dates au format date_format

    Entrée :
        - filepath : chemin du fichier ; string
        - date_format : format date colonne "time" ; string
        - delimiter : délimiteur ; string
        - decimal : décimal ; string

    Sortie :
        - df : données ; dataframe
    """

    custom_date_parser = lambda x: datetime.strptime(x, date_format)
    df = pd.read_csv(filepath, index_col=0, delimiter=delimiter, decimal=decimal, parse_dates=["time"],
                     date_parser=custom_date_parser)

    return df

def check_continuity(filepath_list):
    """
    vérification de la continuité d'une liste de fichiers

    exemple nom fichier = "mpsintegrationcontinue_1M_2024_01_01_00h00m_0_CALIBRATED.csv

    renvoie vrai si les fichiers sont continus (1 fichier toutes les minutes par exemple)
    ou faux dans le cas contraire

    b_continuity = check_continuity(filepath_list)

    Entrée :
        - filepath_list : liste de fichiers ; string

    Sortie :
        - b_continuity : test ; booléen

    """

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

def list_files_in_folder(folderpath, substructure, start_dt, stop_dt, delta=1, root_filename=""):
    """
    liste les fichiers dans le dossier compris entre les dates start_dt et stop_dt

    exemple nom fichier = "mpsintegrationcontinue_1M_2024_01_01_00h00m_0_CALIBRATED.csv
    mpsintegrationcontinue -> root_filename
    0 -> substructure

    les fichiers peuvent être organisés en sous-dossiers (plus efficace lorsque le nombre de fichiers est important)

    exemple nom sous-dossier = "2024_01_01"

    le paramètre delta (marge) permet de gérer les éventuels décalages entre le timestamp du fichier et les timestamps des données dans le fichier

    filepath_list = list_files_in_folder(folderpath, substructure, start_dt, stop_dt, delta=1, root_filename="")

    Entrée :
        - folderpath : chemin dossier comportant les fichiers (ou sous-dossiers) ; string
        - substructure : sous-structure ; int
        - start_dt : date de début ; datetime
        - stop_dt : date de fin ; datetime
        - delta : marge en minute ; int
        - root_filename : nom racine des fichiers ; string

    Sortie :
        - filepath_list : liste chemin fichiers ; liste de string
    """

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
            regex = root_filename + r"_(?P<duration>\d{1,2})M_(?P<year>\d{4})_(?P<month>\d{2})_(?P<day>\d{2})_(?P<hour>\d{2})h(?P<minute>\d{2})m_(?P<substructure>\d)_CALIBRATED.csv$"
            m = re.search(regex, filename)
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

    input_folderpath = r"C:\Users\md104209\Desktop\2022_06_01"
    output_folderpath = r"C:\Users\md104209\Desktop\Nouveau dossier"
    substructure = 0
    root_filename = ""
    start_dt = datetime(2022,6,1,0,0)
    stop_dt = datetime(2022,6,1,1,0)
    duration = timedelta(minutes=10)
    periodicity = timedelta(minutes=10)

    date_dt_list = []
    current_dt = start_dt
    while current_dt+duration <= stop_dt:
        date_dt_list.append([current_dt, current_dt+duration])
        current_dt += periodicity
    print(date_dt_list)

    parallel_task = mpt.parallel_decorator(read_calibrated_files_task, date_dt_list, input_folderpath, output_folderpath, substructure, 1, root_filename)
    parallel_task()

    # folderpath = r"C:\Users\md104209\Desktop\2022_06_01"
    # substructure = 0
    # start_dt = datetime(2022,6,1,0,0)
    # stop_dt = datetime(2022,6,1,0,20)
    # root_filename = "mpszefyros"
    #
    # df = read_calibrated_files(folderpath, substructure, start_dt, stop_dt, delta=0, root_filename=root_filename)
    #
    # # filename
    # duration = int((stop_dt - start_dt).total_seconds()/60)
    # date_str = start_dt.strftime("%Y_%m_%d_%Hh%Mm")
    # filename = f"{root_filename}_{duration}M_{date_str}_{substructure}_CALIBRATED.csv"
    #
    # df.to_csv(filename, float_format = "%.8e",date_format="%Y-%m-%dT%H:%M:%S.%fZ")
