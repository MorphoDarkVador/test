# -*- coding: utf-8 -*-

import os
import time
import re
import shutil
import gzip
import glob
from datetime import datetime, timedelta
import time
import numpy as np
from math import ceil
import pandas as pd
import morpho_toolbox as mpt

def renameFilesInFolder():

    folderpath = r"C:\Users\md104209\Desktop\bidon"

    filename_list = sorted(os.listdir(folderpath))
    number_of_files = len(filename_list)

    tStart = time.time()

    for n, filename in enumerate(filename_list):

        _, ext = os.path.splitext(filename)
        m = re.search("(?P<year>\d{4})_(?P<month>\d{2})_(?P<day>\d{2})_(?P<hour>\d{2})h(?P<minute>\d{2})m", filename)

        if m is not None and ext == ".csv":

            year = int(m["year"])
            month = int(m["month"])
            day = int(m["day"])
            hour = int(m["hour"])
            minute = int(m["minute"])

            dt = datetime(year, month, day, hour, minute)# - timedelta(hours=1)
            date = dt.strftime("%Y_%m_%d_%Hh%Mm")

            filename_new = "_".join(["slkpontlagnieu", "20M", date, "2", "CALIBRATED"]) + ".csv"

            try:
                os.rename(os.path.join(folderpath,filename), os.path.join(folderpath,filename_new))
            except FileExistsError:
                pass

        if n % 100 == 0 and n > 0:
            tElapsed = time.time() - tStart
            tRemaining = (number_of_files - n - 1) * tElapsed / (n + 1)

            if tRemaining > 60:
                tRemaining /= 60
                print("Remaining time = {:.0f} min".format(tRemaining))
            else:
                print("Remaining time = {:.0f} s".format(tRemaining))

if __name__ == "__main__":

    renameFilesInFolder()