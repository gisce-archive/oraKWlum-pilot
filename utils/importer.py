# -*- coding: utf-8 -*-
__author__ = 'XaviTorello'

import logging, glob, os
from multiprocessing.dummy import Pool as ThreadPool
from importTypes import *


def Import_Massive(path_="./inputs/F1", filter="*.xml", type="F1", CPUs=4):
    os.chdir(path_)
    files = glob.glob(filter)
    count_files = len(files)

    pool = ThreadPool(CPUs)

    # diff hardcoded functions to speedup pool.map without internal conditionals
    if type == "F1":
        processer = process_F1file
    elif type == "Q1":
        processer = process_Q1file
    elif type == "P5D":
        processer = process_P5Dfile

    try:
        pool.map(processer,
                 ((fileF1, idx + 1, count_files, COLLECTION, RENAME_PROCESSED)
                  for idx, fileF1 in enumerate(files)))

    except Exception as e:
        print "Thread error at processing '{}'".format(e)

    pool.close()
    pool.join()


COLLECTION = "importer_test"
RENAME_PROCESSED = False

logging.basicConfig(level=logging.INFO)

#Import_Massive(path_="../inputs/F1", filter="*.xml", type="F1", CPUs=2)
#Import_Massive(path_="../inputs/Q1", filter="*.xml", type="Q1", CPUs=2)
Import_Massive(path_="../inputs/P5D", filter="*0*", type="P5D", CPUs=2)
