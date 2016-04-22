# -*- coding: utf-8 -*-
__author__ = 'XaviTorello'

import logging, glob, os
from multiprocessing.dummy import Pool as ThreadPool

from importTypes import *

def Import_Massive(path_="/opt/srcs/f1/F101", type="F1", CPUs=4):

    os.chdir(path_)
    files = glob.glob("*.xml")
    max = len(files)

    pool = ThreadPool(CPUs)

    if (type=="F1"):
        processer = process_F1file
    elif (type=="Q1"):
        processer = process_Q1file

    try:
        pool.map(processer , ((fileF1, idx, max, COLLECTION) for idx, fileF1 in enumerate(files)) )
    except Exception as e:
        print "Thread error at processing '{}'".format( e )

    pool.close()
    pool.join()



COLLECTION="rolf"

logging.basicConfig(level=logging.INFO)

Import_Massive(path_="/opt/srcs/f1/F101", type="F1", CPUs=2)
