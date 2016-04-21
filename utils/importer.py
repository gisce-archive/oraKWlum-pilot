# -*- coding: utf-8 -*-
__author__ = 'XaviTorello'

from orakwlum.importer import *

import logging, glob, os
from multiprocessing.dummy import Pool as ThreadPool


def process_F1file(args):
    try:
        file, idx, max = args
        mess ="{}/{} Procesing F1 '{}'".format(idx, max, file)
        print mess
        logging.info(mess)

        importer = F1Importer(file_to_import=file, collection=COLLECTION)

        importer.process_consumptions()
        os.rename(file, file+"_done")

    except:
        mess = "Error processing F1 {}.\n{}".format(file)
        print mess
        logging.info(mess)


def process_Q1file(args):
    try:
        file, idx, max = args
        mess ="{}/{} Procesing q1 '{}'".format(idx, max, file)
        print mess
        logging.info(mess)

        importer = Q1Importer(file_to_import=file, collection=COLLECTION)
        importer.process_consumptions()
        os.rename(file, file+"_done")

    except:
        mess = "Error processing Q1 {}.\n{}".format(file)
        print mess
        logging.info(mess)
        pass




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
        pool.map(processer , ((fileF1, idx, max) for idx, fileF1 in enumerate(files)) )
    except Exception as e:
        print "Thread error at processing '{}'".format( e )

    pool.close()
    pool.join()


COLLECTION="rolf"

logging.basicConfig(level=logging.INFO)

#Sampledata_tester()
#Proposal_tester()

Import_Massive(path_="/opt/srcs/f1/F101", type="F1", CPUs=2)
