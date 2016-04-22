# -*- coding: utf-8 -*-
__author__ = 'XaviTorello'

from orakwlum.importer import *

import logging, glob, os
from multiprocessing.dummy import Pool as ThreadPool


def process_F1file(args):
    try:
        file, idx, max, COLLECTION = args
        mess ="{}/{} Procesing F1 '{}'".format(idx, max, file)
        print mess
        logging.info(mess)

        importer = F1Importer(file_to_import=file, collection=COLLECTION)

        importer.process_consumptions()
        os.rename(file, file+"_done")

    except Exception as exc:
        mess = "Error processing F1 {}: <{}>".format(file, exc)
        print mess
        logging.info(mess)


