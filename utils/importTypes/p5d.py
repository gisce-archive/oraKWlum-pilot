# -*- coding: utf-8 -*-
__author__ = 'XaviTorello'


from orakwlum.importer import *

import logging, glob, os
from multiprocessing.dummy import Pool as ThreadPool


def process_P5Dfile(args):
    try:
        file, idx, max, COLLECTION = args
        print file
        mess ="{}/{} Procesing P5D '{}'".format(idx, max, file)
        print mess
        logging.info(mess)

        importer = P5DImporter(file_to_import=file, collection=COLLECTION)
        importer.process_consumptions()
        os.rename(file, file+"_done")

    except Exception as exc:
        mess = "Error processing P5D {}: <{}>".format(file, exc)
        print mess
        logging.info(mess)
        pass


process_P5Dfile(("./mostres/testDataP5D.csv",1,1))