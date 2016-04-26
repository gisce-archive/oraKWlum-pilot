# -*- coding: utf-8 -*-
__author__ = 'XaviTorello'

from orakwlum.importer import F5DImporter

import logging, os


def process_F5Dfile(args):
    try:
        file, idx, max, COLLECTION, rename = args
        mess = "{}/{} Procesing F5D '{}'".format(idx, max, file)
        print mess
        logging.info(mess)

        importer = F5DImporter(file_to_import=file, collection=COLLECTION)
        importer.process_consumption()

        if rename:
            os.rename(file, file + "_done")

    except Exception as exc:
        mess = "Error processing F5D {}: <{}>".format(file, exc)
        print mess
        logging.info(mess)
