# -*- coding: utf-8 -*-
__author__ = 'XaviTorello'


from orakwlum.importer import P5DImporter

import logging, os


def process_P5Dfile(args):
    try:
        file, idx, max, COLLECTION, rename = args
        print file
        mess ="{}/{} Procesing P5D '{}'".format(idx, max, file)
        print mess
        logging.info(mess)

        importer = P5DImporter(file_to_import=file, collection=COLLECTION)

        if rename:
            os.rename(file, file+"_done")

    except Exception as exc:
        mess = "Error processing P5D {}: <{}>".format(file, exc)
        print mess
        logging.info(mess)
