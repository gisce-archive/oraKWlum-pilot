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
        #importer.process_consumptions()
        #os.rename(file, file+"_done")

    except Exception as exc:
        mess = "Error processing P5D {}: <{}>".format(file, exc)
        print mess
        logging.info(mess)
        pass



def parse_line (line):
    data = line['cchval'].data
    cups = data['name']
    ai = data['ai']
    ao = data['ao']
    hour = data['datetime']
    print cups, ai, ao, hour

import click
from cchloader.file import CchFile, PackedCchFile
from cchloader.compress import is_compressed_file

def import_file(file):
    if is_compressed_file(file):
        click.echo("Using packed CCH File for {}".format(file))
        with PackedCchFile(file) as psf:
            for cch_file in psf:
                for line in cch_file:
                    if not line:
                        continue
                    parse_line(line)
    else:
        with CchFile(file) as cch_file:
            for line in cch_file:
                if not line:
                    continue
                parse_line(line)


logging.basicConfig(level=logging.DEBUG)

process_P5Dfile(("./P5D_0303_0642_20160126.0",1,1,"1"))
#import_file("./P5D_0303_0642_20160126.0.bz2")
#import_file("./P5D_0303_0642_20160126.0")