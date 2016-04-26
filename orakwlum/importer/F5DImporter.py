# -*- coding: utf-8 -*-
__author__ = 'XaviTorello'
# __name__ = "F5D_importer"

import logging

from cchloader.file import CchFile, PackedCchFile
from cchloader.compress import is_compressed_file

from enerdata.datetime.timezone import TIMEZONE

from orakwlum.importer import Import
from orakwlum.consumption import *

logger = logging.getLogger(__name__)


class F5DImporter(Import):
    """
    Import P5D object
        Use gisce/switching lib for parse P5D file
    """

    def __init__(self, file_to_import, collection="dataset"):
        """
        Set P5D file and start parsing it
        """
        super(F5DImporter, self).__init__(file_to_import, collection)

    @property
    def type(self):
        return "F5D"

    def process_consumption(self):
        self.parse()

    def parse(self):
        """
        Adapted from gisce/cchloader/blob/gisce/cchloader/cli/__init__.py

        Review if a F5D file is compressed or not and extract all lines

        Parse each line and update DB if needed
        """
        if is_compressed_file(self.file_name):
            with PackedCchFile(self.file_name) as psf:
                for cch_file in psf:
                    for line in cch_file:
                        if not line:
                            continue
                        self.parse_line(line)
        else:
            with CchFile(self.file_name) as cch_file:
                for line in cch_file:
                    if not line:
                        continue
                    self.parse_line(line)

    def parse_line(self, line):
        """
        Create the Consumption object from a F5D line and save it (if more significant) to DB
        """
        data = line['cchfact'].data
        cups = data['name']
        consumption = data['ai']
        #ao = data['ao']
        hour = TIMEZONE.localize(data['datetime'])

        logger.debug("  [F5D] Processing {} {} {}".format(cups, hour,
                                                          consumption))

        consumption_from_line = Consumption(cups=cups,
                                            hour=hour,
                                            real=consumption,
                                            origin=self.type)

        # Save (Upsert) consumption following strategy "importance of data"
        self.save_consumption_if_needed(consumption_from_line)
