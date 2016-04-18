# -*- coding: utf-8 -*-
__author__ = 'XaviTorello'
#__name__ = "Import"

import logging

logger = logging.getLogger(__name__)


class Import(object):
    """
    Main Import object
    """

    def __init__(self, file):
        self.file_name = file
        self.file = open(file, "r")

    def get_type(self):
        pass
