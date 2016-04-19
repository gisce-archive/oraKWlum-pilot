# -*- coding: utf-8 -*-
__author__ = 'XaviTorello'
#__name__ = "Import"

import logging

logger = logging.getLogger(__name__)


from orakwlum.datasource import Mongo



class Import(object):
    """
    Main Import object
    """


    def __init__(self, file, collection):
        self.file_name = file
        self.file = open(file, "r")
        self.dataset = Mongo(user="orakwlum", db="orakwlum")
        self.collection = collection

    def get_type(self):
        pass
