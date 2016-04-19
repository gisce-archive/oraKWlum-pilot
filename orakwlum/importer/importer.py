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
        try:
            self.file = open(file, "r")
            self.dataset = Mongo(user="orakwlum", db="orakwlum")
            self.collection = collection
            
        except Exception, e:
            print "The file {} can't be processed\n{}".format(file,e)



    def get_type(self):
        pass
