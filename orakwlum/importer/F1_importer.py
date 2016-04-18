# -*- coding: utf-8 -*-
__author__ = 'XaviTorello'
#__name__ = "Import"

import logging
from importer import Import

from switching.input.messages import F1

logger = logging.getLogger(__name__)


class F1_importer(Import):
    """
    Import F1 object
    """

    def __init__(self, file):
        self.file = open((file), "r")
        self.parse()



    def get_type (self):
        return self.f1.get_tipus_xml()
    #   return "F1"


    def parse (self):
        self.f1 = F1(self.file)
        self.f1.set_xsd()
        self.type = self.f1.get_tipus_xml()
        self.f1.parse_xml()


    def get_invoices_count (self):
        return self.f1.num_factures


    def get_invoices(self):
        return self.f1.get_factures()