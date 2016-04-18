# -*- coding: utf-8 -*-
__author__ = 'XaviTorello'
#__name__ = "Import"

import logging
from importer import Import

logger = logging.getLogger(__name__)


class F1_importer(Import):
    """
    Import F1 object
    """

    def __init__(self, file):
        self.file = file

    def get_type (self):
        return "F1"
