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
        self.file = file

    def get_type (self):
        pass
